# 備份 Pipeline

這個版本把讀取、CPU 計算、物件寫入與 SQLite 提交分成可以重疊執行的階段。Reader 讀完一個檔案後，會接著讀下一個檔案，已排入工作的 chunk 由後續階段繼續處理。

```mermaid
flowchart LR
    R[Reader<br/>讀檔與來源變更檢查] --> Q1[有界 Chunk Queue]
    Q1 --> C[CPU Workers<br/>BLAKE3 / 去重 / Zstd]
    C --> Q2[有界 Object Queue]
    Q2 --> W[Storage Writers<br/>writev / fsync / 發布]
    R --> Q3[有界 Metadata Queue<br/>檔案資訊與 chunk futures]
    Q3 --> M[Metadata Consumer<br/>等待 durable ACK]
    W -. ACK .-> M
    M --> DB[批次 SQLite Transaction]
    DB --> F[所有階段完成後發布 COMPLETE]
```

## 各階段如何前進

1. Source scanner 列出檔案。Reader 在配置 raw buffer 前先取得 memory credit，讀完 chunk 就排入 CPU queue，並把相對路徑、ordinal、大小和 future 放進 metadata batch。
2. CPU worker 執行 hashing，先取得 digest 的處理權再查 repository。這個順序避免 metadata 剛提交、claim 剛釋放時，另一個 worker 用先前查不到物件的結果再寫一份。
3. 新物件完成壓縮後交給 writer，CPU worker 繼續下一個 chunk。重複物件會釋放 raw buffer 和 credit，登記完成回呼；owner 完成後再填入對應 future。
4. Pack writer 會將佇列裡已經有的最多 8 個物件，用一次 `writev()` 寫入。它不會等滿 8 個才寫，所以只有一個 chunk 額度也能前進。每組寫完立即釋放 payload buffer 和 credit。Pack 仍要完成 fsync、無覆寫發布及目錄同步才回 ACK。
5. Metadata consumer 先等 batch 中的 ACK，再開始 SQLite transaction；在同一 transaction 中插入 entries、objects、chunk references。Prepared statements 在 batch 內重用。提交成功後才釋放完成的 claims。

每個 metadata batch 最多 128 個 entry，或到達 chunk batch 上限時送出。Pack-v2 的 chunk 上限為 `256 × writer_workers`；files-v1 為 `max(8, 4 × CPU workers)`。Metadata queue 容量是 4，因此 futures 與尚待提交的 metadata 也有固定上限。

## 記憶體、停止與錯誤

每個 chunk 的 credit 涵蓋 1 MiB raw buffer 和 Zstd 壓縮上界。`writev()` 使用原有 buffers，不建立另一份合併 payload。這個額度控制在途 chunk buffer；程式、SQLite、metadata、allocator 和作業系統 page cache 的記憶體另外計算。

正常結束時，Reader 先送出最後的 metadata batch。Coordinator 關閉輸入，等 CPU workers 完成，再關閉並等待 storage writers，最後等 metadata consumer 完成。經過 chunk layout 檢查後，才以獨立 transaction 將 snapshot 設為 `COMPLETE`。

任一階段發生錯誤時會記住第一個 exception，關閉三個 queues 和 CPU admission gate，讓所有階段停止。Metadata future 的等待每 10 ms 檢查取消，memory credit 等待每 50 ms 檢查取消。所有執行緒 join 後才寫入 `FAILED`。程序突然終止則沿用 staging、未完成 snapshot 與 orphan pack recovery 流程。

## 如何量測

`tools/benchmark_pipeline.py` 對同一份保留的資料測 1／2／4／8 CPU workers，每次使用新的 repository，並完整驗證結果。它記錄 process wall/user/system time、各階段耗時、memory credit 峰值，以及小於 1 KiB 和大於 64 MiB 的 pack 數量。這可分辨「大量小檔各自一包」和「大型資料 pack 沒填滿」兩種情形。

```sh
python3 tools/generate_pipeline_fixture.py --output /private/tmp/pbe-log-fixture --kind logs
python3 tools/benchmark_pipeline.py \
  --pbe build-release/pbe --dataset /private/tmp/pbe-log-fixture/source \
  --label pipeline --output reports/pipeline-logs \
  --workers 1,2,4,8 --writer-workers 1 --repeat 2
```

`logs` 是每個 1 MiB chunk 都有唯一前綴的可壓縮紀錄資料；`random` 則是不可壓縮隨機資料。兩者都使用真實 hashing、壓縮、寫入和驗證，不注入 CPU 空轉或人為等待。應分別看待 CPU 計算與儲存吞吐限制，不能把單一 fixture 的加速比套用到所有資料。

Schema v6 將實際 queue 阻塞與非同步 ACK 延遲分開。詳細欄位見 [DIAGNOSTICS_SCHEMA.md](DIAGNOSTICS_SCHEMA.md)。
