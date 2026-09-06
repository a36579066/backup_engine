const $ = (selector) => document.querySelector(selector);
const repoInput = $('#repoPath');
const sourceInput = $('#sourcePathInput');
const form = $('#backupForm');
let lastStatus = null;

const diagnosisInfo = {
  CPU_LIMITED: ['CPU 計算是目前主要限制', '輸入工作持續排隊，計算階段使用率較高，寫入端仍有餘裕。', 'cpu'],
  WRITER_LIMITED: ['物件寫入端形成回壓', '寫入工作較忙，Worker 正等待物件寫入或確認。', 'writer'],
  MEMORY_LIMITED: ['記憶體額度接近上限', '管線 Buffer 使用量偏高，部分工作正在等待記憶體額度。', 'memory'],
  INPUT_LIMITED: ['來源端供應工作較慢', '輸入佇列較常為空，Worker 有可用計算能力。', 'input'],
  DEDUP_CONTENTION: ['重複 Chunk 正在互相等待', '多個 Worker 遇到相同內容，正在共用同一筆物件寫入結果。', 'neutral'],
  MIXED: ['多個階段共同影響速度', '目前量測尚未指出單一明顯瓶頸。', 'neutral'],
  UNKNOWN: ['等待足夠的監控資料', '第一個監控視窗尚未累積足夠工作量。', 'neutral'],
};

const actionLabels = {
  HOLD: '保持目前 Worker 上限',
  INCREASE_CPU_ADMISSION: '增加一個 CPU 工作額度',
  DECREASE_CPU_ADMISSION: '減少一個 CPU 工作額度',
  ROLLBACK_CPU_ADMISSION: '回退上一次額度調整',
};

function formatBytes(value, digits = 1) {
  const bytes = Number(value || 0);
  if (!Number.isFinite(bytes)) return '—';
  if (bytes < 1024) return `${bytes.toFixed(0)} B`;
  const units = ['KiB', 'MiB', 'GiB', 'TiB'];
  let size = bytes / 1024;
  let index = 0;
  while (size >= 1024 && index < units.length - 1) { size /= 1024; index += 1; }
  return `${size.toFixed(digits)} ${units[index]}`;
}

function formatRate(value) {
  const number = Number(value || 0);
  return number > 0 ? `${formatBytes(number, 1)}/s` : '—';
}

function formatDuration(seconds) {
  const value = Math.max(0, Math.floor(Number(seconds || 0)));
  const hours = Math.floor(value / 3600);
  const minutes = Math.floor((value % 3600) / 60);
  const secs = value % 60;
  return hours ? `${String(hours).padStart(2, '0')}:${String(minutes).padStart(2, '0')}:${String(secs).padStart(2, '0')}` : `${String(minutes).padStart(2, '0')}:${String(secs).padStart(2, '0')}`;
}

function localDate(epochSeconds) {
  if (!epochSeconds) return '—';
  return new Date(Number(epochSeconds) * 1000).toLocaleString('zh-TW', { hour12: false });
}

function safeText(element, value) {
  element.textContent = value ?? '';
}

function setSignal(name, value) {
  const percent = Math.max(0, Math.min(100, Number(value || 0) * 100));
  $(`#${name}Signal`).textContent = `${percent.toFixed(0)}%`;
  $(`#${name}Bar`).style.width = `${percent}%`;
}

function renderChart(history) {
  const chart = $('#throughputChart');
  chart.replaceChildren();
  const values = (history || []).map(Number).filter(Number.isFinite);
  if (!values.length) {
    const placeholder = document.createElement('div');
    placeholder.className = 'chart-placeholder';
    placeholder.textContent = '等待第一個監控視窗';
    chart.append(placeholder);
    $('#chartCurrent').textContent = '—';
    $('#chartWindows').textContent = '0 個視窗';
    return;
  }
  const max = Math.max(1, ...values);
  values.forEach((value) => {
    const bar = document.createElement('i');
    bar.className = 'chart-bar';
    bar.style.height = `${Math.max(4, (value / max) * 94)}%`;
    bar.title = formatRate(value);
    chart.append(bar);
  });
  $('#chartCurrent').textContent = (values.at(-1) / 1048576).toFixed(2);
  $('#chartWindows').textContent = `${values.length} 個視窗`;
}

function renderSnapshots(snapshots) {
  const body = $('#snapshotRows');
  body.replaceChildren();
  $('#historyCount').textContent = `${snapshots.length} 個快照`;
  if (!snapshots.length) {
    const row = document.createElement('tr');
    const cell = document.createElement('td');
    cell.colSpan = 7;
    cell.className = 'table-empty';
    cell.textContent = '這個儲存庫目前還沒有完整快照';
    row.append(cell);
    body.append(row);
    return;
  }
  for (const snapshot of snapshots) {
    const row = document.createElement('tr');
    const values = [
      snapshot.id,
      localDate(snapshot.created_ns / 1e9),
      snapshot.source_root,
      Number(snapshot.files || 0).toLocaleString('zh-TW'),
      Number(snapshot.chunks || 0).toLocaleString('zh-TW'),
      formatBytes(snapshot.logical_bytes),
    ];
    values.forEach((value, index) => {
      const cell = document.createElement('td');
      if (index === 0) cell.className = 'snapshot-id';
      if (index === 2) { cell.className = 'snapshot-source'; cell.title = value; }
      cell.textContent = value;
      row.append(cell);
    });
    const status = document.createElement('td');
    const pill = document.createElement('span');
    pill.className = 'complete-pill';
    pill.textContent = 'COMPLETE';
    status.append(pill);
    row.append(status);
    body.append(row);
  }
}

function renderJob(job) {
  const state = job?.state || 'idle';
  const running = state === 'running' || state === 'stopping' || state === 'external';
  const statusNames = { idle: '待命', running: '備份中', stopping: '正在安全停止', external: '外部備份中', complete: '已完成', cancelled: '已取消', failed: '備份失敗' };
  const statusNode = $('#jobStatus');
  safeText(statusNode, statusNames[state] || '待命');
  statusNode.className = `metric-value status-value${running ? ' running' : ''}${state === 'failed' ? ' failed' : ''}`;
  safeText($('#jobSubstatus'), running ? `${job.scheduler === 'adaptive' ? 'Adaptive' : 'Fixed'} · 已執行 ${formatDuration(job.elapsed_seconds)}` : state === 'complete' ? '快照已安全發布' : state === 'failed' ? '請查看錯誤訊息' : state === 'cancelled' ? '工作已清理並停止' : '等待備份工作');
  $('#jobSubstatus').className = 'metric-foot';
  $('#jobSubstatus').insertAdjacentHTML('afterbegin', `<span class="foot-dot${running ? ' live' : ''}"></span>`);

  const metrics = job?.metrics;
  const totals = metrics?.totals || {};
  const finishedSnapshot = job?.summary?.snapshot_id
    ? lastStatus?.repository?.snapshots?.find((snapshot) => snapshot.id === job.summary.snapshot_id)
    : null;
  safeText($('#filesValue'), metrics ? Number(totals.progress_file_count || 0).toLocaleString('zh-TW') : finishedSnapshot ? Number(finishedSnapshot.files || 0).toLocaleString('zh-TW') : '—');
  safeText($('#filesFoot'), running ? '已完成檔案 · 總數在掃描完成後確認' : job?.summary ? `快照共 ${Number(job.summary.logical_bytes || 0).toLocaleString('zh-TW')} bytes` : '完成檔案數');
  safeText($('#bytesValue'), metrics ? formatBytes(totals.completed_raw_bytes || 0) : job?.summary ? formatBytes(job.summary.read_bytes) : '—');
  safeText($('#bytesFoot'), running ? 'Chunk 完成並取得物件確認' : job?.summary ? `新增儲存 ${formatBytes(job.summary.new_stored_bytes)}` : '讀取與驗證的資料量');
  const latestRate = metrics?.latest?.observations?.throughput_bytes_per_sec || 0;
  safeText($('#speedValue'), latestRate > 0 ? `${(latestRate / 1048576).toFixed(2)}` : '—');
  safeText($('#speedFoot'), latestRate > 0 ? 'MiB/s · 最近監控視窗' : 'MiB/s · 最近監控視窗');

  safeText($('#runBadge').querySelector('span'), job?.run_id ? job.run_id : running ? (state === 'external' ? 'CLI 工作' : '正在準備監控視窗') : '尚無執行工作');
  $('#runBadge').classList.toggle('live', running);
  const track = $('#progressTrack');
  track.classList.toggle('indeterminate', running);
  $('#progressBar').style.width = running ? '' : (state === 'complete' ? '100%' : '0%');
  safeText($('#sourcePath'), job?.source || '開始備份後，這裡會顯示來源路徑');
  safeText($('#elapsedTime'), formatDuration(job?.elapsed_seconds || 0));
  safeText($('#chunksValue'), metrics ? Number(totals.completed_chunks || 0).toLocaleString('zh-TW') : finishedSnapshot ? Number(finishedSnapshot.chunks || 0).toLocaleString('zh-TW') : '—');
  safeText($('#uniqueValue'), job?.summary ? formatBytes(job.summary.unique_raw_bytes) : metrics ? formatBytes(totals.completed_unique_raw_bytes || 0) : '—');
  safeText($('#workersValue'), job ? `${metrics?.active_limit != null
    ? `${metrics.active_limit}${job.workers ? ` / ${job.workers}` : ''} CPU 額度`
    : job.workers ? `${job.workers} CPU`
    : '外部工作'} · ${job.writer_workers || metrics?.latest?.observations?.writer_workers || 1} Writer` : '—');
  safeText($('#schedulerValue'), job?.scheduler || '—');
  safeText($('#emptyPipeline'), running && !metrics ? '正在掃描來源並建立備份清單；第一個監控視窗出現後就會顯示即時指標。' : running ? '已處理檔案數會持續增加；總檔案數在完整掃描結束後確認。' : state === 'failed' ? (job.error || '備份遇到錯誤。') : state === 'cancelled' ? '備份已取消，先前完成的快照不會受到影響。' : state === 'complete' ? `Snapshot ${job.summary?.snapshot_id || ''} 已完成。` : '備份開始後，每兩秒更新一次管線資料。掃描階段會先顯示準備狀態.');
  $('#emptyPipeline').classList.toggle('error-note', state === 'failed');

  const diagnosis = metrics?.diagnosis || 'UNKNOWN';
  const [title, description, kind] = diagnosisInfo[diagnosis] || diagnosisInfo.UNKNOWN;
  $('#diagnosisTag').className = `diagnosis-tag ${kind}`;
  safeText($('#diagnosisTag'), diagnosis.replaceAll('_', ' '));
  safeText($('#diagnosisTitle'), metrics ? title : '等待第一筆診斷資料');
  safeText($('#diagnosisText'), metrics ? description : '備份開始後，這裡會顯示目前主要的管線瓶頸與排程器動作。');
  safeText($('#diagnosisIcon'), diagnosis === 'CPU_LIMITED' ? '⌁' : diagnosis === 'WRITER_LIMITED' ? '⇥' : diagnosis === 'MEMORY_LIMITED' ? '◈' : diagnosis === 'INPUT_LIMITED' ? '↓' : '✦');
  if (metrics) {
    setSignal('cpu', metrics.cpu_compute);
    setSignal('input', metrics.input_queue_fill);
    setSignal('output', metrics.object_queue_fill);
    setSignal('memory', metrics.memory_used);
  } else {
    for (const name of ['cpu', 'input', 'output', 'memory']) { $(`#${name}Signal`).textContent = '—'; $(`#${name}Bar`).style.width = '0'; }
  }
  safeText($('#actionValue'), actionLabels[metrics?.action] || (job?.error ? '查看備份錯誤' : '—'));
  safeText($('#ruleValue'), metrics?.latest?.rule_id || '');
  renderChart(metrics?.throughput_history || []);

  $('#startButton').hidden = running;
  $('#stopButton').hidden = !running || !job.can_stop;
  $('#stopButton').textContent = state === 'stopping' ? '正在安全停止…' : '▣  安全停止備份';
}

function renderStatus(data) {
  lastStatus = data;
  const repository = data.repository || {};
  const initialized = Boolean(repository.initialized);
  safeText($('#serverText'), '本機服務正常');
  safeText($('#repoState'), repository.error ? '讀取失敗' : initialized ? '已連線' : repoInput.value.trim() ? '尚未初始化' : '輸入儲存庫路徑');
  $('#repoState').className = `repo-state ${initialized ? 'good' : repository.error ? 'bad' : ''}`;
  $('#initRepo').hidden = !repoInput.value.trim() || initialized;
  $('#storageFormatInput').closest('.repo-format').hidden = !repoInput.value.trim() || initialized;
  safeText($('#snapshotCount'), initialized ? Number(repository.stats?.snapshot_count || 0).toLocaleString('zh-TW') : '—');
  safeText($('#objectCount'), initialized ? Number(repository.stats?.object_count || 0).toLocaleString('zh-TW') : '—');
  safeText($('#storedSize'), initialized ? formatBytes(repository.stats?.stored_bytes || 0) : '—');
  safeText($('#storageFormat'), initialized ? (repository.storage_format || 'files-v1') : '—');
  safeText($('#repoPathShort'), repoInput.value.trim() || '尚未選擇儲存庫');
  $('#repoPathShort').title = repoInput.value.trim();
  $('#startButton').disabled = !initialized || Boolean(data.job && ['running', 'stopping', 'external'].includes(data.job.state));
  renderSnapshots(repository.snapshots || []);
  renderJob(data.job);
}

async function poll() {
  const repo = repoInput.value.trim();
  try {
    const response = await fetch(`/api/status?repo=${encodeURIComponent(repo)}`, { cache: 'no-store' });
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    renderStatus(await response.json());
  } catch (error) {
    safeText($('#serverText'), '連線中斷');
    safeText($('#repoState'), error.message);
    $('#repoState').className = 'repo-state bad';
  }
}

async function postJson(path, body) {
  const response = await fetch(path, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
  const result = await response.json();
  if (!response.ok) throw new Error(result.error || `HTTP ${response.status}`);
  return result;
}

form.addEventListener('submit', async (event) => {
  event.preventDefault();
  $('#formError').hidden = true;
  const button = $('#startButton');
  button.disabled = true;
  button.querySelector('span').textContent = '正在啟動…';
  try {
    const result = await postJson('/api/start', {
      repo: repoInput.value.trim(),
      source: sourceInput.value.trim(),
      workers: Number($('#workerInput').value),
      writer_workers: Number($('#writerInput').value),
      memory_mib: Number($('#memoryInput').value),
      scheduler: $('#schedulerInput').value,
      paranoid: false,
    });
    localStorage.setItem('pbe.source', sourceInput.value.trim());
    localStorage.setItem('pbe.workers', $('#workerInput').value);
    localStorage.setItem('pbe.writer-workers', $('#writerInput').value);
    localStorage.setItem('pbe.memory', $('#memoryInput').value);
    await poll();
  } catch (error) {
    safeText($('#formError'), error.message);
    $('#formError').hidden = false;
  } finally {
    button.querySelector('span').textContent = '開始備份';
    await poll();
  }
});

$('#stopButton').addEventListener('click', async () => {
  try {
    await postJson('/api/stop', {});
    await poll();
  } catch (error) {
    safeText($('#formError'), error.message);
    $('#formError').hidden = false;
  }
});

$('#initRepo').addEventListener('click', async () => {
  const button = $('#initRepo');
  const original = button.textContent;
  button.disabled = true;
  button.textContent = '初始化中…';
  try {
    const result = await postJson('/api/init', {
      repo: repoInput.value.trim(),
      storage_format: $('#storageFormatInput').value,
    });
    safeText($('#repoState'), '儲存庫已初始化');
    localStorage.setItem('pbe.repo', repoInput.value.trim());
    await poll();
  } catch (error) {
    safeText($('#repoState'), error.message);
    $('#repoState').className = 'repo-state bad';
  } finally {
    button.disabled = false;
    button.textContent = original;
  }
});

repoInput.addEventListener('input', () => {
  localStorage.setItem('pbe.repo', repoInput.value.trim());
  window.clearTimeout(repoInput._timer);
  repoInput._timer = window.setTimeout(poll, 250);
});

$('#schedulerInput').addEventListener('change', () => {
  safeText($('#modeNote'), $('#schedulerInput').value === 'adaptive' ? 'Adaptive 會從 1 個活動額度開始，依診斷逐步調整至設定上限。' : '固定模式會使用指定數量的 Worker。');
});

function updateClock() {
  safeText($('#clockText'), new Date().toLocaleTimeString('zh-TW', { hour12: false }));
}

repoInput.value = localStorage.getItem('pbe.repo') || '';
sourceInput.value = localStorage.getItem('pbe.source') || '';
$('#workerInput').value = localStorage.getItem('pbe.workers') || '4';
$('#writerInput').value = localStorage.getItem('pbe.writer-workers') || '1';
$('#memoryInput').value = localStorage.getItem('pbe.memory') || '256';
updateClock();
window.setInterval(updateClock, 1000);
poll();
window.setInterval(poll, 1500);
