# Failure model

PBE handles process interruption and ordinary read/write errors by preserving already complete snapshots. Its durability model relies on SQLite WAL with `synchronous=FULL`, file `fsync`, immutable object files, and directory sync around object publication. It cannot promise behavior beyond the guarantees supplied by the operating system, filesystem, controller, and storage device. Network filesystems and cloud-synchronized folders are outside the supported contract.

## Recovery

The next writer process obtains the repository lock, marks leftover `IN_PROGRESS` snapshots as `FAILED`, and clears recognizable staging contents. In files-v1, unreferenced object files remain as orphans. In pack-v2, it also removes published packs that have no catalog object row; packs already indexed remain available even if their interrupted snapshot was not completed.

## Fault injection

`--failpoint` supports these test-only abrupt exits (exit 86): `after-temp-write`, `after-object-publish`, `before-sqlite-batch-commit`, `before-final-publish`, and `after-final-publish`. Use a disposable repository. The first four must leave the new snapshot invisible; after final publish the new complete snapshot must verify. These exits model process death, not power loss.

## Path and metadata boundary

Source symlinks are recorded as symlinks and are never followed. Restore traverses directory components with `openat` and `O_NOFOLLOW`, writes regular files to temporary names, and publishes with a no-replace hard link. Restore requires an absent or empty target and never overwrites existing paths. Basic mode and modification time are stored; ownership, ACLs, xattrs, Finder metadata, resource forks, and hardlink relationships are not preserved.
