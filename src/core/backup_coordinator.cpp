#include "pbe/engine.hpp"
#include "core/backup_session.hpp"
#include "pipeline/backup_pipeline.hpp"

#include <exception>

namespace pbe {
BackupSummary backup(const BackupOptions& options) {
    // 1. Validate inputs and lock the repository; session owns snapshots, pipeline owns workers.
    detail::BackupSession session(detail::make_backup_plan(options));
    detail::BackupPipeline pipeline(session);
    session.begin_snapshot();

    try {
        // 2. Start stages, run the Reader, and drain work; cancel and join before reporting failure.
        pipeline.run();

        // 3. All workers have exited after run returns; the snapshot can now be published.
        session.publish_snapshot();
        return session.finish_summary(pipeline.context());
    } catch (...) {
        // Join workers before snapshot cleanup: they may still hold buffers or session callbacks.
        pipeline.cancel_and_join(std::current_exception());
        session.fail_snapshot();
        throw;
    }
}
} // namespace pbe
