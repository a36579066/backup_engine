#pragma once

#include "storage/object_store.hpp"

namespace pbe::detail {
// Pipeline tasks use completion callbacks; the optional promise is a reserved direct-caller fallback.
// ack is borrowed for this callback only; copy its contents if they must outlive the call.
inline void complete_blob_task(BlobWriteTask& task, const WriteAck* ack, std::exception_ptr error) {
    if (task.completion)
        task.completion(ack, error);
    else if (task.promise) {
        if (error)
            task.promise->set_exception(error);
        else if (ack)
            task.promise->set_value(*ack);
    }
}
} // namespace pbe::detail
