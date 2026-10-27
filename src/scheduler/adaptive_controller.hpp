#pragma once

#include "diagnostics/diagnostics.hpp"

#include <cstddef>
#include <string>

namespace pbe::detail {

struct ControllerDecision {
    Diagnosis diagnosis;
    std::size_t active_limit = 1;
    std::string action = "HOLD";
    std::string outcome = "OBSERVED";
    bool limit_changed = false;
};

// Choose CPU admission from consecutive windows without directly manipulating threads or queues.
// The monitor applies decisions to ActiveWorkerGate and records their reasons and outcomes.
class AdaptiveController {
public:
    ControllerDecision evaluate(const WindowSample& sample, Diagnosis diagnosis,
                                std::size_t current_limit, std::size_t max_limit,
                                bool adaptive_enabled, bool cancelled);

private:
    unsigned warmup_ = 0;
    unsigned cooldown_ = 0;
    bool regression_watch_ = false;
    std::size_t rollback_limit_ = 1;
    double baseline_throughput_ = 0;
    std::string baseline_diagnosis_;
    int regression_count_ = 0;
    int windows_left_ = 0;
};

} // namespace pbe::detail
