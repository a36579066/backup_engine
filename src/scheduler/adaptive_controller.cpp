#include "scheduler/adaptive_controller.hpp"

#include <utility>

namespace pbe::detail {

ControllerDecision AdaptiveController::evaluate(const WindowSample& sample, Diagnosis diagnosis,
                                                std::size_t current_limit, std::size_t max_limit,
                                                bool adaptive_enabled, bool cancelled) {
    ControllerDecision decision;
    decision.diagnosis = std::move(diagnosis);
    decision.active_limit = current_limit;

    // Compare adequately sampled windows with the same diagnosis; roll back sustained regressions.
    if (regression_watch_) {
        if (sample.completed_chunks >= 4 &&
            decision.diagnosis.classification == baseline_diagnosis_) {
            if (sample.throughput_bytes_per_sec < baseline_throughput_ * 0.80)
                ++regression_count_;
            else if (sample.throughput_bytes_per_sec >= baseline_throughput_ * 0.95)
                regression_count_ = 0;
            --windows_left_;
            if (regression_count_ >= 2) {
                decision.active_limit = rollback_limit_;
                decision.limit_changed = true;
                decision.action = "ROLLBACK_CPU_ADMISSION";
                decision.diagnosis.rule_id = "SUSTAINED_REGRESSION_R1";
                decision.diagnosis.reason =
                    "Two comparable windows showed sustained throughput regression after the previous limit change.";
                decision.outcome = "ROLLED_BACK_BEST_EFFORT";
                regression_watch_ = false;
                cooldown_ = 2;
            } else if (windows_left_ <= 0) {
                decision.outcome = regression_count_ ? "UNVERIFIABLE" : "NO_CLEAR_REGRESSION";
                regression_watch_ = false;
            } else {
                decision.outcome = "COMPARISON_PENDING";
            }
        } else {
            decision.outcome = "UNVERIFIABLE_DIFFERENT_WINDOW";
            regression_watch_ = false;
        }
    }

    // Warmup and cooldown damp short-term noise; change admission by one permit at a time.
    if (adaptive_enabled && !cancelled && decision.action == "HOLD") {
        if (warmup_ < 2) {
            ++warmup_;
            decision.diagnosis.rule_id = "WARMUP_HOLD";
            decision.diagnosis.reason = "The controller is collecting baseline windows.";
            decision.outcome = "WARMUP";
        } else if (sample.completed_chunks < 4) {
            decision.diagnosis.rule_id = "INSUFFICIENT_DATA";
            decision.diagnosis.reason =
                "Not enough completed chunks were measured; the current CPU limit is retained.";
            decision.outcome = "INSUFFICIENT_DATA";
        } else if (cooldown_ > 0) {
            --cooldown_;
            decision.diagnosis.rule_id = "COOLDOWN_HOLD";
            decision.diagnosis.reason = "The controller is waiting through its cooldown windows.";
            decision.outcome = "COOLDOWN";
        } else if ((decision.diagnosis.classification == "WRITER_LIMITED" ||
                    decision.diagnosis.classification == "MEMORY_LIMITED") &&
                   current_limit > 1) {
            decision.active_limit = current_limit - 1;
            decision.limit_changed = true;
            decision.action = "DECREASE_CPU_ADMISSION";
            decision.diagnosis.rule_id += "_DECREASE";
            decision.diagnosis.reason +=
                " CPU admission decreases by one permit to reduce downstream pressure.";
            rollback_limit_ = current_limit;
            baseline_throughput_ = sample.throughput_bytes_per_sec;
            baseline_diagnosis_ = decision.diagnosis.classification;
            regression_count_ = 0;
            windows_left_ = 2;
            regression_watch_ = true;
            cooldown_ = 2;
            decision.outcome = "PENDING_NEXT_WINDOW";
        } else if (decision.diagnosis.classification == "CPU_LIMITED" &&
                   current_limit < max_limit) {
            decision.active_limit = current_limit + 1;
            decision.limit_changed = true;
            decision.action = "INCREASE_CPU_ADMISSION";
            decision.diagnosis.rule_id += "_INCREASE";
            decision.diagnosis.reason +=
                " CPU admission increases by one permit while downstream queues have headroom.";
            rollback_limit_ = current_limit;
            baseline_throughput_ = sample.throughput_bytes_per_sec;
            baseline_diagnosis_ = decision.diagnosis.classification;
            regression_count_ = 0;
            windows_left_ = 2;
            regression_watch_ = true;
            cooldown_ = 2;
            decision.outcome = "PENDING_NEXT_WINDOW";
        }
    } else if (cancelled) {
        decision.outcome = "STOPPED_AFTER_ERROR";
    }

    return decision;
}

} // namespace pbe::detail
