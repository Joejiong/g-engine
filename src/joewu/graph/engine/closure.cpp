#include <joewu/graph/engine/closure.h>
#include <joewu/graph/engine/data.h>

namespace joewu {
namespace feed {
namespace graph {

ClosureContext::~ClosureContext() noexcept {
    if (_flush_callback != nullptr) {
        delete _flush_callback;
    }
}

void ClosureContext::log_unfinished_data() const noexcept {
    BABYLON_STACK(const GraphData*, unfinished_data, _all_data_num);
    for (auto one_waiting_data : _waiting_data) {
        if (!one_waiting_data->ready()) {
            unfinished_data.emplace_back(one_waiting_data);
        }
    }

    ::std::unordered_set<const GraphData*> checked_unfinished_data;
    LOG(WARNING) << "all vertex finish but data not ready " << noflush;
    while (!unfinished_data.empty()) {
        auto data = unfinished_data.back();
        unfinished_data.pop_back();
        if (data->ready()) {
            continue;
        }
        
        auto producer = data->producer();
        if (!producer) {
            continue;
        }

        // 先设置依赖ready
        bool dependencies_ready = true;
        for (auto& dependency : producer->dependencies()) {
            auto condition = dependency.inner_condition();
            auto target = dependency.inner_target();
            // 条件不ready
            if (condition != nullptr && !condition->ready()) {
                dependencies_ready = false;
                // 进一步检测依赖数据
                if (checked_unfinished_data.count(condition) == 0) {
                    unfinished_data.emplace_back(condition);
                    checked_unfinished_data.emplace(condition);
                }
            // 目标不ready
            } else if (!target->ready()) {
                dependencies_ready = false;
                // 进一步检测条件
                if (checked_unfinished_data.count(target) == 0) {
                    unfinished_data.emplace_back(target);
                    checked_unfinished_data.emplace(target);
                }
            }
        }
        // 依赖ready，但是自身没有ready，问题数据
        if (dependencies_ready) {
            LOG(WARNING) << *data << " " << noflush;
        }
    }
    LOG(WARNING);
}

ClosureCallback* ClosureContext::SEALED_CALLBACK =
    reinterpret_cast<ClosureCallback*>(0xFFFFFFFFFFFFFFFFL);

} // graph
} // feed
} // joewu
