#include <joewu/graph/engine/data.h>
#include <joewu/graph/engine/vertex.h>
#include <joewu/graph/engine/closure.h>
#include <joewu/graph/engine/dependency.h>

namespace joewu {
namespace feed {
namespace graph {

void GraphData::release() noexcept {
    ClosureContext* closure = _closure.load(::std::memory_order_relaxed);
    do {
        if (unlikely(closure == SEALED_CLOSURE)) {
            LOG(FATAL) << "data[" << _name << "] double release, maybe a bug?";
            abort();
        }
    } while (unlikely(!_closure.compare_exchange_weak(closure,
                SEALED_CLOSURE, ::std::memory_order_acq_rel)));

    if (unlikely(nullptr != closure)) {
        closure->depend_data_sub();
    }
    if (_on_emit){
        (*_on_emit)(*(_producer), _data);
    }
    BABYLON_STACK(GraphVertex*, runnable_vertexes, _vertex_num);
    auto trivial_runnable_vertexes = _producer != nullptr ? _producer->runnable_vertexes()
        : nullptr;
    if (trivial_runnable_vertexes == nullptr) {
        for (auto successor : _successors) {
            successor->ready(this, runnable_vertexes);
        }
    } else {
        for (auto successor : _successors) {
            successor->ready(this, *trivial_runnable_vertexes);
        }
    }
    while (!runnable_vertexes.empty()) {
        auto vertex = runnable_vertexes.back();
        runnable_vertexes.pop_back();
        vertex->invoke(runnable_vertexes);
    }
}

int32_t GraphData::recursive_activate(Stack<GraphVertex*>& runnable_vertexes, ClosureContext* closure) noexcept {
    LOG(TRACE) << "recursive activation from data " << _name;
    BABYLON_STACK(GraphData*, activating_data, _data_num);
    trigger(activating_data);
    while (!activating_data.empty()) {
        GraphData* one_data = activating_data.back();
        activating_data.pop_back();
        if (0 != one_data->activate(activating_data, runnable_vertexes, closure)) {
            LOG(WARNING) << "activate " << *one_data << " failed";
            return -1;
        }
    }
    return 0;
}

ClosureContext* GraphData::SEALED_CLOSURE =
    reinterpret_cast<ClosureContext*>(0xFFFFFFFFFFFFFFFFL);

} // graph
} // feed
} // joewu
