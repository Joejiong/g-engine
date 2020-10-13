#include <joewu/graph/engine/vertex.h>
#include <joewu/graph/engine/executor.h>
#include <joewu/graph/engine/expect.h>

namespace joewu {
namespace feed {
namespace graph {

////////////////////////////////////////////////////////////////////////////////
// GraphProcessor begin
GraphProcessor::~GraphProcessor() noexcept {}

int32_t GraphProcessor::setup(GraphVertex&) const noexcept {
    return 0;
}

int32_t GraphProcessor::on_activate(GraphVertex&) const noexcept {
    return 0;
}

void GraphProcessor::process(GraphVertex& vertex, GraphVertexClosure&& closure) noexcept {
    closure.done(process(vertex));
}

int32_t GraphProcessor::process(GraphVertex&) noexcept {
    return 0;
}
// GraphProcessor end
////////////////////////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////////////////////////
// GraphFunction begin
int32_t GraphFunction::setup(GraphVertex& vertex) const noexcept {
    const_cast<GraphFunction*>(this)->_vertex = &vertex;
    if (0 != const_cast<GraphFunction*>(this)->__babylon_auto_declare_interface(vertex)) {
        return -1;
    }
    if (0 != const_cast<GraphFunction*>(this)->setup(*vertex.option<Any>())) {
        return -1;
    }
    return 0;
}

int32_t GraphFunction::on_activate(GraphVertex& vertex) const noexcept {
    return GraphProcessor::on_activate(vertex);
}

void GraphFunction::process(GraphVertex& vertex, GraphVertexClosure&& closure) noexcept {
    GraphProcessor::process(vertex, ::std::move(closure));
}

int32_t GraphFunction::process(GraphVertex& vertex) noexcept {
    int32_t ret = __babylon_auto_prepare_input();
    if (ret != 0) {
        LOG(WARNING) << "auto prepare input for " << vertex << " failed";
        return -1;
    }
    return (*this)();
}

void GraphFunction::reset(GraphVertex&) const noexcept {
    const_cast<GraphFunction*>(this)->reset();
}

int32_t GraphFunction::__babylon_auto_declare_interface(GraphVertex&) noexcept {
    return 0;
}

int32_t GraphFunction::__babylon_auto_prepare_input() noexcept {
    return 0;
}

int32_t GraphFunction::setup(const Any&) noexcept {
    return 0;
}

int32_t GraphFunction::operator()() noexcept {
    return 0;
}

void GraphFunction::reset() noexcept {
}
// GraphFunction end
////////////////////////////////////////////////////////////////////////////////

///////////////////////////////////////////////////////////////////////////////
// GraphVertex begin
int32_t GraphVertex::activate(Stack<GraphData*>& activating_data,
    Stack<GraphVertex*>& runnable_vertexes, ClosureContext* closure) noexcept {
    LOG(TRACE) << "activating vertex[" << _index << "]";
    // 只能激活一次
    bool expected = false;
    if (!_activated.compare_exchange_strong(expected, true, ::std::memory_order_relaxed)) {
        LOG(DEBUG) << "vertex[" << _index << "] already activated skip activation";
        return 0;
    }

    // 记录激活vertex的closure
    _closure = closure;

    // 无依赖直接记入可执行列表
    int64_t waiting_num = _dependencies.size();
    if (waiting_num == 0) {
        LOG(DEBUG) << "vertex[" << _index << "] ready to run";
        runnable_vertexes.emplace(this);
        return 0;
    }
    _waiting_num.store(waiting_num, ::std::memory_order_relaxed);

    if (0 != _processor->on_activate(*this)) {
        LOG(WARNING) << "on_activate callback execute failed for " << *this;
        return -1;
    }

    // 激活每个依赖，记录激活时已经就绪的数目
    int64_t finished = 0;
    for (auto& dependency : _dependencies) {
        int64_t ret = dependency.activate(activating_data);
        if (unlikely(ret < 0)) {
            return ret;
        }
        finished += ret;
    }

    // 去掉已经就绪的数目，如果全部就绪，节点加入待运行集合
    if (finished > 0) {
        waiting_num = _waiting_num.fetch_sub(finished, ::std::memory_order_acq_rel) - finished;
        if (waiting_num == 0) {
            LOG(TRACE) << *this << " ready to run " << runnable_vertexes.size() << " / " << runnable_vertexes.capacity();
            runnable_vertexes.emplace(this);
            return 0;
        }
    }

    LOG(TRACE) << *this << " activated waiting";
    return 0;
}

GraphProcessor GraphVertex::DEFAULT_EMPTY_PROCESSOR;
} // graph
} // feed
} // joewu
