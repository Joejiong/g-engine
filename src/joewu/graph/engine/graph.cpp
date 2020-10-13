#include <joewu/graph/engine/graph.h>
#include <joewu/graph/engine/data.h>
#include <joewu/graph/engine/vertex.h>
#include <joewu/graph/engine/executor.h>

namespace joewu {
namespace feed {
namespace graph {

Graph::Graph(GraphExecutor& executer,
    size_t vertex_size,
    const ::std::unordered_map<::std::string, size_t>& data_index_by_name) noexcept :
    _executor(&executer), _vertexes(vertex_size), 
    _data(data_index_by_name.size()) {
    for (const auto& pair : data_index_by_name) {
        auto& data = _data[pair.second];
        data.executer(*_executor);
        data.name(pair.first);
        data.data_num(_data.size());
        data.vertex_num(_vertexes.size());
        _data_by_name.emplace(::std::make_pair(pair.first, &data));
    }
}

::std::vector<GraphData>& Graph::data() noexcept {
    return _data;
}

::std::vector<GraphVertex>& Graph::vertexes() noexcept {
    return _vertexes;
}

GraphData* Graph::find_data(const ::std::string& name) noexcept {
    auto it = _data_by_name.find(name);
    if (unlikely(it == _data_by_name.end())) {
        LOG(WARNING) << "no data named " << name << " in graph";
        return nullptr;
    }
    return it->second;
}

int Graph::func_each_vertex(std::function<int(GraphVertex&)> func) noexcept {
    for (auto& vertex : _vertexes) {
        if (func(vertex) != 0) {
            LOG(WARNING) << "func on vertex[" << vertex.index() << "] failed";
            return -1; 
        }
    } 

    return 0;
}

void Graph::reset() noexcept {
    for (auto& one_data : _data) {
        one_data.reset();
    }
    for (auto& vertex : _vertexes) {
        vertex.reset();
    }
    #ifdef GOOGLE_PROTOBUF_HAS_ARENAS
    _arena_mem_manager.clear();
    #endif // GOOGLE_PROTOBUF_HAS_ARENAS
}

Closure Graph::run(GraphData* data[], size_t size) noexcept {
    LOG(TRACE) << "run graph for " << size << " data";
    auto closure = _executor->create_closure();
    auto context = closure.context();
    context->all_data_num(_data.size());
    BABYLON_STACK(GraphVertex*, runnable_vertexes, _vertexes.size());
    for (size_t i = 0; i < size; ++i) {
        if (unlikely(!data[i]->bind(*context))) {
            continue;
        }
        if (0 != data[i]->recursive_activate(runnable_vertexes, context)) {
            LOG(WARNING) << "activate " << *data[i] << " failed";
            context->finish(-1);
            context->fire();
            return closure;
        }
    }

    LOG(TRACE) << "vertexes size:" << runnable_vertexes.size();
    // lijiang01 todo: 区分是否是trivial算子，先启动其他算子的异步执行
    // 最后再串行执行这些trivial算子
    while (!runnable_vertexes.empty()) {
        auto vertex = runnable_vertexes.back();
        runnable_vertexes.pop_back();
        vertex->invoke(runnable_vertexes);
    }
    context->fire();
    return closure;
}

} // graph
} // feed
} // joewu
