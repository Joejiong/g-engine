#ifndef joewu_HAOKAN_REC_GRAPH_ENGINE_GRAPH_GRAPH_HPP
#define joewu_HAOKAN_REC_GRAPH_ENGINE_GRAPH_GRAPH_HPP

#include <joewu/graph/engine/graph.h>
#include <joewu/graph/engine/vertex.h>

namespace joewu {
namespace feed {
namespace graph {

size_t Graph::data_size() const noexcept {
    return _data.size();
}

size_t Graph::vertex_size() const noexcept {
    return _vertexes.size();
}

template <typename ...D>
Closure Graph::run(D... data) noexcept {
    GraphData* root_data[] = {data...};
    return run(root_data, sizeof(root_data) >> 3);
}

template <typename T>
T* Graph::context() noexcept {
    if (unlikely(!_context)) {
        _context = T();
    }
    return _context.get<T>();
}

template <typename T>
T* Graph::mutable_context() noexcept {
    if (unlikely(!_mutable_context)) {
        _mutable_context = T();
    }
    return _mutable_context.get<T>();
}

#ifdef GOOGLE_PROTOBUF_HAS_ARENAS
template <typename T, typename... Args>
inline ReusableAccessor<T> Graph::create(Args&&... args) {
    return _arena_mem_manager.create<T>(::std::forward<Args>(args)...);
}
#endif // GOOGLE_PROTOBUF_HAS_ARENAS

inline ::std::ostream& operator<<(::std::ostream& os, const GraphLog& graph_log) noexcept {
    if (graph_log.vertexes() != nullptr) {
        for (const auto& vertex : *(graph_log.vertexes())) {
            if (vertex.clog().empty()) {
                continue;
            } 
            os << vertex.clog();
        }
    }
    return os;
}

}  // graph
}  // feed
}  // joewu

#endif // joewu_HAOKAN_REC_GRAPH_ENGINE_GRAPH_GRAPH_HPP
