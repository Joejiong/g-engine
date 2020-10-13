#ifndef joewu_HAOKAN_REC_GRAPH_ENGINE_BUILTIN_CONST_HPP
#define joewu_HAOKAN_REC_GRAPH_ENGINE_BUILTIN_CONST_HPP

namespace joewu {
namespace feed {
namespace graph {
namespace builtin {

template <typename T>
void ConstProcessor::apply(GraphBuilder& builder, const ::std::string& name,
        T&& value) noexcept {
    static ConstProcessor processor;
    auto& vertex = builder.add_vertex(processor);
    vertex.name(std::string("ConstProcessor").append(std::to_string(++processor._g_idx)));
    vertex.option(::std::forward<T>(value));
    vertex.anonymous_emit().to(name);
}

} // builtin
} // graph
} // feed
} // joewu

#endif // joewu_HAOKAN_REC_GRAPH_ENGINE_BUILTIN_CONST_HPP
