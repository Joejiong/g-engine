#ifndef joewu_HAOKAN_REC_GRAPH_ENGINE_BUILTIN_ALIAS_HPP
#define joewu_HAOKAN_REC_GRAPH_ENGINE_BUILTIN_ALIAS_HPP

namespace joewu {
namespace feed {
namespace graph {
namespace builtin {
void AliasProcessor::apply(GraphBuilder& builder, const ::std::string& alias,
        const ::std::string& name) noexcept {
    static AliasProcessor processor;
    auto& vertex = builder.add_vertex(processor);
    vertex.name(std::string("AliasProcessor").append(std::to_string(++AliasProcessor::_g_idx)));
    vertex.anonymous_depend().to(name);
    vertex.anonymous_emit().to(alias);
}

} // builtin
} // graph
} // feed
} // joewu

#endif // joewu_HAOKAN_REC_GRAPH_ENGINE_BUILTIN_ALIAS_HPP
