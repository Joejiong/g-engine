#include <joewu/graph/builtin/select.h>

namespace joewu {
namespace feed {
namespace graph {
namespace builtin {

///////////////////////////////////////////////////////////////////////////////
// SelectProcessor begin
std::atomic<size_t> SelectProcessor::_g_idx;

int32_t SelectProcessor::setup(GraphVertex& vertex) const noexcept {
    if (vertex.anonymous_emit_size() != 1) {
        LOG(WARNING) << "emit num[" << vertex.anonymous_emit_size()
            << "] != 1 for " << vertex;
        return -1;
    }
    vertex.trivial();
    return 0;
}

int32_t SelectProcessor::on_activate(GraphVertex& vertex) const noexcept {
    // 如果传递可变性，则按照0号输出的可变性，设置所有依赖的可变性
    auto option = vertex.option<Option>();
    if (option == nullptr || option->forward_mutable_declaration) {
        bool need_mutable = vertex.anonymous_emit(0)->need_mutable();
        for (size_t i = 0; i < vertex.anonymous_dependency_size(); ++i) {
            vertex.anonymous_dependency(i)->declare_mutable(need_mutable);
        }
    }
    return 0;
}

int32_t SelectProcessor::process(GraphVertex& vertex) noexcept {
    for (size_t i = 0; i < vertex.anonymous_dependency_size(); ++i) {
        auto dependency = vertex.anonymous_dependency(i);
        if (dependency->ready()) {
            if (vertex.anonymous_emit(0)->forward(*dependency)) {
                return 0;
            } else {
                LOG(WARNING) << "forward dependency[" << i << "] failed";
                return -1;
            }
        }
    }
    LOG(WARNING) << "no dependency ready to forward";
    return -1;
}

void SelectProcessor::apply(GraphBuilder& builder, const ::std::string& dest,
        const ::std::string& cond, const ::std::string& true_src,
        const ::std::string& false_src) noexcept {
    apply(builder, Option(), dest, cond, true_src, false_src);
}
void SelectProcessor::apply(GraphBuilder& builder, Option&& option,
        const ::std::string& dest, const ::std::string& cond,
        const ::std::string& true_src, const ::std::string& false_src) noexcept {
    static SelectProcessor processor;
    auto& vertex = builder.add_vertex(processor);
    vertex.name(std::string("SelectProcessor").append(std::to_string(++SelectProcessor::_g_idx)));
    vertex.option(::std::move(option));
    vertex.anonymous_depend().to(true_src).on(cond);
    vertex.anonymous_depend().to(false_src).unless(cond);
    vertex.anonymous_emit().to(dest);
}
// AndProcessor end
///////////////////////////////////////////////////////////////////////////////

} // builtin
} // graph
} // feed
} // joewu
