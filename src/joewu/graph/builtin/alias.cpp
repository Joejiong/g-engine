#include <joewu/graph/builtin/alias.h>

namespace joewu {
namespace feed {
namespace graph {
namespace builtin {

namespace {
struct Context {
    GraphDependency* source;
    GraphData* target;
};
}
std::atomic<size_t> AliasProcessor::_g_idx;
int32_t AliasProcessor::setup(GraphVertex& vertex) const noexcept {
    auto context = vertex.context<Context>();
    context->source = vertex.anonymous_dependency(0);
    if (unlikely(context->source == nullptr)) {
        LOG(WARNING) << "depend num[" << vertex.anonymous_dependency_size()
            << "] != 1 for " << vertex;
        return -1;
    }
    context->target = vertex.anonymous_emit(0);
    if (unlikely(context->target == nullptr)) {
        LOG(WARNING) << "emit num[" << vertex.anonymous_emit_size()
            << "] != 1 for " << vertex;
        return -1;
    }
    vertex.trivial();
    return 0;
}

int32_t AliasProcessor::on_activate(GraphVertex& vertex) const noexcept {
    auto context = vertex.context<Context>();
    bool need_mutable = context->target->need_mutable();
    context->source->declare_mutable(need_mutable);
    return 0;
}

int32_t AliasProcessor::process(GraphVertex& vertex) noexcept {
    auto context = vertex.context<Context>();
    context->target->forward(
        *(context->source));
    return 0;
}

} // builtin
} // graph
} // feed
} // joewu
