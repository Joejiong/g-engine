#include <joewu/graph/builtin/logical.h>

namespace joewu {
namespace feed {
namespace graph {
namespace builtin {

///////////////////////////////////////////////////////////////////////////////
// AndProcessor begin
int32_t AndProcessor::setup(GraphVertex& vertex) const noexcept {
    if (vertex.anonymous_dependency_size() < 2) {
        LOG(WARNING) << "dependency num[" << vertex.anonymous_dependency_size()
            << "] < 2 for " << vertex;
        return -1;
    }
    if (vertex.anonymous_emit_size() != 1) {
        LOG(WARNING) << "emit num[" << vertex.anonymous_emit_size()
            << "] != 1 for " << vertex;
        return -1;
    }
    vertex.trivial();
    return 0;
}

int32_t AndProcessor::process(GraphVertex& vertex) noexcept {
    auto committer = vertex.anonymous_emit(0)->emit<bool>();
    auto& value = *committer = true;
    for (size_t i = 0; i < vertex.anonymous_dependency_size(); ++i) {
        auto dependency_value = vertex.anonymous_dependency(i)->value<bool>();
        if (unlikely(dependency_value == nullptr)) {
            LOG(WARNING) << "dependency[" << i << "] not ready";
            committer.cancel();
            return -1;
        }
        value &= *dependency_value;
    }
    return 0;
}

void AndProcessor::apply(GraphBuilder& builder, const ::std::string& dest,
    const ::std::string& src1, const ::std::string& src2) noexcept {
    static AndProcessor processor;
    auto& vertex = builder.add_vertex(processor);
    vertex.anonymous_depend().to(src1);
    vertex.anonymous_depend().to(src2);
    vertex.anonymous_emit().to(dest);
}
// AndProcessor end
///////////////////////////////////////////////////////////////////////////////

///////////////////////////////////////////////////////////////////////////////
// OrProcessor begin
int32_t OrProcessor::setup(GraphVertex& vertex) const noexcept {
    if (vertex.anonymous_dependency_size() < 2) {
        LOG(WARNING) << "dependency num[" << vertex.anonymous_dependency_size()
            << "] < 2 for " << vertex;
        return -1;
    }
    if (vertex.anonymous_emit_size() != 1) {
        LOG(WARNING) << "emit num[" << vertex.anonymous_emit_size()
            << "] != 1 for " << vertex;
        return -1;
    }
    vertex.trivial();
    return 0;
}

int32_t OrProcessor::process(GraphVertex& vertex) noexcept {
    auto committer = vertex.anonymous_emit(0)->emit<bool>();
    auto& value = *committer = false;
    for (size_t i = 0; i < vertex.anonymous_dependency_size(); ++i) {
        auto dependency_value = vertex.anonymous_dependency(i)->value<bool>();
        if (unlikely(dependency_value == nullptr)) {
            LOG(WARNING) << "dependency[" << i << "] not ready";
            committer.cancel();
            return -1;
        }
        value |= *dependency_value;
    }
    return 0;
}

void OrProcessor::apply(GraphBuilder& builder, const ::std::string& dest,
    const ::std::string& src1, const ::std::string& src2) noexcept {
    static OrProcessor processor;
    auto& vertex = builder.add_vertex(processor);
    vertex.anonymous_depend().to(src1);
    vertex.anonymous_depend().to(src2);
    vertex.anonymous_emit().to(dest);
}
// OrProcessor end
///////////////////////////////////////////////////////////////////////////////

///////////////////////////////////////////////////////////////////////////////
// NotProcessor begin
int32_t NotProcessor::setup(GraphVertex& vertex) const noexcept {
    if (vertex.anonymous_dependency_size() != 1) {
        LOG(WARNING) << "dependency num[" << vertex.anonymous_dependency_size()
            << "] != 1 for " << vertex;
        return -1;
    }
    if (vertex.anonymous_emit_size() != 1) {
        LOG(WARNING) << "emit num[" << vertex.anonymous_emit_size()
            << "] != 1 for " << vertex;
        return -1;
    }
    vertex.trivial();
    return 0;
}

int32_t NotProcessor::process(GraphVertex& vertex) noexcept {
    auto source = vertex.anonymous_dependency(0)->value<bool>();
    if (unlikely(source == nullptr)) {
        LOG(WARNING) << "dependency[0] not ready";
        return -1;
    }
    *vertex.anonymous_emit(0)->emit<bool>() = !*source;
    return 0;
}

void NotProcessor::apply(GraphBuilder& builder, const ::std::string& dest,
    const ::std::string& src) noexcept {
    static NotProcessor processor;
    auto& vertex = builder.add_vertex(processor);
    vertex.anonymous_depend().to(src);
    vertex.anonymous_emit().to(dest);
}
// NotProcessor end
///////////////////////////////////////////////////////////////////////////////

} // builtin
} // graph
} // feed
} // joewu
