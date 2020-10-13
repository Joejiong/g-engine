#include <joewu/graph/builtin/const.h>

namespace joewu {
namespace feed {
namespace graph {
namespace builtin {

int32_t ConstProcessor::setup(GraphVertex& vertex) const noexcept {
    if (vertex.anonymous_emit_size() != 1) {
        LOG(WARNING) << "emit num[" << vertex.anonymous_emit_size()
            << "] != 1 for " << vertex;
        return -1;
    }
    vertex.trivial();
    return 0;
}

int32_t ConstProcessor::process(GraphVertex& vertex) noexcept {
    auto option = vertex.option<Any>();
    vertex.anonymous_emit(0)->emit<Any>()->ref(*option);
    return 0;
}

} // builtin
} // graph
} // feed
} // joewu
