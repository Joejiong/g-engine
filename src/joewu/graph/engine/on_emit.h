#ifndef joewu_HAOKAN_REC_GRAPH_ENGINE_GRAPH_ON_EMIT_H
#define joewu_HAOKAN_REC_GRAPH_ENGINE_GRAPH_ON_EMIT_H

#include <functional>
#include <joewu/feed/mlarch/babylon/any.h>

namespace joewu {
namespace feed {
namespace graph {
using ::joewu::feed::mlarch::babylon::Any;

class GraphVertex;
typedef ::std::function<void(GraphVertex&, const Any&)> OnEmitFunction;

}
}
}
#endif