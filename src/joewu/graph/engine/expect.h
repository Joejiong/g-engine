#ifndef joewu_HAOKAN_REC_GRAPH_ENGINE_GRAPH_EXPECT_H
#define joewu_HAOKAN_REC_GRAPH_ENGINE_GRAPH_EXPECT_H

#include <joewu/feed/mlarch/babylon/expect.h>

namespace joewu {
namespace feed {
namespace graph {

#if BABYLON_DEFINE_EXPECT == 0
// 其他lib也可能定义了likely和unlikely，避免冲突
#ifndef likely
using ::joewu::feed::mlarch::babylon::likely;
#endif
#ifndef unlikely
using ::joewu::feed::mlarch::babylon::unlikely;
#endif
#endif // BABYLON_DEFINE_EXPECT

}  // graph
}  // feed
}  // joewu

#endif  // joewu_HAOKAN_REC_GRAPH_ENGINE_GRAPH_EXPECT_H
