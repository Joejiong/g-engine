#ifndef joewu_HAOKAN_REC_GRAPH_ENGINE_BUILTIN_CONST_H
#define joewu_HAOKAN_REC_GRAPH_ENGINE_BUILTIN_CONST_H

#include <joewu/graph/engine/vertex.h>

namespace joewu {
namespace feed {
namespace graph {
namespace builtin {

// 常量算子
class ConstProcessor : public GraphProcessor {
public:
    template <typename T>
    inline static void apply(GraphBuilder&, const ::std::string& data, T&& value) noexcept;

    virtual int32_t setup(GraphVertex&) const noexcept override;
    virtual int32_t process(GraphVertex&) noexcept override;
private:
    std::atomic<size_t> _g_idx;
};

} // builtin
} // graph
} // feed
} // joewu

#endif //joewu_HAOKAN_REC_GRAPH_ENGINE_BUILTIN_CONST_H

#include <joewu/graph/builtin/const.hpp>
