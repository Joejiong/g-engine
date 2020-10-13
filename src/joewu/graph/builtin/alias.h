#ifndef joewu_HAOKAN_REC_GRAPH_ENGINE_BUILTIN_ALIAS_H
#define joewu_HAOKAN_REC_GRAPH_ENGINE_BUILTIN_ALIAS_H

#include <joewu/graph/engine/vertex.h>

namespace joewu {
namespace feed {
namespace graph {
namespace builtin {

// 重命名算子
class AliasProcessor : public GraphProcessor {
public:
    inline static void apply(GraphBuilder&, const ::std::string& alias, const ::std::string& data) noexcept;

    virtual int32_t setup(GraphVertex&) const noexcept override;
    virtual int32_t on_activate(GraphVertex&) const noexcept override;
    virtual int32_t process(GraphVertex&) noexcept override;
private:
    static std::atomic<size_t> _g_idx;
};

} // builtin
} // graph
} // feed
} // joewu

#endif //joewu_HAOKAN_REC_GRAPH_ENGINE_BUILTIN_ALIAS_H

#include <joewu/graph/builtin/alias.hpp>
