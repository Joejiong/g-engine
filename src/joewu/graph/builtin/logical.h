#ifndef joewu_HAOKAN_REC_GRAPH_ENGINE_BUILTIN_LOGICAL_H
#define joewu_HAOKAN_REC_GRAPH_ENGINE_BUILTIN_LOGICAL_H

#include <joewu/graph/engine/vertex.h>

namespace joewu {
namespace feed {
namespace graph {
namespace builtin {

class AndProcessor : public GraphProcessor {
public:
    static void apply(GraphBuilder&,
        const ::std::string& dest, const ::std::string& src1, const ::std::string& src2) noexcept;
    virtual int32_t setup(GraphVertex&) const noexcept override;
    virtual int32_t process(GraphVertex&) noexcept override;
};

class OrProcessor : public GraphProcessor {
public:
    static void apply(GraphBuilder&,
        const ::std::string& dest, const ::std::string& src1, const ::std::string& src2) noexcept;
    virtual int32_t setup(GraphVertex&) const noexcept override;
    virtual int32_t process(GraphVertex&) noexcept override;
};

class NotProcessor : public GraphProcessor {
public:
    static void apply(GraphBuilder&,
        const ::std::string& dest, const ::std::string& src) noexcept;
    virtual int32_t setup(GraphVertex&) const noexcept override;
    virtual int32_t process(GraphVertex&) noexcept override;
};

} // builtin
} // graph
} // feed
} // joewu
#endif //joewu_HAOKAN_REC_GRAPH_ENGINE_BUILTIN_LOGICAL_H
