#ifndef joewu_HAOKAN_REC_GRAPH_ENGINE_BUILTIN_SELECT_H
#define joewu_HAOKAN_REC_GRAPH_ENGINE_BUILTIN_SELECT_H

#include <joewu/graph/engine/vertex.h>

namespace joewu {
namespace feed {
namespace graph {
namespace builtin {

// 选择算子
// 从多个依赖中选择第一个满足条件的输出
class SelectProcessor : public GraphProcessor {
public:
    // 选择算子的配置
    struct Option {
        // 是否传递可变性，即是否对自身的依赖，声明为可变
        // 取决于自身的输出数据，是否被声明为可变
        // 传递可以避免进行拷贝，但是使依赖无法共享
        bool forward_mutable_declaration {true};
    };
    // 最常见的dest = cond ? true_src : false_src简写，默认option
    static void apply(GraphBuilder&, const ::std::string& dest, const ::std::string& cond,
            const ::std::string& true_src, const ::std::string& false_src) noexcept;
    // 最常见的dest = cond ? true_src : false_src简写，指定option
    static void apply(GraphBuilder&, Option&& option, const ::std::string& dest,
            const ::std::string& cond, const ::std::string& true_src,
            const ::std::string& false_src) noexcept;

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
#endif //joewu_HAOKAN_REC_GRAPH_ENGINE_BUILTIN_SELECT_H
