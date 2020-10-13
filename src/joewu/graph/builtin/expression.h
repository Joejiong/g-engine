#ifndef joewu_HAOKAN_REC_GRAPH_ENGINE_BUILTIN_EXPRESSION_H
#define joewu_HAOKAN_REC_GRAPH_ENGINE_BUILTIN_EXPRESSION_H

#include <joewu/graph/engine/vertex.h>

namespace joewu {
namespace feed {
namespace graph {
namespace builtin {

// 表达式算子，按照一个expression进行运算，并输出结果的算子
// 支持典型的四则和逻辑运算
// 优先级分组参考https://baike.joewu.com/item/运算符优先级#2
// 使用::boost::spirit驱动解析，对应的BNF如下
// expression[n]中的n表示对应运算符的优先级
//
// root ::= expression[13]
// expression[13] ::= expression[12] ["?" expression[12] ":" expression[12]]
// expression[12] ::= expression[11] {"||" expression[11]}
// expression[11] ::= expression[7] {"&&" expression[7]}
// expression[7] ::= expression[6] {("==" | "!=") expression[6]}
// expression[6] ::= expression[4] {(">" | ">=" | "<" | "<=") expression[4]}
// expression[4] ::= expression[3] {("+" | "-") expression[3]}
// expression[3] ::= expression[2] {("*" | "/") expression[2]}
// expression[2] ::= [("!" | "-")] expression[1]
// expression[1] ::= bool | long | double | variable | "(" root ")"
// variable ::= (alpha | "_") {alnum | "_"}
//
// 例如某个Graph中有A、B、C三个GraphData
// 则可能的有效表达式为
// (A - 3) * 5 < 10 || B == 6 ? X + 4 : Y + 3
class ExpressionProcessor : public GraphProcessor {
public:
    // 遍历图中所有依赖节点，对于其中的表达式条件
    // 如果尚未有节点进行输出，则补充表达式算子节点进行产出
    static int32_t apply(GraphBuilder& builder) noexcept;
    // 根据表达式，创建一个表达式算子来产出表达式数据，产出数据命名为result_name
    // 产出按照expression_string进行计算
    static int32_t apply(GraphBuilder& builder, const ::std::string& result_name,
        const ::std::string& expression_string) noexcept;
    // 等同于apply(builder, expression_string, expression_string)
    // 显示指定result_name更明确，此api作废处理
    static __attribute__((deprecated)) int32_t apply(GraphBuilder& builder,
        const ::std::string& expression_string) noexcept;

    virtual int32_t setup(GraphVertex&) const noexcept override;
    virtual int32_t process(GraphVertex&) noexcept override;

private:
    // 设置比较复杂，不开放直接创建功能，通过apply来使用
    inline ExpressionProcessor() noexcept = default;

    // 级联展开表达式
    static int32_t expend_expression(GraphBuilder& builder,
        ::std::unordered_set<::std::string>& emits,
        const ::std::string& result_name,
        const ::std::string& expression_string) noexcept;
    // 展开一层条件表达式
    static int32_t expend_conditional_expression(
        GraphBuilder& builder,
        ::std::unordered_set<::std::string>& emits,
        ::std::vector<::std::tuple<::std::string, ::std::string>>& unsolved_expressions,
        const ::std::string& result_name,
        const ::std::string& expression_string) noexcept;
    // 展开非条件表达式，终止到基础常/变量，或者内嵌条件表达式
    static int32_t expend_non_conditional_expression(
        GraphBuilder& builder,
        ::std::unordered_set<::std::string>& emits,
        ::std::vector<::std::tuple<::std::string, ::std::string>>& unsolved_expressions,
        const ::std::string& result_name,
        const ::std::string& expression_string) noexcept;
    static std::atomic<size_t> _g_idx;
};

} // builtin
} // graph
} // feed
} // joewu
#endif //joewu_HAOKAN_REC_GRAPH_ENGINE_BUILTIN_EXPRESSION_H
