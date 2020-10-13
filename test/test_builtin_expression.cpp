#include <gflags/gflags.h>
#include <gtest/gtest.h>
#include <base/logging.h>
#include <joewu/graph/engine/builder.h>
#include <joewu/graph/builtin/expression.h>

#include <boost/spirit/include/qi.hpp>
#include <boost/fusion/include/boost_tuple.hpp>
using ::joewu::feed::graph::GraphBuilder;
using ::joewu::feed::graph::GraphVertex;
using ::joewu::feed::graph::GraphProcessor;
using ::joewu::feed::graph::BthreadGraphExecutor;
using ::joewu::feed::graph::builtin::ExpressionProcessor;
using ::joewu::feed::mlarch::babylon::Any;

BthreadGraphExecutor executor;

class OneProcessor : public GraphProcessor {
    virtual int32_t process(GraphVertex& vertex) noexcept override {
        auto depend = vertex.anonymous_dependency(0);
        auto emit = vertex.anonymous_emit(0);
        if (depend != nullptr && emit != nullptr) {
            LOG(NOTICE) << "forward";
            emit->forward(*depend);
        } else {
            LOG(NOTICE) << "none";
        }
        return 0;
    }
};

class MProcessor : public GraphProcessor {
    virtual int32_t setup(GraphVertex& vertex) const noexcept override {
        auto depend = vertex.anonymous_dependency(0);
        depend->declare_mutable();
        return 0;
    }

    virtual int32_t process(GraphVertex& vertex) noexcept override {
        auto depend = vertex.anonymous_dependency(0);
        auto emit = vertex.anonymous_emit(0);
        if (depend != nullptr && emit != nullptr) {
            LOG(NOTICE) << "forward";
            emit->forward(*depend);
        } else {
            LOG(NOTICE) << "none";
        }
        return 0;
    }
};

TEST(expression, do_correct_caculation) {
    ::std::string exp = "!(A > 3) || B + 1 == C * 3";
    GraphBuilder builder;
    builder.executor(executor);
    ASSERT_EQ(0, ExpressionProcessor::apply(builder, "D", exp));
    ASSERT_EQ(0, builder.finish());
    auto graph = builder.build();
    ASSERT_NE(nullptr, graph.get());
    *(graph->find_data("A")->emit<double>()) = 3.5;
    *(graph->find_data("B")->emit<int32_t>()) = 5;
    *(graph->find_data("C")->emit<int32_t>()) = 3;
    ASSERT_EQ(0, graph->run(graph->find_data("D")).get());
    ASSERT_FALSE(graph->find_data("D")->as<bool>());
    graph->reset();
    *(graph->find_data("A")->emit<double>()) = 2.5;
    *(graph->find_data("B")->emit<int32_t>()) = 5;
    *(graph->find_data("C")->emit<int32_t>()) = 3;
    ASSERT_EQ(0, graph->run(graph->find_data("D")).get());
    ASSERT_TRUE(graph->find_data("D")->as<bool>());
    graph->reset();
    *(graph->find_data("A")->emit<double>()) = 3.5;
    *(graph->find_data("B")->emit<int32_t>()) = 5;
    *(graph->find_data("C")->emit<int32_t>()) = 2;
    ASSERT_EQ(0, graph->run(graph->find_data("D")).get());
    ASSERT_TRUE(graph->find_data("D")->as<bool>());
}

TEST(expression, type_may_raise_in_caculation) {
    ::std::string exp = "A + B";
    GraphBuilder builder;
    builder.executor(executor);
    ASSERT_EQ(0, ExpressionProcessor::apply(builder, "D", exp));
    ASSERT_EQ(0, builder.finish());
    auto graph = builder.build();
    ASSERT_NE(nullptr, graph.get());
    *(graph->find_data("A")->emit<int32_t>()) = 1;
    *(graph->find_data("B")->emit<int64_t>()) = 2;
    ASSERT_EQ(0, graph->run(graph->find_data("D")).get());
    ASSERT_NE(nullptr, graph->find_data("D")->cvalue<int64_t>());
    graph->reset();
    *(graph->find_data("A")->emit<int64_t>()) = 1;
    *(graph->find_data("B")->emit<uint64_t>()) = 2;
    ASSERT_EQ(0, graph->run(graph->find_data("D")).get());
    ASSERT_NE(nullptr, graph->find_data("D")->cvalue<uint64_t>());
    graph->reset();
    *(graph->find_data("A")->emit<int64_t>()) = 1;
    *(graph->find_data("B")->emit<double>()) = 2;
    ASSERT_EQ(0, graph->run(graph->find_data("D")).get());
    ASSERT_NE(nullptr, graph->find_data("D")->cvalue<double>());
}

TEST(expression, logic_operator_emit_bool) {
    ::std::string exp = "A >= B";
    GraphBuilder builder;
    builder.executor(executor);
    ASSERT_EQ(0, ExpressionProcessor::apply(builder, "D", exp));
    ASSERT_EQ(0, builder.finish());
    auto graph = builder.build();
    ASSERT_NE(nullptr, graph.get());
    *(graph->find_data("A")->emit<int32_t>()) = 1;
    *(graph->find_data("B")->emit<int64_t>()) = 2;
    ASSERT_EQ(0, graph->run(graph->find_data("D")).get());
    ASSERT_NE(nullptr, graph->find_data("D")->cvalue<bool>());
}

TEST(expression, unary_operator_on_string_like_bool) {
    GraphBuilder builder;
    builder.executor(executor);
    ASSERT_EQ(0, ExpressionProcessor::apply(builder, "C", "!A"));
    ASSERT_EQ(0, ExpressionProcessor::apply(builder, "D", "-B"));
    ASSERT_EQ(0, ExpressionProcessor::apply(builder, "E", R"(-"some text")"));
    ASSERT_EQ(0, builder.finish());
    auto graph = builder.build();
    ASSERT_NE(nullptr, graph.get());
    *(graph->find_data("A")->emit<::std::string>()) = "123";
    *(graph->find_data("B")->emit<::std::string>()) = "";
    ASSERT_EQ(0, graph->run(graph->find_data("C"),
            graph->find_data("D"), graph->find_data("E")).get());
    ASSERT_NE(Any::Type::INSTANCE, graph->find_data("C")->cvalue<Any>()->type());
    ASSERT_FALSE(graph->find_data("C")->as<bool>());
    ASSERT_NE(Any::Type::INSTANCE, graph->find_data("D")->cvalue<Any>()->type());
    ASSERT_FALSE(graph->find_data("D")->as<bool>());
    ASSERT_NE(Any::Type::INSTANCE, graph->find_data("E")->cvalue<Any>()->type());
    ASSERT_TRUE(graph->find_data("E")->as<bool>());
}

TEST(expression, binary_operator_on_instance_support_string_pair_only) {
    GraphBuilder builder;
    builder.executor(executor);
    ASSERT_EQ(0, ExpressionProcessor::apply(builder, "C", "A + B"));
    ASSERT_EQ(0, ExpressionProcessor::apply(builder, "D", "A <= B"));
    ASSERT_EQ(0, ExpressionProcessor::apply(builder, "E", R"(A >= "some text")"));
    ASSERT_EQ(0, ExpressionProcessor::apply(builder, "F", R"(A - "some text")"));
    ASSERT_EQ(0, ExpressionProcessor::apply(builder, "G", R"(3 >= "some text")"));
    ASSERT_EQ(0, builder.finish());
    auto graph = builder.build();
    ASSERT_NE(nullptr, graph.get());
    *(graph->find_data("A")->emit<::std::string>()) = "123";
    *(graph->find_data("B")->emit<::std::string>()) = "456";
    ASSERT_EQ(0, graph->run(graph->find_data("C"),
            graph->find_data("D"), graph->find_data("E")).get());
    ASSERT_NE(nullptr, graph->find_data("C")->cvalue<::std::string>());
    ASSERT_STREQ("123456", graph->find_data("C")->cvalue<::std::string>()->c_str());
    ASSERT_NE(nullptr, graph->find_data("D")->cvalue<bool>());
    ASSERT_TRUE(graph->find_data("D")->as<bool>());
    ASSERT_NE(nullptr, graph->find_data("E")->cvalue<bool>());
    ASSERT_FALSE(graph->find_data("E")->as<bool>());
    graph->reset();
    *(graph->find_data("A")->emit<::std::string>()) = "123";
    *(graph->find_data("B")->emit<::std::string>()) = "456";
    ASSERT_NE(0, graph->run(graph->find_data("F")).get());
    graph->reset();
    *(graph->find_data("A")->emit<::std::string>()) = "123";
    *(graph->find_data("B")->emit<::std::string>()) = "456";
    ASSERT_NE(0, graph->run(graph->find_data("G")).get());
}

TEST(expression, const_expression_supported) {
    GraphBuilder builder;
    builder.executor(executor);
    ASSERT_EQ(0, ExpressionProcessor::apply(builder, "A", "214"));
    ASSERT_EQ(0, ExpressionProcessor::apply(builder, "B", "true"));
    ASSERT_EQ(0, ExpressionProcessor::apply(builder, "C", R"("some \\ \" text")"));
    ASSERT_EQ(0, builder.finish());
    auto graph = builder.build();
    ASSERT_NE(nullptr, graph.get());
    ASSERT_EQ(0, graph->run(graph->find_data("A"),
            graph->find_data("B"), graph->find_data("C")).get());
    ASSERT_EQ(214, graph->find_data("A")->as<int32_t>());
    ASSERT_TRUE(graph->find_data("B")->as<bool>());
    ASSERT_STREQ(R"(some \ " text)", graph->find_data("C")->cvalue<::std::string>()->c_str());
}

TEST(expression, support_single_data_with_different_name_through_alias) {
    GraphBuilder builder;
    builder.executor(executor);
    ASSERT_EQ(0, ExpressionProcessor::apply(builder, "A", "A"));
    ASSERT_EQ(0, ExpressionProcessor::apply(builder, "B", "A"));
    ASSERT_EQ(0, ExpressionProcessor::apply(builder, "((C))", "A"));
    ASSERT_EQ(0, builder.finish());
    auto graph = builder.build();
    ASSERT_NE(nullptr, graph.get());
    graph->find_data("A")->emit<::std::string>()->assign("10086");
    ASSERT_EQ(0, graph->run(graph->find_data("B"), graph->find_data("((C))")).get());
    ASSERT_EQ(
        graph->find_data("A")->cvalue<::std::string>()->data(),
        graph->find_data("B")->cvalue<::std::string>()->data());
    ASSERT_STREQ("10086", graph->find_data("B")->cvalue<::std::string>()->data());
    ASSERT_EQ(
        graph->find_data("A")->cvalue<::std::string>()->data(),
        graph->find_data("((C))")->cvalue<::std::string>()->data());
    ASSERT_STREQ("10086", graph->find_data("((C))")->cvalue<::std::string>()->data());
}

TEST(expression, const_expression_accept_space) {
    GraphBuilder builder;
    builder.executor(executor);
    ASSERT_EQ(0, ExpressionProcessor::apply(builder, "A", "214   "));
    ASSERT_EQ(0, ExpressionProcessor::apply(builder, "B", "   true"));
    ASSERT_EQ(0, ExpressionProcessor::apply(builder, "C", R"(   "some \\ \" text"  )"));
    ASSERT_EQ(0, builder.finish());
    auto graph = builder.build();
    ASSERT_NE(nullptr, graph.get());
    ASSERT_EQ(0, graph->run(graph->find_data("A"),
            graph->find_data("B"), graph->find_data("C")).get());
    ASSERT_EQ(214, graph->find_data("A")->as<int32_t>());
    ASSERT_TRUE(graph->find_data("B")->as<bool>());
    ASSERT_STREQ(R"(some \ " text)", graph->find_data("C")->cvalue<::std::string>()->c_str());
}

TEST(expression, support_conditional_operator) {
    GraphBuilder builder;
    builder.executor(executor);
    ASSERT_EQ(0, ExpressionProcessor::apply(builder, "D", "A ? B : C"));
    ASSERT_EQ(0, builder.finish());
    auto graph = builder.build();
    ASSERT_NE(nullptr, graph.get());
    *(graph->find_data("A")->emit<bool>()) = true;
    *(graph->find_data("B")->emit<int32_t>()) = 1;
    *(graph->find_data("C")->emit<int32_t>()) = 2;
    ASSERT_EQ(0, graph->run(graph->find_data("D")).get());
    ASSERT_EQ(1, graph->find_data("D")->as<int32_t>());
    graph->reset();
    *(graph->find_data("A")->emit<bool>()) = false;
    *(graph->find_data("B")->emit<int32_t>()) = 1;
    *(graph->find_data("C")->emit<int32_t>()) = 2;
    ASSERT_EQ(0, graph->run(graph->find_data("D")).get());
    ASSERT_EQ(2, graph->find_data("D")->as<int32_t>());
}

TEST(expression, support_bracketed_data_name) {
    GraphBuilder builder;
    builder.executor(executor);
    ASSERT_EQ(0, ExpressionProcessor::apply(builder, "D", "(A) ? B : C"));
    ASSERT_EQ(0, ExpressionProcessor::apply(builder, "E", "A ? (B) : C"));
    ASSERT_EQ(0, ExpressionProcessor::apply(builder, "F", "A ? B : (C)"));
    ASSERT_EQ(0, builder.finish());
    auto graph = builder.build();
    ASSERT_NE(nullptr, graph.get());
    *(graph->find_data("A")->emit<bool>()) = true;
    *(graph->find_data("B")->emit<int32_t>()) = 1;
    *(graph->find_data("C")->emit<int32_t>()) = 2;
    ASSERT_EQ(0, graph->run(graph->find_data("D"),
            graph->find_data("E"),
            graph->find_data("F")).get());
    ASSERT_EQ(1, graph->find_data("D")->as<int32_t>());
    ASSERT_EQ(1, graph->find_data("E")->as<int32_t>());
    ASSERT_EQ(1, graph->find_data("F")->as<int32_t>());
}

TEST(expression, only_active_branch_in_condition_expression_when_needed) {
    ::std::string exp = "A ? B : C";
    GraphBuilder builder;
    builder.executor(executor);
    ASSERT_EQ(0, ExpressionProcessor::apply(builder, "D", exp));
    ASSERT_EQ(0, builder.finish());
    auto graph = builder.build();
    ASSERT_NE(nullptr, graph.get());
    *(graph->find_data("A")->emit<bool>()) = true;
    *(graph->find_data("B")->emit<int32_t>()) = 1;
    ASSERT_EQ(0, graph->run(graph->find_data("D")).get());
    ASSERT_EQ(1, graph->find_data("D")->as<int32_t>());
    graph->reset();
    *(graph->find_data("A")->emit<bool>()) = false;
    *(graph->find_data("C")->emit<int32_t>()) = 2;
    ASSERT_EQ(0, graph->run(graph->find_data("D")).get());
    ASSERT_EQ(2, graph->find_data("D")->as<int32_t>());
    graph->reset();
    *(graph->find_data("A")->emit<bool>()) = false;
    *(graph->find_data("B")->emit<int32_t>()) = 1;
    ASSERT_NE(0, graph->run(graph->find_data("D")).get());
}

TEST(expression, allow_const_in_condition_expression) {
    ::std::string exp = "A ? B : 3";
    GraphBuilder builder;
    builder.executor(executor);
    ASSERT_EQ(0, ExpressionProcessor::apply(builder, "D", exp));
    ASSERT_EQ(0, builder.finish());
    auto graph = builder.build();
    ASSERT_NE(nullptr, graph.get());
    *(graph->find_data("A")->emit<bool>()) = true;
    *(graph->find_data("B")->emit<int32_t>()) = 1;
    ASSERT_EQ(0, graph->run(graph->find_data("D")).get());
    ASSERT_EQ(1, graph->find_data("D")->as<int32_t>());
    graph->reset();
    *(graph->find_data("A")->emit<bool>()) = false;
    ASSERT_EQ(0, graph->run(graph->find_data("D")).get());
    ASSERT_EQ(3, graph->find_data("D")->as<int32_t>());
}

TEST(expression, nested_condition_expression_support) {
    GraphBuilder builder;
    builder.executor(executor);
    ASSERT_EQ(0, ExpressionProcessor::apply(builder, "F", "A ? (B ? C : D) : E"));
    ASSERT_EQ(0, builder.finish());
    auto graph = builder.build();
    ASSERT_NE(nullptr, graph.get());
    *(graph->find_data("A")->emit<bool>()) = false;
    *(graph->find_data("E")->emit<int32_t>()) = 1;
    ASSERT_EQ(0, graph->run(graph->find_data("F")).get());
    ASSERT_EQ(1, graph->find_data("F")->as<int32_t>());
    graph->reset();
    *(graph->find_data("A")->emit<bool>()) = true;
    *(graph->find_data("B")->emit<int32_t>()) = 1;
    *(graph->find_data("C")->emit<int32_t>()) = 2;
    ASSERT_EQ(0, graph->run(graph->find_data("F")).get());
    ASSERT_EQ(2, graph->find_data("F")->as<int32_t>());
    graph->reset();
    *(graph->find_data("A")->emit<bool>()) = true;
    *(graph->find_data("B")->emit<int32_t>()) = 0;
    *(graph->find_data("D")->emit<int32_t>()) = 3;
    ASSERT_EQ(0, graph->run(graph->find_data("F")).get());
    ASSERT_EQ(3, graph->find_data("F")->as<int32_t>());
}

TEST(expression, nested_caculation_expression_support) {
    GraphBuilder builder;
    builder.executor(executor);
    ASSERT_EQ(0, ExpressionProcessor::apply(builder, "G", "A > B ? C + D : E * F"));
    ASSERT_EQ(0, builder.finish());
    auto graph = builder.build();
    ASSERT_NE(nullptr, graph.get());
    *(graph->find_data("A")->emit<int32_t>()) = 2;
    *(graph->find_data("B")->emit<int32_t>()) = 1;
    *(graph->find_data("C")->emit<int32_t>()) = 2;
    *(graph->find_data("D")->emit<int32_t>()) = 3;
    ASSERT_EQ(0, graph->run(graph->find_data("G")).get());
    ASSERT_EQ(5, graph->find_data("G")->as<int32_t>());
    graph->reset();
    *(graph->find_data("A")->emit<int32_t>()) = 2;
    *(graph->find_data("B")->emit<int32_t>()) = 3;
    *(graph->find_data("E")->emit<int32_t>()) = 4;
    *(graph->find_data("F")->emit<int32_t>()) = 5;
    ASSERT_EQ(0, graph->run(graph->find_data("G")).get());
    ASSERT_EQ(20, graph->find_data("G")->as<int32_t>());
}

TEST(expression, recursive_nested_expression_support) {
    GraphBuilder builder;
    builder.executor(executor);
    ASSERT_EQ(0, ExpressionProcessor::apply(builder, "I", "A > B ? (C + D ? 214 : F) : G * H"));
    ASSERT_EQ(0, builder.finish());
    auto graph = builder.build();
    ASSERT_NE(nullptr, graph.get());
    *(graph->find_data("A")->emit<int32_t>()) = 2;
    *(graph->find_data("B")->emit<int32_t>()) = 1;
    *(graph->find_data("C")->emit<int32_t>()) = 2;
    *(graph->find_data("D")->emit<int32_t>()) = 3;
    ASSERT_EQ(0, graph->run(graph->find_data("I")).get());
    ASSERT_EQ(214, graph->find_data("I")->as<int32_t>());
}

TEST(expression, nested_expression_is_auto_dedup) {
    GraphBuilder builder;
    builder.executor(executor);
    ASSERT_EQ(0, ExpressionProcessor::apply(builder, "E", "A ? (B ? C : D) + 3 : (B ? C : D) + (B ? C : D)"));
    ASSERT_EQ(0, builder.finish());
    auto graph = builder.build();
    ASSERT_NE(nullptr, graph.get());
    *(graph->find_data("A")->emit<int32_t>()) = 1;
    *(graph->find_data("B")->emit<int32_t>()) = 1;
    *(graph->find_data("C")->emit<int32_t>()) = 2;
    *(graph->find_data("D")->emit<int32_t>()) = 3;
    ASSERT_EQ(0, graph->run(graph->find_data("E")).get());
    ASSERT_EQ(5, graph->find_data("E")->as<int32_t>());
    graph->reset();
    *(graph->find_data("A")->emit<int32_t>()) = 0;
    *(graph->find_data("B")->emit<int32_t>()) = 1;
    *(graph->find_data("C")->emit<int32_t>()) = 2;
    *(graph->find_data("D")->emit<int32_t>()) = 3;
    ASSERT_EQ(0, graph->run(graph->find_data("E")).get());
    ASSERT_EQ(4, graph->find_data("E")->as<int32_t>());
}

TEST(expression, reject_empty_data) {
    ::std::string exp = "A + B";
    GraphBuilder builder;
    builder.executor(executor);
    ASSERT_EQ(0, ExpressionProcessor::apply(builder, "D", exp));
    ASSERT_EQ(0, builder.finish());
    auto graph = builder.build();
    ASSERT_NE(nullptr, graph.get());
    *(graph->find_data("A")->emit<int32_t>()) = 1;
    graph->find_data("B")->emit<int64_t>().clear();
    ASSERT_NE(0, graph->run(graph->find_data("D")).get());
}

TEST(expression, reject_empty_expression) {
    GraphBuilder builder;
    builder.executor(executor);
    ASSERT_NE(0, ExpressionProcessor::apply(builder, "A", ""));
    ASSERT_NE(0, ExpressionProcessor::apply(builder, "A", "   "));
}

TEST(expression, reject_error_expression) {
    ::std::string exp = "A + ";
    GraphBuilder builder;
    builder.executor(executor);
    ASSERT_NE(0, ExpressionProcessor::apply(builder, "D", exp));
}

TEST(expression, can_apply_to_whole_builder) {
    MProcessor mprocessor;
    OneProcessor processor;
    GraphBuilder builder;
    builder.executor(executor);
    {
        auto& vertex = builder.add_vertex(processor);
        vertex.anonymous_depend().to("A");
        vertex.anonymous_emit().to("B");
    }
    {
        auto& vertex = builder.add_vertex(processor);
        vertex.anonymous_depend().to("A");
        // 主动输出 B + C
        vertex.anonymous_emit().to("B + C");
    }
    {
        auto& vertex = builder.add_vertex(mprocessor);
        // 会依赖主动的 B + C
        vertex.anonymous_depend().to("B + C").on("A");
        vertex.anonymous_emit().to("D");
    }
    {
        auto& vertex = builder.add_vertex(processor);
        // 没有主动输出的会自动补充表达式
        vertex.anonymous_depend().to("A ? B - C : 214");
        vertex.anonymous_emit().to("E");
    }
    ASSERT_EQ(0, ExpressionProcessor::apply(builder));
    ASSERT_EQ(0, builder.finish());
    auto graph = builder.build();
    ASSERT_NE(nullptr, graph.get());
    *(graph->find_data("A")->emit<int32_t>()) = 1;
    *(graph->find_data("B")->emit<int32_t>()) = 2;
    *(graph->find_data("C")->emit<int32_t>()) = 3;
    ASSERT_EQ(0, graph->run(graph->find_data("D"),
            graph->find_data("E")).get());
    ASSERT_EQ(1, *graph->find_data("D")->cvalue<int32_t>());
    ASSERT_EQ(-1, *graph->find_data("E")->cvalue<int32_t>());
}

TEST(expression, reject_error_expression_when_apply_to_whole_builder) {
    OneProcessor processor;
    GraphBuilder builder;
    builder.executor(executor);
    {
        auto& vertex = builder.add_vertex(processor);
        vertex.anonymous_depend().to("B * (C");
        vertex.anonymous_emit().to("X");
    }
    ASSERT_NE(0, ExpressionProcessor::apply(builder));
}
