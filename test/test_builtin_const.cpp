#include <joewu/graph/builtin/const.h>
#include <joewu/graph/engine/builder.h>
#include <gtest/gtest.h>

using ::joewu::feed::graph::GraphBuilder;
using ::joewu::feed::graph::BthreadGraphExecutor;
using ::joewu::feed::graph::builtin::ConstProcessor;
using ::joewu::feed::mlarch::babylon::Any;

struct Test : public ::testing::Test {
    virtual void SetUp() {
        builder.executor(executor);
    }
    virtual void TearDown() {
    }
    BthreadGraphExecutor executor;
    GraphBuilder builder;
};

TEST_F(Test, work_with_primitive) {
    ConstProcessor::apply(builder, "A", 1234);
    ConstProcessor::apply(builder, "B", ::std::string("1234"));
    ASSERT_EQ(0, builder.finish());
    auto graph = builder.build();
    ASSERT_NE(nullptr, graph.get());
    ASSERT_EQ(0, graph->run(graph->find_data("A"), graph->find_data("B")).get());
    ASSERT_EQ(1234, graph->find_data("A")->as<size_t>());
    ASSERT_EQ("1234", *graph->find_data("B")->cvalue<::std::string>());
}

TEST_F(Test, work_with_struct) {
    // 可移动
    struct A {
        A(size_t val) : value(val) {}
        size_t value;
    };
    // 不可移动
    struct B {
        B(size_t val) : value(val) {}
        B(B&&) = delete;
        size_t value;
    };
    A a(123);
    ::std::unique_ptr<B> b(new B(456));
    ConstProcessor::apply(builder, "A", ::std::move(a));
    ConstProcessor::apply(builder, "B", ::std::move(b));
    ASSERT_EQ(0, builder.finish());
    auto graph = builder.build();
    ASSERT_NE(nullptr, graph.get());
    ASSERT_EQ(0, graph->run(graph->find_data("A"), graph->find_data("B")).get());
    ASSERT_EQ(123, graph->find_data("A")->cvalue<A>()->value);
    ASSERT_EQ(456, graph->find_data("B")->cvalue<B>()->value);
}

TEST_F(Test, work_with_any) {
    Any any(::std::string("1234"));
    Any empty;
    ConstProcessor::apply(builder, "A", ::std::move(any));
    ConstProcessor::apply(builder, "B", ::std::move(empty));
    ASSERT_EQ(0, builder.finish());
    auto graph = builder.build();
    ASSERT_NE(nullptr, graph.get());
    ASSERT_FALSE(graph->find_data("B")->ready());
    ASSERT_EQ(0, graph->run(graph->find_data("A"), graph->find_data("B")).get());
    ASSERT_EQ("1234", *graph->find_data("A")->cvalue<::std::string>());
    ASSERT_TRUE(graph->find_data("B")->ready());
    ASSERT_TRUE(graph->find_data("B")->empty());
}

TEST_F(Test, no_option_means_empty) {
    ConstProcessor processor;
    builder.add_vertex(processor).anonymous_emit().to("A");
    ASSERT_EQ(0, builder.finish());
    auto graph = builder.build();
    ASSERT_NE(nullptr, graph.get());
    ASSERT_FALSE(graph->find_data("A")->ready());
    ASSERT_EQ(0, graph->run(graph->find_data("A")).get());
    ASSERT_TRUE(graph->find_data("A")->ready());
    ASSERT_TRUE(graph->find_data("A")->empty());
}
