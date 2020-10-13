#include <gtest/gtest.h>
#include <joewu/graph/engine/builder.h>
#include <joewu/graph/builtin/logical.h>

using ::joewu::feed::graph::GraphBuilder;
using ::joewu::feed::graph::GraphVertexBuilder;
using ::joewu::feed::graph::BthreadGraphExecutor;
using ::joewu::feed::graph::builtin::AndProcessor;
using ::joewu::feed::graph::builtin::NotProcessor;
using ::joewu::feed::graph::builtin::OrProcessor;

class Test : public ::testing::Test {
public:
    virtual void SetUp() {
        builder.executor(executor);
    }
    virtual void TearDown() {
    }
    
    BthreadGraphExecutor executor;
    GraphBuilder builder;
    AndProcessor and_processor;
    OrProcessor or_processor;
    NotProcessor not_processor;
};

TEST_F(Test, and_work_on_bool) {
    auto& vertex = builder.add_vertex(and_processor);
    vertex.anonymous_depend().to("A");
    vertex.anonymous_depend().to("B");
    vertex.anonymous_emit().to("C");
    ASSERT_EQ(0, builder.finish());
    auto graph = builder.build();
    ASSERT_NE(nullptr, graph.get());
    *graph->find_data("A")->emit<bool>() = true;
    *graph->find_data("B")->emit<bool>() = true;
    ASSERT_EQ(0, graph->run(graph->find_data("C")).get());
    ASSERT_EQ(true, *graph->find_data("C")->cvalue<bool>());
    graph->reset();
    *graph->find_data("A")->emit<bool>() = true;
    *graph->find_data("B")->emit<bool>() = false;
    ASSERT_EQ(0, graph->run(graph->find_data("C")).get());
    ASSERT_FALSE(*graph->find_data("C")->cvalue<bool>());
}

TEST_F(Test, and_reject_no_depend) {
    auto& vertex = builder.add_vertex(and_processor);
    vertex.anonymous_emit().to("C");
    ASSERT_EQ(0, builder.finish());
    auto graph = builder.build();
    ASSERT_EQ(nullptr, graph.get());
}

TEST_F(Test, and_reject_one_depend) {
    auto& vertex = builder.add_vertex(and_processor);
    vertex.anonymous_depend().to("A");
    vertex.anonymous_emit().to("C");
    ASSERT_EQ(0, builder.finish());
    auto graph = builder.build();
    ASSERT_EQ(nullptr, graph.get());
}

TEST_F(Test, and_reject_no_emit) {
    auto& vertex = builder.add_vertex(and_processor);
    vertex.anonymous_depend().to("A");
    vertex.anonymous_depend().to("B");
    ASSERT_EQ(0, builder.finish());
    auto graph = builder.build();
    ASSERT_EQ(nullptr, graph.get());
}

TEST_F(Test, and_reject_emit_more_than_one) {
    auto& vertex = builder.add_vertex(and_processor);
    vertex.anonymous_depend().to("A");
    vertex.anonymous_depend().to("B");
    vertex.anonymous_emit().to("C");
    vertex.anonymous_emit().to("D");
    ASSERT_EQ(0, builder.finish());
    auto graph = builder.build();
    ASSERT_EQ(nullptr, graph.get());
}

TEST_F(Test, and_reject_missing_depdend) {
    auto& vertex = builder.add_vertex(and_processor);
    vertex.anonymous_depend().to("A");
    vertex.anonymous_depend().to("B").on("D");
    vertex.anonymous_emit().to("C");
    ASSERT_EQ(0, builder.finish());
    auto graph = builder.build();
    ASSERT_NE(nullptr, graph.get());
    *graph->find_data("A")->emit<bool>() = true;
    *graph->find_data("D")->emit<bool>() = false;
    ASSERT_NE(0, graph->run(graph->find_data("C")).get());
}

TEST_F(Test, and_can_apply_simply) {
    AndProcessor::apply(builder, "C", "A", "B");
    ASSERT_EQ(0, builder.finish());
    auto graph = builder.build();
    ASSERT_NE(nullptr, graph.get());
    *graph->find_data("A")->emit<bool>() = true;
    *graph->find_data("B")->emit<bool>() = false;
    ASSERT_EQ(0, graph->run(graph->find_data("C")).get());
    ASSERT_FALSE(*graph->find_data("C")->cvalue<bool>());
}

TEST_F(Test, or_work_on_bool) {
    auto& vertex = builder.add_vertex(or_processor);
    vertex.anonymous_depend().to("A");
    vertex.anonymous_depend().to("B");
    vertex.anonymous_emit().to("C");
    ASSERT_EQ(0, builder.finish());
    auto graph = builder.build();
    ASSERT_NE(nullptr, graph.get());
    *graph->find_data("A")->emit<bool>() = true;
    *graph->find_data("B")->emit<bool>() = false;
    ASSERT_EQ(0, graph->run(graph->find_data("C")).get());
    ASSERT_EQ(true, *graph->find_data("C")->cvalue<bool>());
    graph->reset();
    *graph->find_data("A")->emit<bool>() = false;
    *graph->find_data("B")->emit<bool>() = true;
    ASSERT_EQ(0, graph->run(graph->find_data("C")).get());
    ASSERT_EQ(true, *graph->find_data("C")->cvalue<bool>());
    graph->reset();
    *graph->find_data("A")->emit<bool>() = true;
    *graph->find_data("B")->emit<bool>() = true;
    ASSERT_EQ(0, graph->run(graph->find_data("C")).get());
    ASSERT_EQ(true, *graph->find_data("C")->cvalue<bool>());
    graph->reset();
    *graph->find_data("A")->emit<bool>() = false;
    *graph->find_data("B")->emit<bool>() = false;
    ASSERT_EQ(0, graph->run(graph->find_data("C")).get());
    ASSERT_FALSE(*graph->find_data("C")->cvalue<bool>());
}

TEST_F(Test, or_can_apply_simply) {
    OrProcessor::apply(builder, "C", "A", "B");
    ASSERT_EQ(0, builder.finish());
    auto graph = builder.build();
    ASSERT_NE(nullptr, graph.get());
    *graph->find_data("A")->emit<bool>() = true;
    *graph->find_data("B")->emit<bool>() = false;
    ASSERT_EQ(0, graph->run(graph->find_data("C")).get());
    ASSERT_EQ(true, *graph->find_data("C")->cvalue<bool>());
}

TEST_F(Test, not_work_on_bool) {
    auto& vertex = builder.add_vertex(not_processor);
    vertex.anonymous_depend().to("A");
    vertex.anonymous_emit().to("B");
    ASSERT_EQ(0, builder.finish());
    auto graph = builder.build();
    ASSERT_NE(nullptr, graph.get());
    *graph->find_data("A")->emit<bool>() = true;
    ASSERT_EQ(0, graph->run(graph->find_data("B")).get());
    ASSERT_FALSE(*graph->find_data("B")->cvalue<bool>());
    graph->reset();
    *graph->find_data("A")->emit<bool>() = false;
    ASSERT_EQ(0, graph->run(graph->find_data("B")).get());
    ASSERT_EQ(true, *graph->find_data("B")->cvalue<bool>());
}

TEST_F(Test, not_reject_no_depend) {
    auto& vertex = builder.add_vertex(not_processor);
    vertex.anonymous_emit().to("C");
    ASSERT_EQ(0, builder.finish());
    auto graph = builder.build();
    ASSERT_EQ(nullptr, graph.get());
}

TEST_F(Test, not_reject_depend_more_than_one) {
    auto& vertex = builder.add_vertex(not_processor);
    vertex.anonymous_depend().to("A");
    vertex.anonymous_depend().to("B");
    vertex.anonymous_emit().to("C");
    ASSERT_EQ(0, builder.finish());
    auto graph = builder.build();
    ASSERT_EQ(nullptr, graph.get());
}

TEST_F(Test, not_reject_no_emit) {
    auto& vertex = builder.add_vertex(not_processor);
    vertex.anonymous_depend().to("A");
    ASSERT_EQ(0, builder.finish());
    auto graph = builder.build();
    ASSERT_EQ(nullptr, graph.get());
}

TEST_F(Test, not_reject_emit_more_than_one) {
    auto& vertex = builder.add_vertex(and_processor);
    vertex.anonymous_depend().to("A");
    vertex.anonymous_emit().to("C");
    vertex.anonymous_emit().to("D");
    ASSERT_EQ(0, builder.finish());
    auto graph = builder.build();
    ASSERT_EQ(nullptr, graph.get());
}

TEST_F(Test, not_reject_missing_depend) {
    auto& vertex = builder.add_vertex(not_processor);
    vertex.anonymous_depend().to("A").on("C");
    vertex.anonymous_emit().to("B");
    ASSERT_EQ(0, builder.finish());
    auto graph = builder.build();
    ASSERT_NE(nullptr, graph.get());
    *graph->find_data("C")->emit<bool>() = false;
    ASSERT_NE(0, graph->run(graph->find_data("B")).get());
}
