#include <gtest/gtest.h>
#include <joewu/graph/engine/builder.h>
#include <joewu/graph/builtin/select.h>

using ::joewu::feed::graph::GraphVertex;
using ::joewu::feed::graph::GraphBuilder;
using ::joewu::feed::graph::GraphProcessor;
using ::joewu::feed::graph::GraphVertexBuilder;
using ::joewu::feed::graph::BthreadGraphExecutor;
using ::joewu::feed::graph::builtin::SelectProcessor;

class MockProcessor : public GraphProcessor {
public:
    virtual int32_t setup(GraphVertex& vertex) const noexcept override {
        vertex.anonymous_dependency(0)->declare_mutable(need_mutable);
        return 0;
    }
    virtual int32_t process(GraphVertex& vertex) noexcept override {
        *vertex.anonymous_emit(0)->emit<bool>() = 
            vertex.anonymous_dependency(0)->mutable_value<::std::string>() != nullptr;
        return 0;
    }
    bool need_mutable {false};
};

class Test : public ::testing::Test {
public:
    virtual void SetUp() {
        builder.executor(executor);
    }
    virtual void TearDown() {
    }
    
    BthreadGraphExecutor executor;
    GraphBuilder builder;
    SelectProcessor processor;
    MockProcessor mock_processor;
};

TEST_F(Test, forward_first_ready_dependency) {
    auto& vertex = builder.add_vertex(processor);
    vertex.anonymous_depend().to("A").on("CA");
    vertex.anonymous_depend().to("B");
    vertex.anonymous_depend().to("C");
    vertex.anonymous_emit().to("D");
    ASSERT_EQ(0, builder.finish());
    auto graph = builder.build();
    ASSERT_NE(nullptr, graph.get());
    *graph->find_data("CA")->emit<bool>() = false;
    *graph->find_data("B")->emit<::std::string>() = "b";
    *graph->find_data("C")->emit<::std::string>() = "c";
    ASSERT_EQ(0, graph->run(graph->find_data("D")).get());
    ASSERT_STREQ("b", graph->find_data("D")->cvalue<::std::string>()->c_str());
}

TEST_F(Test, forward_mutable_as_mutable) {
    mock_processor.need_mutable = true;
    {
        auto& vertex = builder.add_vertex(processor);
        vertex.anonymous_depend().to("A");
        vertex.anonymous_emit().to("B");
    }
    {
        auto& vertex = builder.add_vertex(mock_processor);
        vertex.anonymous_depend().to("B");
        vertex.anonymous_emit().to("C");
    }
    ASSERT_EQ(0, builder.finish());
    auto graph = builder.build();
    ASSERT_NE(nullptr, graph.get());
    *graph->find_data("A")->emit<::std::string>() = "a";
    ASSERT_EQ(0, graph->run(graph->find_data("C")).get());
    ASSERT_TRUE(*graph->find_data("C")->cvalue<bool>());
}

TEST_F(Test, forward_immutable_as_immutable) {
    mock_processor.need_mutable = false;
    {
        auto& vertex = builder.add_vertex(processor);
        vertex.anonymous_depend().to("A");
        vertex.anonymous_emit().to("B");
    }
    {
        auto& vertex = builder.add_vertex(mock_processor);
        vertex.anonymous_depend().to("B");
        vertex.anonymous_emit().to("C");
    }
    ASSERT_EQ(0, builder.finish());
    auto graph = builder.build();
    ASSERT_NE(nullptr, graph.get());
    *graph->find_data("A")->emit<::std::string>() = "a";
    ASSERT_EQ(0, graph->run(graph->find_data("C")).get());
    ASSERT_FALSE(*graph->find_data("C")->cvalue<bool>());
}

TEST_F(Test, reject_no_emit) {
    auto& vertex = builder.add_vertex(processor);
    vertex.anonymous_depend().to("A").on("CA");
    vertex.anonymous_depend().to("B");
    vertex.anonymous_depend().to("C");
    ASSERT_EQ(0, builder.finish());
    auto graph = builder.build();
    ASSERT_EQ(nullptr, graph.get());
}

TEST_F(Test, reject_emit_more_than_one) {
    auto& vertex = builder.add_vertex(processor);
    vertex.anonymous_depend().to("A").on("CA");
    vertex.anonymous_depend().to("B");
    vertex.anonymous_depend().to("C");
    vertex.anonymous_emit().to("D");
    vertex.anonymous_emit().to("E");
    ASSERT_EQ(0, builder.finish());
    auto graph = builder.build();
    ASSERT_EQ(nullptr, graph.get());
}

TEST_F(Test, fail_when_no_ready_dependency) {
    auto& vertex = builder.add_vertex(processor);
    vertex.anonymous_depend().to("A").on("CA");
    vertex.anonymous_depend().to("B").on("CB");
    vertex.anonymous_emit().to("D");
    ASSERT_EQ(0, builder.finish());
    auto graph = builder.build();
    ASSERT_NE(nullptr, graph.get());
    *graph->find_data("CA")->emit<bool>() = false;
    *graph->find_data("CB")->emit<bool>() = false;
    ASSERT_NE(0, graph->run(graph->find_data("D")).get());
}

TEST_F(Test, support_simply_apply) {
    SelectProcessor::apply(builder, "DEST", "COND", "A", "B");
    ASSERT_EQ(0, builder.finish());
    auto graph = builder.build();
    ASSERT_NE(nullptr, graph.get());
    *graph->find_data("A")->emit<::std::string>() = "a";
    *graph->find_data("B")->emit<::std::string>() = "b";
    *graph->find_data("COND")->emit<bool>() = true;
    ASSERT_EQ(0, graph->run(graph->find_data("DEST")).get());
    ASSERT_STREQ("a", graph->find_data("DEST")->cvalue<::std::string>()->c_str());
    graph->reset();
    *graph->find_data("A")->emit<::std::string>() = "a";
    *graph->find_data("B")->emit<::std::string>() = "b";
    *graph->find_data("COND")->emit<bool>() = false;
    ASSERT_EQ(0, graph->run(graph->find_data("DEST")).get());
    ASSERT_STREQ("b", graph->find_data("DEST")->cvalue<::std::string>()->c_str());
}
