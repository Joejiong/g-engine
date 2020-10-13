#include <thread>
#include <future>
#include <inttypes.h>
#include <gtest/gtest.h>
#include <base/logging.h>
#include <bthread/mutex.h>
#include <joewu/graph/engine/dependency.h>
#include <joewu/graph/engine/builder.h>

using ::joewu::feed::mlarch::babylon::Stack;
using ::joewu::feed::graph::Closure;
using ::joewu::feed::graph::BthreadGraphExecutor;
using ::joewu::feed::graph::GraphBuilder;
using ::joewu::feed::graph::GraphDependency;
using ::joewu::feed::graph::GraphDependencyBuilder;
using ::joewu::feed::graph::GraphVertex;
using ::joewu::feed::graph::GraphVertexBuilder;
using ::joewu::feed::graph::GraphData;
using ::joewu::feed::graph::GraphProcessor;
using ::joewu::feed::graph::ClosureContext;
using ::joewu::feed::graph::ClosureContextImplement;

class MockProcessor : public GraphProcessor {
    virtual int32_t setup(GraphVertex&) const noexcept override {
        return setup_return;
    }
    virtual int32_t process(GraphVertex& vertex) noexcept override {
        run_times++;
        if (process_return == 0) {
            vertex.named_emit("x")->emit<::std::string>()->assign("x_value");
        }
        return process_return;
    }
    int32_t setup_return {0};
    int32_t process_return {0};
    int32_t run_times {0};
};

class DependencyTest : public ::testing::Test {
public:
    virtual void SetUp() {
        data[0].bind(*closure);
        vertex.builder(vertex_builder);
    }
    virtual void TearDown() {
        if (closure != nullptr) {
            closure->fire();
            delete closure;
        }
    }
    
    BthreadGraphExecutor executor;
    MockProcessor processor;
    GraphBuilder graph_builder;
    GraphVertexBuilder& vertex_builder = graph_builder.add_vertex(processor);
    GraphDependencyBuilder builder {vertex_builder.named_depend("x")};
    GraphVertex vertex;
    GraphDependency dependency;
    ClosureContext* closure {new ClosureContextImplement<::bthread::Mutex>(executor)};
    ::std::vector<GraphData> data {1024};
    ::std::unordered_map<::std::string, size_t> data_index_by_name;
    GraphVertex* _runnable_vertexes[1024];
    Stack<GraphVertex*> runnable_vertexes {_runnable_vertexes, 1024};
    GraphData* _activating_data[1024];
    Stack<GraphData*> activating_data {_activating_data, 1024};
};

TEST_F(DependencyTest, immediately_ready_when_target_ready_and_no_condition) {
    builder.to("target");
    ASSERT_EQ(0, builder.finish(data_index_by_name));
    builder.build(dependency, vertex, data);
    data[0].emit<::std::string>()->assign("pre_value");
    ASSERT_EQ(1, dependency.activate(activating_data));
    ASSERT_TRUE(dependency.established());
    ASSERT_TRUE(dependency.ready());
    ASSERT_EQ(0, activating_data.size());
}

TEST_F(DependencyTest, immediately_ready_when_target_ready_and_condition_established) {
    builder.to("target").on("condition");
    ASSERT_EQ(0, builder.finish(data_index_by_name));
    builder.build(dependency, vertex, data);
    data[0].emit<::std::string>()->assign("pre_value");
    *(data[1].emit<bool>()) = true;
    ASSERT_EQ(1, dependency.activate(activating_data));
    ASSERT_TRUE(dependency.established());
    ASSERT_TRUE(dependency.ready());
    ASSERT_EQ(0, activating_data.size());
}

TEST_F(DependencyTest, immediately_ready_when_condition_not_established) {
    builder.to("target").on("condition");
    ASSERT_EQ(0, builder.finish(data_index_by_name));
    builder.build(dependency, vertex, data);
    *(data[1].emit<bool>()) = false;
    ASSERT_EQ(1, dependency.activate(activating_data));
    ASSERT_FALSE(dependency.established());
    ASSERT_FALSE(dependency.ready());
    ASSERT_EQ(0, activating_data.size());
}

TEST_F(DependencyTest, activate_target_when_no_condition) {
    builder.to("target");
    ASSERT_EQ(0, builder.finish(data_index_by_name));
    builder.build(dependency, vertex, data);
    ASSERT_EQ(0, dependency.activate(activating_data));
    ASSERT_TRUE(dependency.established());
    ASSERT_FALSE(dependency.ready());
    ASSERT_EQ(1, activating_data.size());
    ASSERT_EQ(&data[0], activating_data[0]);
}

TEST_F(DependencyTest, activate_target_when_condition_established) {
    builder.to("target").on("condition");
    ASSERT_EQ(0, builder.finish(data_index_by_name));
    builder.build(dependency, vertex, data);
    *(data[1].emit<bool>()) = true;
    ASSERT_EQ(0, dependency.activate(activating_data));
    ASSERT_TRUE(dependency.established());
    ASSERT_FALSE(dependency.ready());
    ASSERT_EQ(1, activating_data.size());
    ASSERT_EQ(&data[0], activating_data[0]);
}

TEST_F(DependencyTest, activate_target_when_condition_established_false) {
    builder.to("target").unless("condition");
    ASSERT_EQ(0, builder.finish(data_index_by_name));
    builder.build(dependency, vertex, data);
    *(data[1].emit<bool>()) = false;
    ASSERT_EQ(0, dependency.activate(activating_data));
    ASSERT_TRUE(dependency.established());
    ASSERT_FALSE(dependency.ready());
    ASSERT_EQ(1, activating_data.size());
    ASSERT_EQ(&data[0], activating_data[0]);
}

TEST_F(DependencyTest, activate_condition_if_not_ready_yet) {
    builder.to("target").on("condition");
    ASSERT_EQ(0, builder.finish(data_index_by_name));
    builder.build(dependency, vertex, data);
    ASSERT_EQ(0, dependency.activate(activating_data));
    ASSERT_FALSE(dependency.established());
    ASSERT_FALSE(dependency.ready());
    ASSERT_EQ(1, activating_data.size());
    ASSERT_EQ(&data[1], activating_data[0]);
}

TEST_F(DependencyTest, activate_target_when_condition_establish) {
    builder.to("target").on("condition");
    ASSERT_EQ(0, builder.finish(data_index_by_name));
    GraphVertex target_vertex;
    data[0].producer(target_vertex);
    data[0].data_num(2);
    builder.build(dependency, vertex, data);
    ASSERT_EQ(0, dependency.activate(activating_data));
    ASSERT_EQ(1, activating_data.size());
    ASSERT_EQ(&data[1], activating_data[0]);
    *(data[1].certain_type_non_reference_mutable_value<bool>()) = true;
    data[1].empty(false);
    dependency.ready(&data[1], runnable_vertexes);
    ASSERT_TRUE(dependency.established());
    ASSERT_FALSE(dependency.ready());
    ASSERT_EQ(1, runnable_vertexes.size());
    ASSERT_EQ(&target_vertex, runnable_vertexes[0]);
}

TEST_F(DependencyTest, empty_when_target_empty) {
    builder.to("target");
    ASSERT_EQ(0, builder.finish(data_index_by_name));
    GraphVertex target_vertex;
    builder.build(dependency, vertex, data);
    ASSERT_EQ(0, dependency.activate(activating_data));
    ASSERT_EQ(1, activating_data.size());
    ASSERT_EQ(&data[data_index_by_name["target"]], activating_data[0]);
    data[data_index_by_name["target"]].emit<bool>().clear();
    dependency.ready(&data[data_index_by_name["target"]], runnable_vertexes);
    ASSERT_TRUE(dependency.established());
    ASSERT_TRUE(dependency.ready());
    ASSERT_TRUE(dependency.empty());
    ASSERT_EQ(0, runnable_vertexes.size());
}

TEST_F(DependencyTest, dependency_reuseable_after_reset) {
    builder.to("target").on("condition");
    ASSERT_EQ(0, builder.finish(data_index_by_name));
    GraphVertex target_vertex;
    data[0].producer(target_vertex);
    data[0].data_num(2);
    builder.build(dependency, vertex, data);

    {
        ASSERT_EQ(0, dependency.activate(activating_data));
        ASSERT_FALSE(dependency.established());
        ASSERT_FALSE(dependency.ready());
        ASSERT_EQ(1, activating_data.size());
        ASSERT_EQ(&data[1], activating_data[0]);

        *(data[1].certain_type_non_reference_mutable_value<bool>()) = true;
        data[1].empty(false);
        dependency.ready(&data[1], runnable_vertexes);
        ASSERT_TRUE(dependency.established());
        ASSERT_FALSE(dependency.ready());
        ASSERT_EQ(1, runnable_vertexes.size());
        ASSERT_EQ(&target_vertex, runnable_vertexes[0]);
    }

    activating_data.clear();
    runnable_vertexes.clear();
    dependency.reset();
    data[0].reset();
    data[1].reset();
    vertex.reset();
    target_vertex.reset();

    {
        ASSERT_EQ(0, dependency.activate(activating_data));
        ASSERT_FALSE(dependency.established());
        ASSERT_FALSE(dependency.ready());
        ASSERT_EQ(1, activating_data.size());
        ASSERT_EQ(&data[1], activating_data[0]);

        *(data[1].certain_type_non_reference_mutable_value<bool>()) = true;
        data[1].empty(false);
        dependency.ready(&data[1], runnable_vertexes);
        ASSERT_TRUE(dependency.established());
        ASSERT_FALSE(dependency.ready());
        ASSERT_EQ(1, runnable_vertexes.size());
        ASSERT_EQ(&target_vertex, runnable_vertexes[0]);
    }
}

TEST_F(DependencyTest, only_mutable_dependency_can_get_mutable_value) {
    GraphDependencyBuilder& mutable_builder = vertex_builder.named_depend("mutable_builder");
    GraphDependency mutable_dependency;
    GraphVertex mutable_vertex;
    mutable_vertex.builder(vertex_builder);
    mutable_builder.to("target1").set_mutable();
    builder.to("target2");
    ASSERT_EQ(0, builder.finish(data_index_by_name));
    ASSERT_EQ(0, mutable_builder.finish(data_index_by_name));
    builder.build(dependency, vertex, data);
    mutable_builder.build(mutable_dependency, mutable_vertex, data);
    ASSERT_EQ(0, dependency.activate(activating_data));
    ASSERT_EQ(1, activating_data.size());
    ASSERT_EQ(&data[data_index_by_name["target2"]], activating_data[0]);
    ASSERT_EQ(0, mutable_dependency.activate(activating_data));
    ASSERT_EQ(2, activating_data.size());
    ASSERT_EQ(&data[data_index_by_name["target1"]], activating_data[1]);
    *data[data_index_by_name["target1"]].emit<bool>() = true;
    *data[data_index_by_name["target2"]].emit<bool>() = true;
    ASSERT_NE(nullptr, mutable_dependency.mutable_value<bool>());
    ASSERT_EQ(nullptr, dependency.mutable_value<bool>());
}

TEST_F(DependencyTest, mutable_can_declare_in_setup) {
    builder.to("target1");
    ASSERT_EQ(0, builder.finish(data_index_by_name));
    builder.build(dependency, vertex, data);
    dependency.declare_mutable();
    ASSERT_EQ(0, dependency.activate(activating_data));
    ASSERT_EQ(1, activating_data.size());
    ASSERT_EQ(&data[data_index_by_name["target1"]], activating_data[0]);
    *data[data_index_by_name["target1"]].emit<bool>() = true;
    ASSERT_NE(nullptr, dependency.mutable_value<bool>());
}

TEST_F(DependencyTest, activate_fail_when_try_mutate_same_target_immutable) {
    GraphDependencyBuilder& mutable_builder = vertex_builder.named_depend("mutable_builder");
    GraphDependency mutable_dependency;
    GraphVertex mutable_vertex;
    mutable_vertex.builder(vertex_builder);
    mutable_builder.to("target").set_mutable();
    builder.to("target");
    ASSERT_EQ(0, builder.finish(data_index_by_name));
    ASSERT_EQ(0, mutable_builder.finish(data_index_by_name));
    builder.build(dependency, vertex, data);
    mutable_builder.build(mutable_dependency, mutable_vertex, data);
    ASSERT_EQ(0, dependency.activate(activating_data));
    ASSERT_EQ(1, activating_data.size());
    ASSERT_EQ(&data[0], activating_data[0]);
    ASSERT_GT(0, mutable_dependency.activate(activating_data));
}

TEST_F(DependencyTest, activate_fail_when_try_mutate_same_target_mutable) {
    GraphDependencyBuilder& mutable_builder = vertex_builder.named_depend("mutable_builder");
    GraphDependency mutable_dependency;
    GraphVertex mutable_vertex;
    mutable_vertex.builder(vertex_builder);
    mutable_builder.to("target").set_mutable();
    builder.to("target").set_mutable();
    ASSERT_EQ(0, builder.finish(data_index_by_name));
    ASSERT_EQ(0, mutable_builder.finish(data_index_by_name));
    builder.build(dependency, vertex, data);
    mutable_builder.build(mutable_dependency, mutable_vertex, data);
    ASSERT_EQ(0, dependency.activate(activating_data));
    ASSERT_EQ(1, activating_data.size());
    ASSERT_EQ(&data[0], activating_data[0]);
    ASSERT_GT(0, mutable_dependency.activate(activating_data));
}
