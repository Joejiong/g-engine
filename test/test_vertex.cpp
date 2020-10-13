#include <joewu/graph/engine/vertex.h>

#include <joewu/graph/engine/builder.h>
#include <joewu/graph/engine/closure.h>
#include <joewu/graph/engine/executor.h>

#include <base/logging.h>
#include <bthread/mutex.h>

#include <gtest/gtest.h>

using ::joewu::feed::mlarch::babylon::Stack;
using ::joewu::feed::graph::Closure;
using ::joewu::feed::graph::ClosureContext;
using ::joewu::feed::graph::ClosureContextImplement;
using ::joewu::feed::graph::GraphBuilder;
using ::joewu::feed::graph::GraphData;
using ::joewu::feed::graph::GraphVertex;
using ::joewu::feed::graph::GraphFunction;
using ::joewu::feed::graph::GraphProcessor;
using ::joewu::feed::graph::GraphDependency;
using ::joewu::feed::graph::GraphVertexBuilder;
using ::joewu::feed::graph::BthreadGraphExecutor;
using ::joewu::feed::mlarch::babylon::SerialAllocator;

class MockProcessor : public GraphProcessor {
    virtual int32_t setup(GraphVertex& vertex) const noexcept override {
        if (trivial) {
            vertex.trivial();
        }
        switch (declare_x_type) {
        case 1: {
                    vertex.named_dependency("x")->declare_type<int32_t>();
                    break;
                }
        case 2: {
                    vertex.named_dependency("x")->declare_type<::std::string>();
                    break;
                }
        default: {
                     break;
                }
        }
        switch (declare_y_type) {
        case 1: {
                    vertex.named_dependency("y")->declare_type<int32_t>();
                    break;
                }
        case 2: {
                    vertex.named_dependency("y")->declare_type<::std::string>();
                    break;
                }
        default: {
                     break;
                }
        }
        switch (declare_emit_type) {
        case 1: {
                    vertex.named_emit("x")->declare_type<int32_t>();
                    break;
                }
        case 2: {
                    vertex.named_emit("x")->declare_type<::std::string>();
                    break;
                }
        default: {
                     break;
                }
        }
        return setup_return;
    }
    virtual int32_t process(GraphVertex& vertex) noexcept override {
        usleep(100000);
        run_times++;
        if (process_return == 0) {
            vertex.named_emit("x")->emit<::std::string>()->assign("x_value");
        }
        return process_return;
    }
    int32_t setup_return {0};
    int32_t process_return {0};
    int32_t run_times {0};
    bool trivial {false};
    int32_t declare_x_type {0};
    int32_t declare_y_type {0};
    int32_t declare_emit_type {0};
};

class VertexTest : public ::testing::Test {
public:
    virtual void SetUp() {
        data[0].bind(*closure);
        for (auto& one_data : data) {
            one_data.data_num(data.size());
        }
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
    GraphVertexBuilder& builder = graph_builder.add_vertex(processor);
    GraphVertex vertex;
    ClosureContext* closure {new ClosureContextImplement<::bthread::Mutex>(executor)};
    ::std::vector<GraphData> data {1024};
    ::std::unordered_map<::std::string, size_t> data_index_by_name;
    ::std::unordered_map<size_t, const GraphVertexBuilder*> producer_by_data_index;
    GraphVertex* _runnable_vertex[1024];
    Stack<GraphVertex*> runnable_vertex {_runnable_vertex, 1024};
    GraphData* _activating_data[1024];
    Stack<GraphData*> activating_data {_activating_data, 1024};
};

TEST_F(VertexTest, setup_fail_break_build) {
    processor.setup_return = -10086;
    ASSERT_EQ(0, builder.finish(data_index_by_name, producer_by_data_index));
    ASSERT_EQ(-10086, builder.build(executor, vertex, data));
}

TEST_F(VertexTest, named_dependency_can_get_by_name) {
    builder.named_depend("x").to("data1");
    builder.anonymous_depend().to("data3");
    builder.named_depend("y").to("data2");
    ASSERT_EQ(0, builder.finish(data_index_by_name, producer_by_data_index));
    ASSERT_EQ(0, builder.build(executor, vertex, data));
    ASSERT_NE(nullptr, vertex.named_dependency("x"));
    ASSERT_NE(nullptr, vertex.named_dependency("y"));
    ASSERT_EQ(nullptr, vertex.named_dependency("z"));
    ASSERT_EQ(&data[data_index_by_name["data1"]], vertex.named_dependency("x")->_target);
    ASSERT_EQ(&data[data_index_by_name["data2"]], vertex.named_dependency("y")->_target);
}

TEST_F(VertexTest, anonymous_dependency_can_get_by_index) {
    builder.anonymous_depend().to("data1");
    builder.named_depend("x").to("data3");
    builder.anonymous_depend().to("data2");
    ASSERT_EQ(0, builder.finish(data_index_by_name, producer_by_data_index));
    ASSERT_EQ(0, builder.build(executor, vertex, data));
    ASSERT_EQ(2, vertex.anonymous_dependency_size());
    ASSERT_NE(nullptr, vertex.anonymous_dependency(0));
    ASSERT_NE(nullptr, vertex.anonymous_dependency(1));
    ASSERT_EQ(nullptr, vertex.anonymous_dependency(2));
    ASSERT_EQ(&data[data_index_by_name["data1"]], vertex.anonymous_dependency(0)->_target);
    ASSERT_EQ(&data[data_index_by_name["data2"]], vertex.anonymous_dependency(1)->_target);
}

TEST_F(VertexTest, named_emit_can_get_by_name) {
    builder.named_emit("x").to("data1");
    builder.anonymous_emit().to("data3");
    builder.named_emit("y").to("data2");
    ASSERT_EQ(0, builder.finish(data_index_by_name, producer_by_data_index));
    ASSERT_EQ(0, builder.build(executor, vertex, data));
    ASSERT_NE(nullptr, vertex.named_emit("x"));
    ASSERT_NE(nullptr, vertex.named_emit("y"));
    ASSERT_EQ(nullptr, vertex.named_emit("z"));
    ASSERT_EQ(&data[data_index_by_name["data1"]], vertex.named_emit("x"));
    ASSERT_EQ(&data[data_index_by_name["data2"]], vertex.named_emit("y"));
}

TEST_F(VertexTest, named_emit_is_uniq_by_name) {
    builder.named_emit("x").to("data1");
    builder.named_emit("x").to("data2");
    ASSERT_EQ(0, builder.finish(data_index_by_name, producer_by_data_index));
    ASSERT_EQ(0, builder.build(executor, vertex, data));
    ASSERT_NE(nullptr, vertex.named_emit("x"));
    ASSERT_EQ(nullptr, vertex.named_emit("y"));
    ASSERT_EQ(&data[data_index_by_name["data2"]], vertex.named_emit("x"));
}

TEST_F(VertexTest, anonymous_emit_can_get_by_index) {
    builder.anonymous_emit().to("data1");
    builder.named_emit("x").to("data3");
    builder.anonymous_emit().to("data2");
    ASSERT_EQ(0, builder.finish(data_index_by_name, producer_by_data_index));
    ASSERT_EQ(0, builder.build(executor, vertex, data));
    ASSERT_EQ(2, vertex.anonymous_emit_size());
    ASSERT_NE(nullptr, vertex.anonymous_emit(0));
    ASSERT_NE(nullptr, vertex.anonymous_emit(1));
    ASSERT_EQ(nullptr, vertex.anonymous_emit(2));
    ASSERT_EQ(&data[data_index_by_name["data1"]], vertex.anonymous_emit(0));
    ASSERT_EQ(&data[data_index_by_name["data2"]], vertex.anonymous_emit(1));
}

TEST_F(VertexTest, reject_multiple_emit_to_same_data) {
    {
        GraphVertexBuilder& builder = graph_builder.add_vertex(processor);
        builder.named_emit("x").to("data1");
        builder.named_emit("y").to("data1");
        ASSERT_NE(0, builder.finish(data_index_by_name, producer_by_data_index));
    }
    data_index_by_name.clear();
    producer_by_data_index.clear();
    {
        GraphVertexBuilder& builder = graph_builder.add_vertex(processor);
        builder.anonymous_emit().to("data1");
        builder.anonymous_emit().to("data1");
        ASSERT_NE(0, builder.finish(data_index_by_name, producer_by_data_index));
    }
    data_index_by_name.clear();
    producer_by_data_index.clear();
    {
        GraphVertexBuilder& builder = graph_builder.add_vertex(processor);
        builder.named_emit("x").to("data1");
        builder.anonymous_emit().to("data1");
        ASSERT_NE(0, builder.finish(data_index_by_name, producer_by_data_index));
    }
}

TEST_F(VertexTest, option_of_builder_shared_by_instance) {
    builder.option(::std::string("payload"));
    ASSERT_EQ(0, builder.finish(data_index_by_name, producer_by_data_index));
    {
        GraphVertex vertex;
        ASSERT_EQ(0, builder.build(executor, vertex, data));
        ASSERT_STREQ("payload", vertex.option<::std::string>()->c_str());
    }
    {
        GraphVertex vertex;
        ASSERT_EQ(0, builder.build(executor, vertex, data));
        ASSERT_STREQ("payload", vertex.option<::std::string>()->c_str());
    }
}

TEST_F(VertexTest, context_not_cleared_by_reset) {
    ASSERT_EQ(0, builder.finish(data_index_by_name, producer_by_data_index));
    ASSERT_EQ(0, builder.build(executor, vertex, data));
    vertex.context<::std::string>()->assign("payload");
    vertex.reset();
    ASSERT_STREQ("payload", vertex.context<::std::string>()->c_str());
}

TEST_F(VertexTest, no_depend_vertex_become_runnable_when_activate) {
    ASSERT_EQ(0, builder.finish(data_index_by_name, producer_by_data_index));
    ASSERT_EQ(0, builder.build(executor, vertex, data));
    ASSERT_EQ(0, vertex.activate(activating_data, runnable_vertex, closure));
    ASSERT_EQ(0, activating_data.size());
    ASSERT_EQ(1, runnable_vertex.size());
    ASSERT_EQ(&vertex, runnable_vertex[0]);
}

TEST_F(VertexTest, vertex_activation_only_once) {
    ASSERT_EQ(0, builder.finish(data_index_by_name, producer_by_data_index));
    ASSERT_EQ(0, builder.build(executor, vertex, data));
    {
        BABYLON_STACK(GraphVertex*, runnable_vertex, 2);
        BABYLON_STACK(GraphData*, activating_data, 2);
        ASSERT_EQ(0, vertex.activate(activating_data, runnable_vertex, closure));
        ASSERT_EQ(0, activating_data.size());
        ASSERT_EQ(1, runnable_vertex.size());
        ASSERT_EQ(&vertex, runnable_vertex[0]);
    }
    {
        BABYLON_STACK(GraphVertex*, runnable_vertex, 2);
        BABYLON_STACK(GraphData*, activating_data, 2);
        ASSERT_EQ(0, vertex.activate(activating_data, runnable_vertex, closure));
        ASSERT_EQ(0, activating_data.size());
        ASSERT_EQ(0, runnable_vertex.size());
    }
}

TEST_F(VertexTest, vertex_become_runnable_when_activate_with_depned_already_ready) {
    builder.named_depend("x").to("data1");
    ASSERT_EQ(0, builder.finish(data_index_by_name, producer_by_data_index));
    ASSERT_EQ(0, builder.build(executor, vertex, data));
    data[0].emit<::std::string>()->assign("payload");
    ASSERT_EQ(0, vertex.activate(activating_data, runnable_vertex, closure));
    ASSERT_EQ(0, activating_data.size());
    ASSERT_EQ(1, runnable_vertex.size());
    ASSERT_EQ(&vertex, runnable_vertex[0]);
}

TEST_F(VertexTest, also_activate_dependency_not_ready) {
    builder.named_depend("x").to("data1");
    ASSERT_EQ(0, builder.finish(data_index_by_name, producer_by_data_index));
    ASSERT_EQ(0, builder.build(executor, vertex, data));
    ASSERT_EQ(0, vertex.activate(activating_data, runnable_vertex, closure));
    ASSERT_EQ(1, activating_data.size());
    ASSERT_EQ(&data[0], activating_data[0]);
    ASSERT_EQ(0, runnable_vertex.size());
}

TEST_F(VertexTest, dependency_know_whether_its_the_last_one) {
    builder.named_depend("x").to("data1");
    builder.named_depend("y").to("data2");
    builder.named_depend("z").to("data3");
    ASSERT_EQ(0, builder.finish(data_index_by_name, producer_by_data_index));
    ASSERT_EQ(0, builder.build(executor, vertex, data));
    data[0].emit<::std::string>()->assign("payload");
    ASSERT_EQ(0, vertex.activate(activating_data, runnable_vertex, closure));
    ASSERT_EQ(2, activating_data.size());
    ASSERT_EQ(&data[1], activating_data[0]);
    ASSERT_EQ(&data[2], activating_data[1]);
    ASSERT_EQ(0, runnable_vertex.size());
    ASSERT_FALSE(vertex.ready(vertex.named_dependency("y")));
    ASSERT_TRUE(vertex.ready(vertex.named_dependency("z")));
}

TEST_F(VertexTest, invoke_run_processor_with_executor) {
    builder.named_emit("x").to("data1");
    ASSERT_EQ(0, builder.finish(data_index_by_name, producer_by_data_index));
    ASSERT_EQ(0, builder.build(executor, vertex, data));
    ASSERT_EQ(0, vertex.activate(activating_data, runnable_vertex, closure));
    ASSERT_EQ(0, processor.run_times);
    ASSERT_EQ(1, runnable_vertex.size());
    runnable_vertex.pop_back();
    vertex.invoke(runnable_vertex);
    ASSERT_EQ(0, runnable_vertex.size());
    closure->fire();
    ASSERT_FALSE(closure->finished());
    ASSERT_EQ(0, closure->get());
    delete closure;
    closure = nullptr;
    ASSERT_EQ(1, processor.run_times);
    ASSERT_NE(nullptr, data[0].cvalue<::std::string>());
    ASSERT_STREQ("x_value", data[0].cvalue<::std::string>()->c_str());
}

TEST_F(VertexTest, processor_error_finish_closure_with_error) {
    builder.named_emit("x").to("data1");
    ASSERT_EQ(0, builder.finish(data_index_by_name, producer_by_data_index));
    ASSERT_EQ(0, builder.build(executor, vertex, data));
    ASSERT_EQ(0, vertex.activate(activating_data, runnable_vertex, closure));
    ASSERT_EQ(0, processor.run_times);
    processor.process_return = -10087;
    ASSERT_EQ(1, runnable_vertex.size());
    runnable_vertex.pop_back();
    vertex.invoke(runnable_vertex);
    ASSERT_EQ(0, runnable_vertex.size());
    closure->fire();
    ASSERT_FALSE(closure->finished());
    ASSERT_EQ(-10087, closure->get());
    delete closure;
    closure = nullptr;
    ASSERT_EQ(1, processor.run_times);
    ASSERT_EQ(nullptr, data[0].cvalue<::std::string>());
}

TEST_F(VertexTest, trivial_invoke_run_processor_inplace) {
    processor.trivial = true;
    builder.named_emit("x").to("data1");
    ASSERT_EQ(0, builder.finish(data_index_by_name, producer_by_data_index));
    ASSERT_EQ(0, builder.build(executor, vertex, data));
    ASSERT_EQ(0, vertex.activate(activating_data, runnable_vertex, closure));
    ASSERT_EQ(0, processor.run_times);
    ASSERT_EQ(1, runnable_vertex.size());
    runnable_vertex.pop_back();
    vertex.invoke(runnable_vertex);
    ASSERT_EQ(0, runnable_vertex.size());
    closure->fire();
    ASSERT_TRUE(closure->finished());
    ASSERT_STREQ("x_value", data[0].cvalue<::std::string>()->c_str());
    delete closure;
    closure = nullptr;
}

TEST_F(VertexTest, vertex_activate_fail_if_dependency_fail) {
    builder.named_depend("x").to("data1").set_mutable();
    builder.named_depend("y").to("data1").set_mutable();
    ASSERT_EQ(0, builder.finish(data_index_by_name, producer_by_data_index));
    ASSERT_EQ(0, builder.build(executor, vertex, data));
    ASSERT_NE(0, vertex.activate(activating_data, runnable_vertex, closure));
}

TEST_F(VertexTest, data_activate_fail_if_vertex_fail) {
    builder.named_depend("x").to("data1").set_mutable();
    builder.named_depend("y").to("data1").set_mutable();
    builder.named_emit("z").to("data2");
    ASSERT_EQ(0, builder.finish(data_index_by_name, producer_by_data_index));
    ASSERT_EQ(0, builder.build(executor, vertex, data));
    ASSERT_NE(0, data[2].activate(activating_data, runnable_vertex, closure));
}

TEST_F(VertexTest, data_build_fail_if_different_type_declared) {
    builder.named_depend("x").to("data1");
    builder.named_depend("y").to("data1");
    builder.named_emit("z").to("data2");
    processor.declare_x_type = 1;
    processor.declare_y_type = 2;
    ASSERT_EQ(0, builder.finish(data_index_by_name, producer_by_data_index));
    ASSERT_EQ(0, builder.build(executor, vertex, data));
    ASSERT_NE(0, data[data_index_by_name["data1"]].error_code());
}

TEST_F(VertexTest, invoke_with_essential_dependency) {
    builder.named_emit("x").to("data1");
    builder.named_depend("y").to("data2").on("condition").set_essential(true);
    ASSERT_EQ(0, builder.finish(data_index_by_name, producer_by_data_index));
    ASSERT_EQ(0, builder.build(executor, vertex, data));
    ASSERT_EQ(2, data_index_by_name["data1"]);
   
    *(data[data_index_by_name["condition"]].emit<bool>()) = true;
    data[data_index_by_name["data2"]].emit<int>().clear();
    GraphDependency *dependency = vertex.named_dependency("y");
    ASSERT_FALSE(dependency->ready());
    ASSERT_TRUE(dependency->empty());
    ASSERT_TRUE(dependency->is_essential());
  
    ASSERT_EQ(0, vertex.activate(activating_data, runnable_vertex, closure));
    ASSERT_EQ(0, processor.run_times);
    ASSERT_EQ(1, runnable_vertex.size());
    ASSERT_FALSE(data[2].ready());
    
    runnable_vertex.pop_back();
    vertex.invoke(runnable_vertex);
    ASSERT_EQ(0, runnable_vertex.size());
    ASSERT_EQ(&runnable_vertex, vertex.runnable_vertexes());
    closure->fire();
    ASSERT_TRUE(closure->finished());
    ASSERT_EQ(0, closure->get());
    delete closure;
    closure = nullptr;
    ASSERT_EQ(0, processor.run_times);
    ASSERT_TRUE(data[2].ready());
    ASSERT_TRUE(data[2].empty());
    ASSERT_EQ(nullptr, data[2].cvalue<::std::string>());
}

TEST_F(VertexTest, support_reuse_obj_function) {
    auto str = vertex.create_local<std::string>();
    ASSERT_EQ(0, str->size());
    *str = "1234567890";
    ASSERT_EQ(10, str->size());
    vertex.reset();
    ASSERT_EQ(0, str->size());
    *str = "1234567890";
    ASSERT_EQ(10, str->size());
}
