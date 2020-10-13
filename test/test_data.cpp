#include <joewu/graph/engine/data.h>
#include <joewu/graph/engine/builder.h>
#include <joewu/graph/engine/vertex.h>
#include <joewu/graph/engine/executor.h>

#include <base/logging.h>

#include <gtest/gtest.h>

#include <inttypes.h>

using joewu::feed::graph::Graph;
using joewu::feed::graph::GraphData;
using joewu::feed::graph::GraphVertex;
using joewu::feed::graph::GraphBuilder;
using joewu::feed::graph::GraphProcessor;
using joewu::feed::graph::Commiter;
using joewu::feed::graph::ClosureContextImplement;
using joewu::feed::graph::BthreadGraphExecutor;
using ::joewu::feed::mlarch::babylon::Any;

struct DataTest : public ::testing::Test {
    // C -> [Y] -> B -> [X] -> A
    virtual void SetUp() {
        builder.executor(executor);
        {
            auto& vertex = builder.add_vertex(processor);
            vertex.anonymous_depend().to("A");
            vertex.anonymous_emit().to("B");
        }
        {
            auto& vertex = builder.add_vertex(processor);
            vertex.anonymous_depend().to("B");
            vertex.anonymous_emit().to("C");
        }
        builder.finish();
        graph = builder.build();

        a = graph->find_data("A");
        b = graph->find_data("B");
        c = graph->find_data("C");

        x = b->producer();
        y = c->producer();
    }

    virtual void TearDown() {
    }

    BthreadGraphExecutor executor;
    GraphBuilder builder;
    GraphProcessor processor;
    ::std::unique_ptr<Graph> graph;

    GraphData* a;
    GraphData* b;
    GraphData* c;
    GraphVertex* x;
    GraphVertex* y;
};

TEST(data, default_constructable) {
    ::std::vector<GraphData> data(10);
}

TEST(data, acquire_success_only_once) {
    GraphData data;
    ASSERT_TRUE(data.acquire());
    ASSERT_FALSE(data.acquire());
}

TEST(data, release_make_data_ready) {
    GraphData data;
    ASSERT_FALSE(data.ready());
    data.release();
    ASSERT_TRUE(data.ready());
}

TEST(data, acquire_and_release_default_publish_empty_data) {
    {
        GraphData data;
        ASSERT_TRUE(data.acquire());
        *data.certain_type_non_reference_mutable_value<int32_t>() = 0;
        data.release();
        ASSERT_TRUE(data.ready());
        ASSERT_TRUE(data.empty());
    }
    {
        GraphData data;
        ASSERT_TRUE(data.acquire());
        *data.certain_type_non_reference_mutable_value<int32_t>() = 0;
        data.empty(false);
        data.release();
        ASSERT_TRUE(data.ready());
        ASSERT_FALSE(data.empty());

    }
}

TEST(data, writable_value_create_value_on_first_call) {
    GraphData data;
    ASSERT_TRUE(data.acquire());
    ASSERT_EQ(nullptr, data.cvalue<::std::string>());
    ASSERT_NE(nullptr, data.certain_type_non_reference_mutable_value<::std::string>());
    data.empty(false);
    ASSERT_NE(nullptr, data.cvalue<::std::string>());
}

TEST(data, reset_manipulate_state_but_not_value) {
    GraphData data;
    ASSERT_TRUE(data.acquire());
    data.certain_type_non_reference_mutable_value<::std::string>()->assign("some value");
    data.empty(false);
    data.release();
    data.reset();

    ASSERT_EQ(nullptr, data.cvalue<::std::string>());
    ASSERT_TRUE(data.acquire());
    ASSERT_STREQ("some value", data.certain_type_non_reference_mutable_value<::std::string>()->c_str());
    data.release();
}

TEST(data, commiter_compete_for_data) {
    GraphData data;
    auto commiter1 = data.emit<::std::string>();
    auto commiter2 = data.emit<::std::string>();
    ASSERT_TRUE(commiter1.valid());
    ASSERT_TRUE(commiter1);
    ASSERT_NE(nullptr, commiter1.get());
    ASSERT_FALSE(commiter2.valid());
    ASSERT_FALSE(commiter2);
    ASSERT_EQ(nullptr, commiter2.get());
}

TEST(data, commiter_can_output_ref) {
    GraphData data;
    ::std::string value("some value");
    {
        auto commiter = data.emit<::std::string>();
        commiter.ref(value);
    }
    ASSERT_EQ(&value, data.value<::std::string>());
    ASSERT_EQ(&value, data.cvalue<::std::string>());

    data.reset();
    {
        auto commiter = data.emit<::std::string>();
        commiter.cref(value);
    }
    ASSERT_EQ(nullptr, data.mutable_value<::std::string>());
    ASSERT_EQ(&value, data.cvalue<::std::string>());
}

TEST(data, commiter_release_data_on_demend) {
    GraphData data;
    auto commiter = data.emit<::std::string>();
    ASSERT_FALSE(data.ready());
    commiter.release();
    ASSERT_TRUE(data.ready());
    ASSERT_FALSE(commiter.valid());
}

TEST(data, commiter_release_data_when_destruct) {
    GraphData data;
    {
        auto commiter = data.emit<::std::string>();
        ASSERT_FALSE(data.ready());
    }
    ASSERT_TRUE(data.ready());
}

TEST(data, commiter_work_as_pointer) {
    GraphData data;
    {
        auto commiter = data.emit<::std::string>();
        commiter->assign("some value");
        ASSERT_STREQ("some value", (*commiter).c_str());
    }
    ASSERT_STREQ("some value", data.cvalue<::std::string>()->c_str());
}

TEST(data, commiter_support_move) {
    GraphData data;
    auto commiter1 = data.emit<::std::string>();
    {
        Commiter<::std::string> commiter2(::std::move(commiter1));
        commiter2->assign("some value");
    }
    ASSERT_FALSE(commiter1);
    ASSERT_STREQ("some value", data.cvalue<::std::string>()->c_str());
}

TEST(data, closure_bind_fail_when_data_released) {
    GraphData data;
    BthreadGraphExecutor executor;
    ClosureContextImplement<::std::mutex> closure(executor);
    {
        data.emit<::std::string>();
    }
    ASSERT_FALSE(data.bind(closure));
    closure.fire();
}

TEST(data, closure_finish_when_data_ready) {
    GraphData data;
    BthreadGraphExecutor executor;
    ClosureContextImplement<::std::mutex> closure(executor);
    closure.depend_vertex_add();
    {
        auto commiter = data.emit<::std::string>();
        ASSERT_TRUE(data.bind(closure));
        closure.fire();
        ASSERT_FALSE(closure.finished());
    }
    ASSERT_TRUE(closure.finished());
    closure.depend_vertex_sub();
}

TEST(data, mark_active_at_most_once) {
    GraphData data;
    ASSERT_FALSE(data.mark_active());
    ASSERT_TRUE(data.mark_active());
}

TEST(data, trigger_at_most_once) {
    GraphData data;
    {
        BABYLON_STACK(GraphData*, activating_data, 1);
        data.trigger(activating_data);
        ASSERT_EQ(1, activating_data.size());
    }
    {
        BABYLON_STACK(GraphData*, activating_data, 1);
        data.trigger(activating_data);
        ASSERT_EQ(0, activating_data.size());
    }
}

TEST(data, activate_data_means_activate_producer) {
    GraphData data;
    GraphVertex vertex;
    data.producer(vertex);
    BthreadGraphExecutor executor;
    ClosureContextImplement<::std::mutex> closure(executor);

    ASSERT_FALSE(vertex.activated());
    BABYLON_STACK(GraphData*, activating_data, 1);
    BABYLON_STACK(GraphVertex*, runnable_vertex, 1);
    ASSERT_EQ(0, data.activate(activating_data, runnable_vertex, &closure));
    ASSERT_EQ(0, activating_data.size());
    ASSERT_EQ(1, runnable_vertex.size());
    ASSERT_EQ(&vertex, runnable_vertex[0]);
    ASSERT_TRUE(vertex.activated());

    closure.depend_data_sub();
    closure.depend_vertex_sub();
}

TEST(data, activate_data_fail_for_no_producer) {
    GraphData data;
    BthreadGraphExecutor executor;
    ClosureContextImplement<::std::mutex> closure(executor);

    BABYLON_STACK(GraphData*, activating_data, 1);
    BABYLON_STACK(GraphVertex*, runnable_vertex, 1);
    ASSERT_NE(0, data.activate(activating_data, runnable_vertex, &closure));

    closure.fire();
}

TEST_F(DataTest, recursive_activate_all_by_depend_accessible) {
    a->emit<bool>();
    ClosureContextImplement<::std::mutex> closure(executor);

    ASSERT_FALSE(x->activated());
    ASSERT_FALSE(y->activated());
    BABYLON_STACK(GraphVertex*, runnable_vertex, 10);
    ASSERT_EQ(0, c->recursive_activate(runnable_vertex, &closure));
    ASSERT_EQ(1, runnable_vertex.size());
    ASSERT_EQ(x, runnable_vertex[0]);
    ASSERT_TRUE(x->activated());
    ASSERT_TRUE(y->activated());

    closure.fire();
}

TEST_F(DataTest, can_change_type_after_reset) {
    a->emit<::std::string>()->assign("123");
    ASSERT_STREQ("123", a->value<::std::string>()->c_str());
    a->reset();
    a->emit<::std::vector<::std::string>>()->emplace_back("456");
    ASSERT_STREQ("456", a->value<::std::vector<::std::string>>()->back().c_str());
    a->reset();
    ::std::string str("789");
    a->emit<::std::string>().ref(str);
    ASSERT_EQ(&str, a->value<::std::string>());
    a->reset();
    a->emit<::std::string>()->assign("123");
    ASSERT_NE(&str, a->value<::std::string>());
    ASSERT_STREQ("123", a->value<::std::string>()->c_str());
}

TEST_F(DataTest, support_instance_not_default_constructible) {
    struct A {
        A(int32_t value) : value(value) {}
        int32_t value {0};
    };
    *a->emit<Any>() = A(123);
    ASSERT_EQ(123, a->value<A>()->value);
}

TEST_F(DataTest, ref_clear_on_first_access_but_keep_after_ref_before_reset) {
    ::std::string str("789");
    auto committer = a->emit<::std::string>();
    committer.ref(str);
    ASSERT_EQ(str.size(), committer->size());
    ASSERT_EQ(&str, committer.get());
    committer.release();
    graph->reset();
    committer = a->emit<::std::string>();
    ASSERT_EQ(0, committer->size());
    ASSERT_NE(&str, committer.get());
}

TEST_F(DataTest, preset_value_will_use_in_emit_once) {
    ::std::string str;
    a->preset(str);
    auto committer = a->emit<::std::string>();
    ASSERT_EQ(&str, committer.get());
    committer.release();
    graph->reset();
    committer = a->emit<::std::string>();
    ASSERT_NE(&str, committer.get());
}

TEST_F(DataTest, support_non_copyable_nor_moveable_class) {
    struct S {
        S() = default;
        S(const S&) = delete;
        S(S&&) = delete;
    } s;
    a->emit<S>().get();
    graph->reset();
    a->emit<S>().ref(s);
    graph->reset();
    a->emit<S>().get();
}

TEST_F(DataTest, conflict_type_declare_make_data_fail) {
    struct Int32Processor : public GraphProcessor {
        int32_t setup(GraphVertex& vertex) const noexcept override {
            vertex.anonymous_dependency(0)->declare_type<int32_t>();
            return 0;
        }
    } int32_processor;
    struct Int64Processor : public GraphProcessor {
        int32_t setup(GraphVertex& vertex) const noexcept override {
            vertex.anonymous_dependency(0)->declare_type<int64_t>();
            return 0;
        }
    } int64_processor;
    struct AnyProcessor : public GraphProcessor {
        int32_t setup(GraphVertex& vertex) const noexcept override {
            vertex.anonymous_dependency(0)->declare_type<Any>();
            return 0;
        }
    } any_processor;
    {
        GraphBuilder builder;
        builder.executor(executor);
        {
            auto& vertex = builder.add_vertex(int32_processor);
            vertex.anonymous_depend().to("A");
            vertex.anonymous_emit().to("B");
        }
        {
            auto& vertex = builder.add_vertex(int32_processor);
            vertex.anonymous_depend().to("A");
            vertex.anonymous_emit().to("C");
        }
        ASSERT_EQ(0, builder.finish());
        ASSERT_NE(nullptr, builder.build().get());
    }
    {
        GraphBuilder builder;
        builder.executor(executor);
        {
            auto& vertex = builder.add_vertex(int32_processor);
            vertex.anonymous_depend().to("A");
            vertex.anonymous_emit().to("B");
        }
        {
            auto& vertex = builder.add_vertex(any_processor);
            vertex.anonymous_depend().to("A");
            vertex.anonymous_emit().to("C");
        }
        ASSERT_EQ(0, builder.finish());
        ASSERT_NE(nullptr, builder.build().get());
    }
    {
        GraphBuilder builder;
        builder.executor(executor);
        {
            auto& vertex = builder.add_vertex(int32_processor);
            vertex.anonymous_depend().to("A");
            vertex.anonymous_emit().to("B");
        }
        {
            auto& vertex = builder.add_vertex(int64_processor);
            vertex.anonymous_depend().to("A");
            vertex.anonymous_emit().to("C");
        }
        ASSERT_EQ(0, builder.finish());
        ASSERT_EQ(nullptr, builder.build().get());
    }
}
