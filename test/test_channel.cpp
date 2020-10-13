#include <joewu/graph/engine/builder.h>
#include <joewu/graph/engine/data.h>
#include <joewu/graph/engine/executor.h>
#include <joewu/graph/engine/vertex.h>

#include <joewu/feed/mlarch/babylon/future.h>
#include <base/logging.h>

#include <gtest/gtest.h>

using joewu::feed::graph::BthreadGraphExecutor;
using joewu::feed::graph::ChannelConsumer;
using joewu::feed::graph::ChannelPublisher;
using joewu::feed::graph::Graph;
using joewu::feed::graph::GraphBuilder;
using joewu::feed::graph::GraphData;
using joewu::feed::graph::GraphDependency;
using joewu::feed::graph::GraphProcessor;
using joewu::feed::graph::GraphVertex;
using joewu::feed::mlarch::babylon::ConcurrentTransientQueue;
using joewu::feed::mlarch::babylon::Promise;

struct Context {
    void break_and_wait() {
        breaked.store(true);
        while (!continue_run.load()) {
            bthread_yield();
        }
        continue_run.store(false);
    }
    void wait_until_break() {
        while (!breaked.load()) {
            bthread_yield();
        }
    }
    void resume() {
        continue_run.store(true);
        breaked.store(false);
    }
    ::std::atomic<bool> breaked {false};
    ::std::atomic<bool> continue_run {false};
};

struct Processor : public GraphProcessor {
    int32_t process(GraphVertex& vertex) noexcept override {
        vertex.context<Context>()->break_and_wait();
        return 0;
    }
};

struct ChannelTest : public ::testing::Test {
    // [X] -> A -> [Y] -> B
    virtual void SetUp() {
        builder.executor(executor);
        {
            auto& vertex = builder.add_vertex(processor);
            vertex.anonymous_emit().to("A");
        }
        {
            auto& vertex = builder.add_vertex(processor);
            vertex.anonymous_depend().to("A");
            vertex.anonymous_emit().to("B");
        }
        builder.finish();
        graph = builder.build();

        a = graph->find_data("A");
        b = graph->find_data("B");
        x = a->producer();
        x->context<Context>();
        y = b->producer();
        y->context<Context>();
        dya = y->anonymous_dependency(0);
    }

    BthreadGraphExecutor executor;
    GraphBuilder builder;
    Processor processor;
    ::std::unique_ptr<Graph> graph;

    GraphData* a;
    GraphData* b;
    GraphVertex* x;
    GraphVertex* y;
    GraphDependency* dya;
};

TEST_F(ChannelTest, data_ready_after_channel_open) {
    auto ac = a->declare_channel<::std::string>();
    auto bc = b->declare_channel<::std::string>();
    {
        auto closure = graph->run(b);
        x->context<Context>()->wait_until_break();
        x->context<Context>()->resume();
        ASSERT_NE(0, closure.get());
    }
    graph->reset();
    {
        auto closure = graph->run(b);
        x->context<Context>()->wait_until_break();
        auto publisher = ac.open();
        x->context<Context>()->resume();
        y->context<Context>()->wait_until_break();
        y->context<Context>()->resume();
        ASSERT_NE(0, closure.get());
    }
    graph->reset();
    {
        auto closure = graph->run(b);
        x->context<Context>()->wait_until_break();
        auto publisher_a = ac.open();
        x->context<Context>()->resume();
        y->context<Context>()->wait_until_break();
        auto publisher_b = bc.open();
        ASSERT_EQ(0, closure.get());
        y->context<Context>()->resume();
    }
}

TEST_F(ChannelTest, publish_consume_through_channel) {
    auto ac = a->declare_channel<::std::string>();
    auto dyac = dya->declare_channel<::std::string>();
    {
        auto closure = graph->run(b);
        x->context<Context>()->wait_until_break();
        auto publisher = ac.open();
        *publisher->publish() = "10086";
        auto consumer = dyac.subscribe();
        ASSERT_EQ("10086", *consumer.consume());
        x->context<Context>()->resume();
        y->context<Context>()->wait_until_break();
        y->context<Context>()->resume();
    }
    graph->reset();
    {
        auto closure = graph->run(b);
        x->context<Context>()->wait_until_break();
        auto publisher = ac.open();
        *publisher->publish() = "10086";
        *(*publisher).publish() = "10010";
        auto consumer = dyac.subscribe();
        auto range = consumer.consume(2);
        ASSERT_EQ(2, range.size());
        ASSERT_EQ("10086", range[0]);
        ASSERT_EQ("10010", range[1]);
        x->context<Context>()->resume();
        y->context<Context>()->wait_until_break();
        y->context<Context>()->resume();
    }
}

TEST_F(ChannelTest, publisher_can_default_construct_and_assign) {
    auto ac = a->declare_channel<::std::string>();
    auto dyac = dya->declare_channel<::std::string>();
    {
        ChannelPublisher<::std::string> publisher;
        publisher = ac.open();
        auto closure = graph->run(b);
        *publisher->publish() = "10086";
        auto consumer = dyac.subscribe();
        ASSERT_EQ("10086", *consumer.consume());
        y->context<Context>()->wait_until_break();
        y->context<Context>()->resume();
    }
    graph->reset();
    {
        ChannelPublisher<::std::string> publisher;
        publisher = ac.open();
        auto closure = graph->run(b);
        *publisher->publish() = "10086";
        auto consumer = dyac.subscribe();
        ASSERT_EQ("10086", *consumer.consume());
        y->context<Context>()->wait_until_break();
        y->context<Context>()->resume();
        publisher = ChannelPublisher<::std::string>();
    }
    graph->reset();
    {
        auto pub = ac.open();
        ChannelPublisher<::std::string> publisher(::std::move(pub));
        auto closure = graph->run(b);
        *publisher->publish() = "10086";
        auto consumer = dyac.subscribe();
        ASSERT_EQ("10086", *consumer.consume());
        y->context<Context>()->wait_until_break();
        y->context<Context>()->resume();
        publisher = ChannelPublisher<::std::string>();
    }
}

TEST_F(ChannelTest, mutable_channel_get_mutable_item) {
    auto ac = a->declare_channel<::std::string>();
    auto dyac = dya->declare_mutable_channel<::std::string>();
    {
        auto closure = graph->run(b);
        x->context<Context>()->wait_until_break();
        auto publisher = ac.open();
        *publisher->publish() = "10086";
        auto consumer = dyac.subscribe();
        auto* value = consumer.consume();
        value->append("10010");
        ASSERT_EQ("1008610010", *value);
        x->context<Context>()->resume();
        y->context<Context>()->wait_until_break();
        y->context<Context>()->resume();
    }
    graph->reset();
    {
        auto closure = graph->run(b);
        x->context<Context>()->wait_until_break();
        auto publisher = ac.open();
        *publisher->publish() = "10086";
        *publisher->publish() = "10010";
        auto consumer = dyac.subscribe();
        auto range = consumer.consume(2);
        ASSERT_EQ(2, range.size());
        range[0].append("10010");
        ASSERT_EQ("1008610010", range[0]);
        ASSERT_EQ("10010", range[1]);
        x->context<Context>()->resume();
        y->context<Context>()->wait_until_break();
        y->context<Context>()->resume();
    }
}

TEST_F(ChannelTest, publisher_close_channel_when_destruct) {
    auto ac = a->declare_channel<::std::string>();
    auto dyac = dya->declare_channel<::std::string>();
    {
        auto closure = graph->run(b);
        x->context<Context>()->wait_until_break();
        ChannelConsumer<::std::string> consumer;
        {
            auto publisher = ac.open();
            *publisher->publish() = "10086";
            consumer = dyac.subscribe();
            ASSERT_EQ("10086", *consumer.consume());
        }
        ASSERT_EQ(nullptr, consumer.consume());
        x->context<Context>()->resume();
        y->context<Context>()->wait_until_break();
        y->context<Context>()->resume();
    }
}

TEST_F(ChannelTest, subscribe_not_a_valid_ready_channel_dependency_get_default_empty_consumer) {
    auto dyac = dya->declare_channel<::std::string>();
    {
        auto closure = graph->run(b);
        x->context<Context>()->wait_until_break();
        *a->emit<::std::string>() = "10086";
        auto consumer = dyac.subscribe();
        ASSERT_EQ(nullptr, consumer.consume());
        x->context<Context>()->resume();
        y->context<Context>()->wait_until_break();
        y->context<Context>()->resume();
    }
}

TEST_F(ChannelTest, mutable_subscribe_mutable_forward_channel) {
    ConcurrentTransientQueue<::std::string> queue;
    auto ac = a->declare_channel<::std::string>();
    auto dyac = dya->declare_mutable_channel<::std::string>();
    {
        auto closure = graph->run(b);
        x->context<Context>()->wait_until_break();
        ac.forward(queue);
        *queue.publish() = "10086";
        auto consumer = dyac.subscribe();
        auto* value = consumer.consume();
        value->append("10010");
        ASSERT_EQ("1008610010", *value);
        x->context<Context>()->resume();
        y->context<Context>()->wait_until_break();
        y->context<Context>()->resume();
    }
}

TEST_F(ChannelTest, mutable_subscribe_non_mutable_channel_get_default_empty_consumer) {
    ConcurrentTransientQueue<::std::string> queue;
    auto ac = a->declare_channel<::std::string>();
    auto dyac = dya->declare_mutable_channel<::std::string>();
    {
        auto closure = graph->run(b);
        x->context<Context>()->wait_until_break();
        ac.forward((const ConcurrentTransientQueue<::std::string>&)queue);
        *queue.publish() = "10086";
        auto consumer = dyac.subscribe();
        ASSERT_EQ(nullptr, consumer.consume());
        x->context<Context>()->resume();
        y->context<Context>()->wait_until_break();
        y->context<Context>()->resume();
    }
}
