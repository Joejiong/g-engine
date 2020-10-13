#include <joewu/graph/engine/vertex.h>

#include <joewu/graph/engine/builder.h>

#include <gtest/gtest.h>

#include <future>

using ::joewu::feed::graph::Graph;
using ::joewu::feed::graph::GraphBuilder;
using ::joewu::feed::graph::GraphData;
using ::joewu::feed::graph::GraphFunction;
using ::joewu::feed::graph::GraphProcessor;
using ::joewu::feed::graph::GraphVertex;
using ::joewu::feed::mlarch::babylon::ApplicationContext;
using ::joewu::feed::mlarch::babylon::Any;
using ::joewu::feed::mlarch::babylon::string_printf;
using ::joewu::feed::mlarch::babylon::ConcurrentTransientQueue;

struct AddFunction : public GraphFunction {
    virtual int32_t setup(const Any&) noexcept override {
        set_up_called = true;
        return 0;
    }

    virtual int32_t operator()() noexcept override {
        string_printf(x.emit().get(), "%d", (a == nullptr ? 100 : *a) + *b + *c);
        return 0;
    }

    virtual void reset() noexcept override {
        reset_called = true;
    }

    GRAPH_FUNCTION_INTERFACE(
        GRAPH_FUNCTION_DEPEND_DATA(int32_t, a, 0)
        GRAPH_FUNCTION_DEPEND_DATA(int32_t, b, 1)
        GRAPH_FUNCTION_DEPEND_DATA(int32_t, c)
        GRAPH_FUNCTION_EMIT_DATA(::std::string, x)
    );
    static bool set_up_called;
    static bool reset_called;
};
bool AddFunction::set_up_called = false;
bool AddFunction::reset_called = false;
BABYLON_REGISTER_FACTORY_COMPONENT_WITH_TYPE_NAME(AddFunction, GraphProcessor, AddFunction);

struct PlusFunction : public GraphFunction {
    virtual int32_t setup(const Any& option) noexcept override {
        increasement = option.as<size_t>();
        return 0;
    }

    virtual int32_t operator()() noexcept override {
        auto publisher = x.open();
        for (auto* item = a.consume(); item != nullptr; item = a.consume()) {
            auto committer = publisher->publish();
            string_printf(*committer, "%d", *item + increasement);
        }
        return 0;
    }

    size_t increasement;

    GRAPH_FUNCTION_INTERFACE(
        GRAPH_FUNCTION_DEPEND_CHANNEL(int32_t, a)
        GRAPH_FUNCTION_EMIT_CHANNEL(::std::string, x)
    );
};
BABYLON_REGISTER_FACTORY_COMPONENT_WITH_TYPE_NAME(PlusFunction, GraphProcessor, PlusFunction);

struct FunctionTest : public ::testing::Test {
    static void SetUpTestCase() {
        ApplicationContext::instance().initialize(false);
    }
    
    virtual void SetUp() override {
        {
            auto& vertex = builder.add_vertex("AddFunction");
            vertex.named_depend("a").to("A");
            vertex.named_depend("b").to("B");
            vertex.named_depend("c").to("C");
            vertex.named_emit("x").to("D");
        }
        {
            auto& vertex = builder.add_vertex("PlusFunction");
            vertex.option(50);
            vertex.named_depend("a").to("CA");
            vertex.named_emit("x").to("CX");
        }
        builder.finish();
        graph = builder.build();
        a = graph->find_data("A");
        b = graph->find_data("B");
        c = graph->find_data("C");
        d = graph->find_data("D");
        ca = graph->find_data("CA");
        cx = graph->find_data("CX");
    }

    GraphBuilder builder;
    ::std::unique_ptr<Graph> graph;
    GraphData* a;
    GraphData* b;
    GraphData* c;
    GraphData* d;
    GraphData* ca;
    GraphData* cx;
};


TEST_F(FunctionTest, setup_when_build) {
    AddFunction::set_up_called = false;
    ASSERT_TRUE((bool)builder.build());
    ASSERT_TRUE(AddFunction::set_up_called);
}

TEST_F(FunctionTest, run_function_as_processor) {
    *a->emit<int32_t>() = 1;
    *b->emit<int32_t>() = 2;
    *c->emit<int32_t>() = 3;
    ASSERT_EQ(0, graph->run(d).get());
    ASSERT_EQ("6", *d->value<::std::string>());
}

TEST_F(FunctionTest, run_stream_function_as_processor) {
    {
        auto publisher = ca->declare_channel<int32_t>().open();
        *publisher->publish() = 1;
        *publisher->publish() = 2;
    }
    ASSERT_EQ(0, graph->run(cx).get());
    auto consumer = cx->value<ConcurrentTransientQueue<::std::string>>()->subscribe();
    ASSERT_EQ("51", *consumer.consume());
    ASSERT_EQ("52", *consumer.consume());
    ASSERT_EQ(nullptr, consumer.consume());
}

TEST_F(FunctionTest, reset_with_graph) {
    *a->emit<int32_t>() = 1;
    *b->emit<int32_t>() = 2;
    *c->emit<int32_t>() = 3;
    ASSERT_EQ(0, graph->run(d).get());
    ASSERT_EQ("6", *d->value<::std::string>());
    AddFunction::reset_called = false;
    graph->reset();
    ASSERT_TRUE(AddFunction::reset_called);
}

TEST_F(FunctionTest, fail_when_essential_level_2_depend_empty) {
    *a->emit<int32_t>() = 1;
    *b->emit<int32_t>() = 2;
    c->emit<int32_t>();
    ASSERT_NE(0, graph->run(d).get());
    graph->reset();
    *a->emit<int32_t>() = 1;
    *b->emit<int32_t>() = 2;
    *c->emit<int64_t>() = 3;
    ASSERT_NE(0, graph->run(d).get());
}

TEST_F(FunctionTest, skip_when_essential_level_1_depend_empty) {
    *a->emit<int32_t>() = 1;
    b->emit<int32_t>();
    *c->emit<int32_t>() = 3;
    ASSERT_EQ(0, graph->run(d).get());
    ASSERT_EQ(nullptr, d->value<::std::string>());
    graph->reset();
    *a->emit<int32_t>() = 1;
    *b->emit<int64_t>() = 2;
    *c->emit<int32_t>() = 3;
    ASSERT_NE(0, graph->run(d).get());
}

TEST_F(FunctionTest, essential_level_0_can_handle_empty_as_needed) {
    a->emit<int32_t>();
    *b->emit<int32_t>() = 2;
    *c->emit<int32_t>() = 3;
    ASSERT_EQ(0, graph->run(d).get());
    ASSERT_EQ("105", *d->value<::std::string>());
    graph->reset();
    *a->emit<int64_t>() = 1;
    *b->emit<int32_t>() = 2;
    *c->emit<int32_t>() = 3;
    ASSERT_EQ(0, graph->run(d).get());
    ASSERT_EQ("105", *d->value<::std::string>());
}

TEST_F(FunctionTest, build_fail_if_type_conflict) {
    struct F : public GraphFunction {
        GRAPH_FUNCTION_INTERFACE(
            GRAPH_FUNCTION_DEPEND_DATA(int32_t, a)
            GRAPH_FUNCTION_EMIT_DATA(::std::string, x)
        );
    } f;
    auto& vertex = builder.add_vertex(f);
    vertex.named_depend("a").to("D");
    vertex.named_emit("x").to("E");
    ASSERT_EQ(0, builder.finish());
    ASSERT_FALSE((bool)builder.build());
}

TEST_F(FunctionTest, any_accept_any_type) {
    struct F : public GraphFunction {
        virtual int32_t operator()() noexcept override {
            x.emit().ref(*a->get<::std::string>());
            return 0;
        }
        GRAPH_FUNCTION_INTERFACE(
            GRAPH_FUNCTION_DEPEND_DATA(Any, a)
            GRAPH_FUNCTION_EMIT_DATA(::std::string, x)
        );
    } f;
    auto& vertex = builder.add_vertex(f);
    vertex.named_depend("a").to("D");
    vertex.named_emit("x").to("E");
    ASSERT_EQ(0, builder.finish());
    graph = builder.build();
    ASSERT_TRUE((bool)graph);
    a = graph->find_data("A");
    b = graph->find_data("B");
    c = graph->find_data("C");
    auto* e = graph->find_data("E");
    *a->emit<int32_t>() = 1;
    *b->emit<int32_t>() = 2;
    *c->emit<int32_t>() = 3;
    ASSERT_EQ(0, graph->run(e).get());
    ASSERT_EQ("6", *e->value<::std::string>());
}

TEST_F(FunctionTest, mutable_data_get_non_const_pointer) {
    struct F : public GraphFunction {
        virtual int32_t operator()() noexcept override {
            a->append(" end");
            x.emit().ref(*a);
            return 0;
        }
        GRAPH_FUNCTION_INTERFACE(
            GRAPH_FUNCTION_DEPEND_MUTABLE_DATA(::std::string, a)
            GRAPH_FUNCTION_EMIT_DATA(::std::string, x)
        );
    } f;
    auto& vertex = builder.add_vertex(f);
    vertex.named_depend("a").to("D");
    vertex.named_emit("x").to("E");
    ASSERT_EQ(0, builder.finish());
    graph = builder.build();
    ASSERT_TRUE((bool)graph);
    a = graph->find_data("A");
    b = graph->find_data("B");
    c = graph->find_data("C");
    auto* e = graph->find_data("E");
    *a->emit<int32_t>() = 1;
    *b->emit<int32_t>() = 2;
    *c->emit<int32_t>() = 3;
    ASSERT_EQ(0, graph->run(e).get());
    ASSERT_EQ("6 end", *e->value<::std::string>());
}

TEST_F(FunctionTest, downstream_function_run_before_current_one_return) {
    struct F1 : public GraphFunction {
        virtual int32_t operator()() noexcept override {
            *x.emit() = *a;
            f.get();
            return 0;
        }
        GRAPH_FUNCTION_INTERFACE(
            GRAPH_FUNCTION_DEPEND_DATA(int32_t, a)
            GRAPH_FUNCTION_EMIT_DATA(int32_t, x)
        );
        ::std::promise<void> p;
        ::std::future<void> f = p.get_future();
    } f1;
    struct F2 : public GraphFunction {
        virtual int32_t operator()() noexcept override {
            *x.emit() = *a;
            return 0;
        }
        GRAPH_FUNCTION_INTERFACE(
            GRAPH_FUNCTION_DEPEND_DATA(int32_t, a)
            GRAPH_FUNCTION_EMIT_DATA(int32_t, x)
        );
    } f2;
    auto& v1 = builder.add_vertex(f1);
    v1.named_depend("a").to("X1");
    v1.named_emit("x").to("X2");
    auto& v2 = builder.add_vertex(f2);
    v2.named_depend("a").to("X2");
    v2.named_emit("x").to("X3");
    ASSERT_EQ(0, builder.finish());
    graph = builder.build();
    ASSERT_TRUE((bool)graph);
    auto* x1 = graph->find_data("X1");
    auto* x3 = graph->find_data("X3");
    *x1->emit<int32_t>() = 1;
    auto closure = graph->run(x3);
    ASSERT_EQ(0, closure.get());
    f1.p.set_value();
}

TEST_F(FunctionTest, downstream_function_run_aftert_return_to_avoid_stack_overflow) {
    struct F1 : public GraphFunction {
        virtual int32_t operator()() noexcept override {
            *x.emit() = *a;
            return 0;
        }
        GRAPH_FUNCTION_INTERFACE(
            GRAPH_FUNCTION_DEPEND_DATA(int32_t, a)
            GRAPH_FUNCTION_EMIT_DATA(int32_t, x)
        );
    } f1;
    struct F2 : public GraphFunction {
        virtual int32_t setup(const Any&) noexcept override {
            vertex().trivial();
            return 0;
        }
        virtual int32_t operator()() noexcept override {
            *x.emit() = *a;
            pe.set_value();
            f.get();
            return 0;
        }
        GRAPH_FUNCTION_INTERFACE(
            GRAPH_FUNCTION_DEPEND_DATA(int32_t, a)
            GRAPH_FUNCTION_EMIT_DATA(int32_t, x)
        );
        bool emitted = false;
        ::std::promise<void> p;
        ::std::future<void> f = p.get_future();
        ::std::promise<void> pe;
        ::std::future<void> fe = pe.get_future();
    } f2;
    struct F3 : public GraphFunction {
        virtual int32_t operator()() noexcept override {
            *x.emit() = *a;
            return 0;
        }
        GRAPH_FUNCTION_INTERFACE(
            GRAPH_FUNCTION_DEPEND_DATA(int32_t, a)
            GRAPH_FUNCTION_EMIT_DATA(int32_t, x)
        );
    } f3;
    auto& v1 = builder.add_vertex(f1);
    v1.named_depend("a").to("X1");
    v1.named_emit("x").to("X2");
    auto& v2 = builder.add_vertex(f2);
    v2.named_depend("a").to("X2");
    v2.named_emit("x").to("X3");
    auto& v3 = builder.add_vertex(f3);
    v3.named_depend("a").to("X3");
    v3.named_emit("x").to("X4");
    ASSERT_EQ(0, builder.finish());
    graph = builder.build();
    ASSERT_TRUE((bool)graph);
    auto* x1 = graph->find_data("X1");
    auto* x4 = graph->find_data("X4");
    *x1->emit<int32_t>() = 1;
    auto closure = graph->run(x4);
    f2.fe.get();
    ASSERT_FALSE(closure.finished());
    f2.p.set_value();
    ASSERT_EQ(0, closure.get());
}
