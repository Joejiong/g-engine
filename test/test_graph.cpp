#include <future>
#include <inttypes.h>
#include <sstream>
#include <gflags/gflags.h>
#include <gtest/gtest.h>
#include <base/logging.h>
#include <joewu/graph/engine/graph.h>
#include <joewu/graph/engine/data.h>
#include <joewu/graph/engine/vertex.h>
#include <joewu/graph/engine/executor.h>
#include <joewu/graph/engine/builder.h>
#include <joewu/graph/engine/closure.h>
#include <base/string_printf.h>
#include <joewu/feed/mlarch/babylon/any.h>

using joewu::feed::mlarch::babylon::Any;
using joewu::feed::graph::GraphBuilder;
using joewu::feed::graph::GraphProcessor;
using joewu::feed::graph::GraphVertex;
using joewu::feed::graph::Commiter;
using joewu::feed::graph::GraphData;
using joewu::feed::graph::GraphLog;
using joewu::feed::graph::Graph;
using joewu::feed::graph::Closure;
using joewu::feed::graph::BthreadGraphExecutor;

struct GraphMutableContext {
    uint32_t count;
};

class OneProcessor : public GraphProcessor {
    virtual int32_t process(GraphVertex& vertex) noexcept override {
        LOG(NOTICE) << "run process with vertex[" << vertex.index() << "]"
                    << " vertex_name:" << vertex.name();
        //auto data = vertex.emit(0);
        int err = 0;
        for (auto& dep : vertex.dependencies()) {
            std::string vertex_name;
            dep.activated_vertex_name(vertex_name);
            LOG(NOTICE) << "OneProcessor activated vertex name=" << vertex_name
            << " err=" << err;;
        }
        auto data = vertex.anonymous_emit(0);
        std::stringstream ss;
        ss << "x from vertex[" << vertex.index() << "]";
        data->emit<std::string>()->assign(ss.str());
        LOG(TRACE) << "current thread:" << std::this_thread::get_id();
        std::this_thread::sleep_for(std::chrono::microseconds(10000));
        auto graph_mutable_context_p = vertex.get_graph_mutable_context<GraphMutableContext>();
        graph_mutable_context_p->count++;
        std::string format = "(%s,%s,%d,%d,%d)";
        base::string_appendf(&vertex.log(), format.c_str(), "process", "parse", 0, 200, 10);
        base::string_appendf(&vertex.log(), format.c_str(), "rpc", "grc", 0, 200, 10);
        return 0;
    }
};



class ConstProcessor : public GraphProcessor {
    virtual int32_t process(GraphVertex& vertex) noexcept override {
        LOG(NOTICE) << "emit const value";
        int err = 0;
        for (auto& dep : vertex.dependencies()) {
            std::string vertex_name;
            dep.activated_vertex_name(vertex_name);
            LOG(NOTICE) << "activated vertex name=" << vertex_name
            << " err=" << err;;
        }
        *vertex.anonymous_emit(0)->emit<int32_t>() = 10086;
        return 0;
    }
};

class EmptyProcessor : public GraphProcessor {
    virtual int32_t process(GraphVertex& vertex) noexcept override {
        int err = 0;
        for (auto& dep : vertex.dependencies()) {
            std::string name;
            dep.activated_vertex_name(name);
            LOG(NOTICE) << "activated vertex name=" << name << " err=" << err;;
        }
        LOG(NOTICE) << "enter but no action";
        return 0;
    }
};


class SessionContex {
    int32_t value{0};
};

class OnEmitProcessor : public GraphProcessor {
    virtual int32_t process(GraphVertex& vertex) noexcept override {
        LOG(NOTICE) << "emit data with on_emit";
        auto data = vertex.anonymous_emit(0);
        *(data->emit<int32_t>()) = 10086;
        return 0;
    }
};

TEST(graph, log_unfinished_data_when_end_without_target_ready) {
    ConstProcessor const_processor;
    EmptyProcessor empty_processor;
    GraphBuilder builder;
    BthreadGraphExecutor executor;
    builder.executor(executor);
    {
        auto& v = builder.add_vertex(const_processor);
        v.anonymous_emit().to("A");
        v.anonymous_depend().to("B");
        v.anonymous_depend().to("E");
    }
    {
        auto& v = builder.add_vertex(empty_processor);
        v.anonymous_emit().to("B");
        v.anonymous_depend().to("C");
    }
    {
        auto& v = builder.add_vertex(empty_processor);
        v.anonymous_emit().to("E");
        v.anonymous_depend().to("B");
    }
    {
        auto& v = builder.add_vertex(const_processor);
        v.anonymous_emit().to("C");
        v.anonymous_depend().to("D");
    }
    builder.finish();
    auto graph = builder.build();
    auto a = graph->find_data("A");
    auto d = graph->find_data("D");
    *(d->emit<bool>()) = true;
    int32_t error_code = graph->run(a).get();
    LOG(NOTICE) << "code: " << error_code;
}

TEST(graph, hellworld) {
    OneProcessor processor;
    GraphBuilder builder;
    BthreadGraphExecutor executor;
    builder.executor(executor);
    {
        auto& v = builder.add_vertex(processor);
        //v.emit("A");
        v.anonymous_emit().to("A");
        //v.depend("B");
        v.anonymous_depend().to("B");
        v.name("one_vertex");
    }
    {
        auto& v = builder.add_vertex(processor);
        //v.emit("B");
        v.anonymous_emit().to("B");
        //v.depend("C").on("D");
        v.anonymous_depend().to("C").on("D");
		std::string tmp_vertex_name = "two_vertex";
        v.name(std::move(tmp_vertex_name));
    }
    {
        auto& v = builder.add_vertex(processor);
        //v.emit("C");
        v.anonymous_emit().to("C");
        //v.depend("E");
        v.anonymous_depend().to("E");
        v.name("three_vertex");
    }
    builder.finish();
    auto graph = builder.build();
    auto mutable_graph_context = graph->mutable_context<GraphMutableContext>();
    mutable_graph_context->count = 100;
    auto a = graph->find_data("A");
    auto b = graph->find_data("B");
    //auto c = graph->find_data("C");
    auto d = graph->find_data("D");
    auto e = graph->find_data("E");
    *(d->emit<bool>()) = true;
    LOG(NOTICE) << "emit data D";
    *(e->emit<::std::string>()) = "eeeee";
    LOG(NOTICE) << "emit data E";
    int32_t error_code = graph->run(a, b).get();
    mutable_graph_context = graph->mutable_context<GraphMutableContext>();
    LOG(NOTICE) << "mutable_graph_context count val=" << mutable_graph_context->count;
    ASSERT_EQ(mutable_graph_context->count, 103);
    LOG(NOTICE) << "code: " << error_code;
    LOG(NOTICE) << "graph_logs: " <<  GraphLog(graph.get());
}

TEST(graph, on_emit) {
    OnEmitProcessor processor;
    GraphBuilder builder;
    BthreadGraphExecutor executor;
    builder.executor(executor);
    {
        auto& v = builder.add_vertex(processor);
        //v.emit("A");
        v.anonymous_emit().to("A");
        //v.depend("B");
        v.anonymous_depend().to("B");
    }
    {
        auto& v = builder.add_vertex(processor);
        //v.emit("E");
        v.anonymous_emit().to("B").on_emit([](GraphVertex& vertex, const Any& data){
            auto mutable_context_p = vertex.get_graph_mutable_context<SessionContex>();
            LOG(NOTICE) << "release data B on_emit, data value: " << *(data.get<int32_t>());
            mutable_context_p->value = *(data.get<int32_t>());
        });
        //v.depend("F");
        v.anonymous_depend().to("C");
    }
    builder.finish();
    auto graph = builder.build();
    auto mutable_graph_context = graph->mutable_context<SessionContex>();
    auto a = graph->find_data("A");
    auto c = graph->find_data("C");
    *(c->emit<bool>()) = true;
    LOG(NOTICE) << "emit data C";
    graph->run(a);
    ASSERT_EQ(mutable_graph_context->value, 10086);
}

TEST(graph, async) {
    OneProcessor processor;
    GraphBuilder builder;
    BthreadGraphExecutor executor;
    builder.executor(executor);
    {
        auto& v = builder.add_vertex(processor);
        //v.emit("A");
        v.anonymous_emit().to("A");
        //v.depend("B");
        v.anonymous_depend().to("B");
    }
    {
        auto& v = builder.add_vertex(processor);
        //v.emit("B");
        v.anonymous_emit().to("B");
        //v.depend("C").on("D");
        v.anonymous_depend().to("C").on("D");
    }
    {
        auto& v = builder.add_vertex(processor);
        //v.emit("C");
        v.anonymous_emit().to("C");
        //v.depend("E");
        v.anonymous_depend().to("E");
    }
    builder.finish();
    auto graph = builder.build();
    auto a = graph->find_data("A");
    auto b = graph->find_data("B");
    //auto c = graph->find_data("C");
    auto d = graph->find_data("D");
    auto e = graph->find_data("E");
    *(d->emit<bool>()) = true;
    LOG(NOTICE) << "emit data D";
    *(e->emit<::std::string>()) = "eeeee";
    LOG(NOTICE) << "emit data E";

    ::std::promise<void> promise;
    auto future = promise.get_future();
    graph->run(a, b).on_finish(::std::bind([] (::std::promise<void>& promise, Closure&& closure) {
            int error_code = closure.error_code();
            LOG(NOTICE) << "code: " << error_code;
            
            closure.wait();
            promise.set_value();
    }, ::std::move(promise), ::std::placeholders::_1));
    future.get();
    LOG(NOTICE) << "end";
}

#ifdef GOOGLE_PROTOBUF_HAS_ARENAS
TEST(graph, support_reuse_obj_function) {
    OneProcessor processor;
    GraphBuilder builder;
    BthreadGraphExecutor executor;
    builder.executor(executor);
    {
        auto& v = builder.add_vertex(processor);
        //v.emit("A");
        v.anonymous_emit().to("A");
        //v.depend("B");
        v.anonymous_depend().to("B");
    }
    {
        auto& v = builder.add_vertex(processor);
        //v.emit("B");
        v.anonymous_emit().to("B");
        //v.depend("C").on("D");
        v.anonymous_depend().to("C").on("D");
    }
    {
        auto& v = builder.add_vertex(processor);
        //v.emit("C");
        v.anonymous_emit().to("C");
        //v.depend("E");
        v.anonymous_depend().to("E");
    }
    builder.finish();
    auto graph = builder.build();
    auto message = graph->create<std::string>();
    ASSERT_EQ(0, message->capacity());
    *message = "1234567890";
    ASSERT_EQ(10, message->size());
    graph->reset();
    ASSERT_EQ(0, message->size());
    *message = "1234567890";
    ASSERT_EQ(10, message->size());
}
#endif
