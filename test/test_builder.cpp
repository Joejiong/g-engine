#include <inttypes.h>
#include <gtest/gtest.h>
#include <base/logging.h>
#include <joewu/graph/engine/builder.h>
#include <joewu/graph/engine/executor.h>
#include <joewu/graph/engine/vertex.h>
#include <joewu/graph/engine/graph.h>
#include <joewu/graph/engine/data.h>

using joewu::feed::graph::BthreadGraphExecutor;
using joewu::feed::graph::GraphDependencyBuilder;
using joewu::feed::graph::GraphVertexBuilder;
using joewu::feed::graph::GraphBuilder;
using joewu::feed::graph::GraphDependency;
using joewu::feed::graph::GraphProcessor;
using joewu::feed::graph::GraphVertex;
using joewu::feed::graph::GraphData;
using joewu::feed::mlarch::babylon::ApplicationContext;
using joewu::feed::mlarch::babylon::DefaultComponentHolder;
using joewu::feed::mlarch::babylon::DefaultFactoryComponentHolder;

class OneProcessor : public GraphProcessor {
    virtual int32_t process(GraphVertex&) noexcept override {
        return 0;
    }
};

BthreadGraphExecutor executor;

TEST(builder, empty_graph_is_ok_though_useless) {
    GraphBuilder builder;
    builder.executor(executor);
    ASSERT_EQ(0, builder.finish());
    ASSERT_NE(nullptr, builder.build().get());
}

TEST(builder, executor_set_in_graph_shared_by_vertex) {
    OneProcessor processor;
    GraphBuilder builder;
    builder.executor(executor).add_vertex(processor);

    ASSERT_EQ(0, builder.finish());
    auto graph = builder.build();
    ASSERT_EQ(1, graph->vertexes().size());
    ASSERT_EQ(&executor, graph->vertexes()[0].executor());
    ASSERT_EQ(&processor, graph->vertexes()[0].processor());
}

TEST(builder, vertex_builder_reference_keep_valid_after_add_vertex) {
    OneProcessor processor;
    GraphBuilder builder;
    auto& v0 = builder.executor(executor).add_vertex(processor);
    auto& v1 = builder.executor(executor).add_vertex(processor);
    auto& v2 = builder.executor(executor).add_vertex(processor);
    auto& v3 = builder.executor(executor).add_vertex(processor);

    auto iter = builder.vertexes().begin();
    
    ASSERT_EQ(&*iter++, &v0);
    ASSERT_EQ(&*iter++, &v1);
    ASSERT_EQ(&*iter++, &v2);
    ASSERT_EQ(&*iter++, &v3);
}

TEST(builder, processor_can_wireup_from_application_context) {
    ApplicationContext context;
    context.register_component(DefaultComponentHolder<OneProcessor, GraphProcessor>(), "sp");
    context.register_component(DefaultFactoryComponentHolder<OneProcessor, GraphProcessor>(), "fp");
    ASSERT_EQ(0, context.initialize());

    GraphBuilder builder;
    builder.application_context(context).executor(executor);
    builder.add_vertex("sp");
    builder.add_vertex("sp");
    builder.add_vertex("fp");
    builder.add_vertex("fp");
    ASSERT_EQ(0, builder.finish());

    auto graph = builder.build();
    ASSERT_TRUE((bool)graph);
    ASSERT_EQ(4, graph->vertexes().size());
    ASSERT_NE(nullptr, graph->vertexes()[0].processor());
    ASSERT_NE(nullptr, graph->vertexes()[1].processor());
    ASSERT_NE(nullptr, graph->vertexes()[2].processor());
    ASSERT_NE(nullptr, graph->vertexes()[3].processor());
    ASSERT_EQ(graph->vertexes()[0].processor(), graph->vertexes()[1].processor());
    ASSERT_NE(graph->vertexes()[2].processor(), graph->vertexes()[3].processor());

    auto another_graph = builder.build();
    ASSERT_TRUE((bool)another_graph);
    ASSERT_EQ(4, another_graph->vertexes().size());
    ASSERT_NE(nullptr, another_graph->vertexes()[0].processor());
    ASSERT_NE(nullptr, another_graph->vertexes()[1].processor());
    ASSERT_NE(nullptr, another_graph->vertexes()[2].processor());
    ASSERT_NE(nullptr, another_graph->vertexes()[3].processor());
    ASSERT_EQ(graph->vertexes()[0].processor(), another_graph->vertexes()[0].processor());
    ASSERT_EQ(graph->vertexes()[1].processor(), another_graph->vertexes()[1].processor());
    ASSERT_NE(graph->vertexes()[2].processor(), another_graph->vertexes()[2].processor());
    ASSERT_NE(graph->vertexes()[3].processor(), another_graph->vertexes()[3].processor());
}

TEST(builder, non_exist_graph_processor_component_report_fail_when_build) {
    ApplicationContext context;
    ASSERT_EQ(0, context.initialize());

    GraphBuilder builder;
    builder.application_context(context).executor(executor);
    builder.add_vertex("sp");
    ASSERT_EQ(0, builder.finish());

    auto graph = builder.build();
    ASSERT_FALSE((bool)graph);
}
