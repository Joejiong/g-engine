#ifndef joewu_HAOKAN_REC_GRAPH_ENGINE_GRAPH_VERTEX_H 
#define joewu_HAOKAN_REC_GRAPH_ENGINE_GRAPH_VERTEX_H  

#include <joewu/feed/mlarch/babylon/any.h>
#include <joewu/feed/mlarch/babylon/application_context.h>
#include <joewu/feed/mlarch/babylon/stack.h>
#include <joewu/feed/mlarch/babylon/reusable/manager.h>

#include <boost/preprocessor/cat.hpp>
#include <boost/preprocessor/control/if.hpp>
#include <boost/preprocessor/comparison/equal.hpp>
#include <boost/preprocessor/comparison/less.hpp>
#include <boost/preprocessor/comparison/greater.hpp>
#include <boost/preprocessor/punctuation/remove_parens.hpp>
#include <boost/preprocessor/seq/for_each.hpp>
#include <boost/preprocessor/tuple/push_back.hpp>
#include <boost/preprocessor/tuple/push_front.hpp>
#include <boost/preprocessor/tuple/size.hpp>

namespace joewu {
namespace feed {
namespace graph {
using ::joewu::feed::mlarch::babylon::Any;
using ::joewu::feed::mlarch::babylon::Stack;
using ::joewu::feed::mlarch::babylon::ReusableAccessor;
using ::joewu::feed::mlarch::babylon::ReusableManager;
using ::joewu::feed::mlarch::babylon::StaticMemoryPool;
// 算子基类，无状态
// 所有交互数据在GraphVertex中记录
class GraphVertex;
class ClosureContext;
class GraphVertexClosure;
class GraphProcessor {
public:
    virtual ~GraphProcessor() noexcept;
    // build阶段调用，processor可以根据vertex的option不同，设置不同的运行模式
    // 并记录在vertex的context上，后续process时可以获取
    virtual int32_t setup(GraphVertex&) const noexcept;
    // 节点激活时调用，一般无需实现，目前用于内置算子用于推导是否需要声明可变依赖
    virtual int32_t on_activate(GraphVertex&) const noexcept;
    ///////////////////////////////////////////////////////////////////////////
    // 实际的算子处理函数，分同步和异步版本，process函数需要确保【可重入】
    // 通过vertex按照序号获取依赖，按照序号输出节点
    // 通过返回值!=0或者closure.done(!=0)来通知框架异常，框架会短路终止
    // 预期内可处理的失败，可以置空输出节点，而非通知异常来在表示
    // 
    // 异步版本，用于算子本身需要异步的场景
    // 比如调用只能异步的组件或者第三方库等
    // 只是并发运行无需实现此接口
    virtual void process(GraphVertex& vertex, GraphVertexClosure&& closure) noexcept;
    // 同步版本，通过返回值通知异常
    virtual int32_t process(GraphVertex&) noexcept;
    ///////////////////////////////////////////////////////////////////////////
    virtual void reset(GraphVertex&) const noexcept {}
};

class GraphFunction : public GraphProcessor {
public:
    // build阶段调用，传入GraphVertexBuilder::option中设置的选项，供定制化
    virtual int32_t setup(const Any&) noexcept;
    // run阶段调用，执行实际的计算逻辑
    virtual int32_t operator()() noexcept;
    // reset阶段调用，进行状态重置，以便完成新一轮的run
    virtual void reset() noexcept;

protected:
    using Any = ::joewu::feed::mlarch::babylon::Any;
    inline GraphVertex& vertex() noexcept;

private:
    virtual int32_t setup(GraphVertex& vertex) const noexcept override final;
    virtual int32_t on_activate(GraphVertex& vertex) const noexcept final;
    virtual void process(GraphVertex& vertex, GraphVertexClosure&& closure) noexcept override final;
    virtual int32_t process(GraphVertex& vertex) noexcept override final;
    virtual void reset(GraphVertex&) const noexcept override final;
    // 用于支持GRAPH_FUNCTION_INTERFACE宏生成自动初始化代码
    virtual int32_t __babylon_auto_declare_interface(GraphVertex&) noexcept;
    virtual int32_t __babylon_auto_prepare_input() noexcept;

    GraphVertex* _vertex {nullptr};
};

class GraphVertexClosure {
public:
    // 为算子包装节点闭包，用生命周期记录运行节点数
    inline GraphVertexClosure(ClosureContext& closure, const GraphVertex& vertex) noexcept;
    // 不能复制，否则干扰运行数控制
    inline GraphVertexClosure(const GraphVertexClosure&) = delete;
    // 可以移动，用来进行异步处理
    inline GraphVertexClosure(GraphVertexClosure&& other) noexcept;
    // 可以移动，用来进行异步处理
    inline GraphVertexClosure& operator=(GraphVertexClosure&& other) noexcept;
    // 析构时如果没有显式结束，则默认正常结束
    inline ~GraphVertexClosure() noexcept;
    inline void done(int32_t error_code = 0) noexcept;

private:
    ClosureContext* _closure {nullptr};
    const GraphVertex* _vertex {nullptr};
};

class Graph;
class GraphData;
class GraphExecutor;
class GraphDependency;
class GraphVertexBuilder;
class GraphVertex {
    template <typename T>
    using ScopedComponent = ::joewu::feed::mlarch::babylon::ApplicationContext::ScopedComponent<T>;
public:
    // 不会被单独构造，提供给Graph中的vector容器使用
    inline GraphVertex() = default;
    // 根据name获取命名dependency，最佳实践是在setup中使用 
    // 并将结果保存到context中
    inline GraphDependency* named_dependency(const ::std::string& name) noexcept;
    // 根据序号获取匿名dependency，范围[0, size)
    inline GraphDependency* anonymous_dependency(size_t index) noexcept;
    inline size_t anonymous_dependency_size() const noexcept;
    inline __attribute__((deprecated)) GraphDependency* dependency(size_t index) noexcept;
    inline __attribute__((deprecated)) size_t dependency_size() const noexcept;
    // 根据name获取命名emit的data，最佳实践是在setup中使用 
    // 并将结果保存到context中
    inline GraphData* named_emit(const ::std::string& name) noexcept;
    // 根据序号获取匿名emit的data，范围[0, size)
    inline GraphData* anonymous_emit(size_t index) noexcept;
    inline size_t anonymous_emit_size() const noexcept;
    inline __attribute__((deprecated)) GraphData* emit(size_t index) noexcept;
    inline __attribute__((deprecated)) size_t emit_size() const noexcept;
    // 获取只读的选项配置，最佳实践是在setup中读取
    // 并将解析后的结果保存到context中
    template <typename T>
    inline const T* option() const noexcept;
    inline const std::string& name() const noexcept;
    // 获取节点运行环境，可以在setup时进行设置，并在运行时使用
    // 可以设置vertex特有的运行环境信息，最佳实践是在setup中分配空间
    // 并在process时进行使用，包括不限于
    // 从名称翻译好的name和dependency
    // vertex特有环境配置
    // 计算中间结果缓冲区
    template <typename T, typename ::std::enable_if<!::std::is_move_constructible<T>::value, int32_t>::type = 0>
    inline T* context() noexcept;
    template <typename T, typename ::std::enable_if<::std::is_move_constructible<T>::value, int32_t>::type = 0>
    inline T* context() noexcept;
    // 标记节点运行为平凡操作
    // 平凡操作在invoke时直接运行而非使用executor
    inline void trivial(bool trivial = true) noexcept;

    inline size_t index() const noexcept;

    //补充算子运行期间日志信息，在processor阶段调用
    inline const ::std::string& clog() const noexcept;
    inline ::std::string& log() noexcept;

    template <typename T>
    inline const T* get_graph_context() const noexcept;

    template <typename T>
    inline T* get_graph_mutable_context() noexcept;

    // 运行callback，由executer在异步环境中调用
    // 运行结束后会销毁this和callback
    inline void run(GraphVertexClosure&& closure) noexcept;

    //graph级别内存复用
    #ifdef GOOGLE_PROTOBUF_HAS_ARENAS
    template <typename T, typename... Args>
    inline ReusableAccessor<T> create(Args&&... args);
    #endif // GOOGLE_PROTOBUF_HAS_ARENAS

    //算子级别内存复用
    template <typename T, typename... Args>
    inline ReusableAccessor<T> create_local(Args&&... args);

    inline ReusableManager<StaticMemoryPool>& local_memory_manager() noexcept {
        return _static_mem_manager;
    }
    inline bool activated() const noexcept;
    inline const ::std::vector<GraphDependency>& dependencies() const noexcept;
    #ifdef GOOGLE_PROTOBUF_HAS_ARENAS
    inline ReusableManager<::google::protobuf::Arena>& memory_manager() noexcept;
    #endif //GOOGLE_PROTOBUF_HAS_ARENAS
private:
    // 供builder操作使用
    inline void builder(const GraphVertexBuilder& builder) noexcept;
    inline void executor(GraphExecutor& executor) noexcept;
    inline void index(size_t index) noexcept;
    inline void processor(ScopedComponent<GraphProcessor>&& processor) noexcept;
    inline ::std::vector<GraphDependency>& dependencies() noexcept;
    inline ::std::vector<GraphData*>& emits() noexcept;
    inline int32_t setup() noexcept;
    inline void set_graph(Graph* graph) noexcept;
    // 清理执行状态，但是保留data空间
    inline void reset() noexcept;
    int32_t activate(Stack<GraphData*>& unsolved_data,
        Stack<GraphVertex*>& runnable_vertexes, ClosureContext* closure) noexcept;
    inline bool ready(GraphDependency* denpendency) noexcept;
    inline ClosureContext* closure() noexcept;
    inline void invoke(Stack<GraphVertex*>& runnable_vertexes) noexcept;
    inline Stack<GraphVertex*>* runnable_vertexes() noexcept;
    // 单测使用
    inline const GraphExecutor* executor() const noexcept;
    inline const GraphProcessor* processor() const noexcept;

    static GraphProcessor DEFAULT_EMPTY_PROCESSOR;

    const GraphVertexBuilder* _builder {nullptr};
    size_t _index {0};
    GraphExecutor* _executor {nullptr};
    ScopedComponent<GraphProcessor> _processor {&DEFAULT_EMPTY_PROCESSOR, nullptr};
    ::std::vector<GraphDependency> _dependencies;
    ::std::vector<GraphData*> _emits;
    Any _context;
    bool _trivial {false};

    // 激活标记
    ::std::atomic<bool> _activated {false};
    // 等待计数
    ::std::atomic<int64_t> _waiting_num {0};
    ClosureContext* _closure {nullptr};
    Stack<GraphVertex*>* _runnable_vertexes {nullptr};
    Graph* _graph {nullptr};
    //日志信息
    ::std::string _log;
    //算子级别内存复用manager
    ReusableManager<StaticMemoryPool> _static_mem_manager;
    friend class Graph;
    friend class GraphData;
    friend class GraphExecutor;
    friend class ClosureContext;
    friend class GraphDependency;
    friend class GraphVertexBuilder;
    friend class GraphBuilder;
    friend void* execute_invoke_vertex(void*);
};

} // graph
} // feed
} // joewu

#define GRAPH_FUNCTION_INTERFACE(members) \
    int32_t __babylon_auto_declare_interface(::joewu::feed::graph::GraphVertex& vertex) noexcept override { \
        (void) vertex; \
        BOOST_PP_SEQ_FOR_EACH(__GRAPH_DEFINE, _, members) \
        return 0; \
    } \
    virtual int32_t __babylon_auto_prepare_input() noexcept override { \
        BOOST_PP_SEQ_FOR_EACH(__GRAPH_PREPARE, _, members) \
        return 0; \
    } \
    BOOST_PP_SEQ_FOR_EACH(__GRAPH_TYPEDEF, _, members) \
    BOOST_PP_SEQ_FOR_EACH(__GRAPH_DECLARE, _, members)

#define __GRAPH_TYPEDEF(r, data, args) \
    typedef BOOST_PP_REMOVE_PARENS(BOOST_PP_TUPLE_ELEM(1, args)) \
        __GRAPH_TYPE_FOR_NAME(BOOST_PP_TUPLE_ELEM(2, args));

#define __GRAPH_TYPE_FOR_NAME(name) \
    BOOST_PP_CAT(__hidden_type_for_, name)

#define __GRAPH_DECLARE(r, data, args) \
    BOOST_PP_IF(BOOST_PP_EQUAL(BOOST_PP_TUPLE_ELEM(0, args), 0), \
        __GRAPH_DECLARE_DEPEND args, \
        __GRAPH_DECLARE_EMIT args)

#define __GRAPH_DECLARE_DEPEND(r, type, name, is_channel, is_mutable, ...) \
    BOOST_PP_IF(is_channel, \
        BOOST_PP_IF(is_mutable, \
            ::joewu::feed::graph::MutableInputChannel<__GRAPH_TYPE_FOR_NAME(name)> __hidden_channel_for_##name;, \
            ::joewu::feed::graph::InputChannel<__GRAPH_TYPE_FOR_NAME(name)> __hidden_channel_for_##name;), \
        ::joewu::feed::graph::GraphDependency* __hidden_depend_for_##name;) \
    BOOST_PP_IF(is_channel, \
        BOOST_PP_IF(is_mutable, \
            ::joewu::feed::graph::MutableChannelConsumer<__GRAPH_TYPE_FOR_NAME(name)> name;, \
            ::joewu::feed::graph::ChannelConsumer<__GRAPH_TYPE_FOR_NAME(name)> name;), \
        BOOST_PP_IF(is_mutable, \
            __GRAPH_TYPE_FOR_NAME(name)* name;, \
            const __GRAPH_TYPE_FOR_NAME(name)* name;)) \

#define __GRAPH_DECLARE_EMIT(r, type, name, is_channel, ...) \
    BOOST_PP_IF(is_channel, \
        ::joewu::feed::graph::OutputChannel<__GRAPH_TYPE_FOR_NAME(name)> name;, \
        ::joewu::feed::graph::OutputData<__GRAPH_TYPE_FOR_NAME(name)> name;)

#define __GRAPH_DEFINE(r, data, args) \
    BOOST_PP_IF(BOOST_PP_EQUAL(BOOST_PP_TUPLE_ELEM(0, args), 0), \
        __GRAPH_DEFINE_DEPEND args, \
        __GRAPH_DEFINE_EMIT args)

#define __GRAPH_DEFINE_DEPEND(r, type, name, is_channel, is_mutable, essential_level, ...) \
    { \
        auto* depend = vertex.named_dependency(#name); \
        if (depend == nullptr) { \
            LOG(WARNING) << "no depend bind to " #name " for " << vertex; \
            return -1; \
        } \
        depend->declare_mutable(is_mutable); \
        depend->declare_essential(essential_level == 1); \
        BOOST_PP_IF(is_channel, \
            BOOST_PP_IF(is_mutable, \
                __hidden_channel_for_##name = depend->declare_mutable_channel<__GRAPH_TYPE_FOR_NAME(name)>(), \
                __hidden_channel_for_##name = depend->declare_channel<__GRAPH_TYPE_FOR_NAME(name)>()), \
            depend->declare_type<__hidden_type_for_##name>(); \
            __hidden_depend_for_##name = depend); \
    }

#define __GRAPH_DEFINE_EMIT(r, type, name, is_channel, ...) \
    { \
        auto* data = vertex.named_emit(#name); \
        if (data == nullptr) { \
            LOG(WARNING) << "no data bind to " #name " for " << vertex; \
            return -1; \
        } \
        BOOST_PP_IF(is_channel, \
            name = data->declare_channel<__GRAPH_TYPE_FOR_NAME(name)>(), \
            name = data->declare_type<__GRAPH_TYPE_FOR_NAME(name)>()); \
        if (!name) { \
            return -1; \
        } \
    }

#define __GRAPH_PREPARE(r, data, args) \
    BOOST_PP_IF(BOOST_PP_EQUAL(BOOST_PP_TUPLE_ELEM(0, args), 0), \
        __GRAPH_PREPARE_DEPEND args, \
        )

#define __GRAPH_PREPARE_DEPEND(r, type, name, is_channel, is_mutable, essential_level, ...) \
    BOOST_PP_IF(is_channel, \
        name = __hidden_channel_for_##name.subscribe(), \
        BOOST_PP_IF(is_mutable, \
            name = __hidden_depend_for_##name->mutable_value<__hidden_type_for_##name>(), \
            name = __hidden_depend_for_##name->value<__hidden_type_for_##name>())); \
    BOOST_PP_IF(BOOST_PP_GREATER(essential_level, 0), \
        if (!(bool)name) { \
            return -1; \
        }, \
    ); \

#define GRAPH_FUNCTION_DEPEND_DATA(type, name, ...) GRAPH_FUNCTION_DEPEND((type, name, 0, 0, ##__VA_ARGS__))
#define GRAPH_FUNCTION_DEPEND_MUTABLE_DATA(type, name, ...) GRAPH_FUNCTION_DEPEND((type, name, 0, 1, ##__VA_ARGS__))
#define GRAPH_FUNCTION_DEPEND_CHANNEL(type, name, ...) GRAPH_FUNCTION_DEPEND((type, name, 1, 0, ##__VA_ARGS__))
#define GRAPH_FUNCTION_DEPEND_MUTABLE_CHANNEL(type, name, ...) GRAPH_FUNCTION_DEPEND((type, name, 1, 1, ##__VA_ARGS__))
#define GRAPH_FUNCTION_DEPEND(args) (__GRAPH_FILL_ARGS(BOOST_PP_TUPLE_PUSH_FRONT(args, 0)))

#define GRAPH_FUNCTION_EMIT_DATA(type, name, ...) GRAPH_FUNCTION_EMIT((type, name, 0))
#define GRAPH_FUNCTION_EMIT_CHANNEL(type, name, ...) GRAPH_FUNCTION_EMIT((type, name, 1))
#define GRAPH_FUNCTION_EMIT(args) (__GRAPH_FILL_ARGS(BOOST_PP_TUPLE_PUSH_FRONT(args, 1)))

#define __GRAPH_FILL_ARGS(args) \
    __GRAPH_FILL_ESSENTIAL_LEVEL(\
        __GRAPH_FILL_IS_MUTABLE(\
            __GRAPH_FILL_IS_CHANNEL(args)))

#define __GRAPH_FILL_ESSENTIAL_LEVEL(args) \
    BOOST_PP_IF(BOOST_PP_LESS(BOOST_PP_TUPLE_SIZE(args), 6), \
        BOOST_PP_TUPLE_PUSH_BACK(args, 2), \
        args)

#define __GRAPH_FILL_IS_MUTABLE(args) \
    BOOST_PP_IF(BOOST_PP_LESS(BOOST_PP_TUPLE_SIZE(args), 5), \
        BOOST_PP_TUPLE_PUSH_BACK(args, 0), \
        args)

#define __GRAPH_FILL_IS_CHANNEL(args) \
    BOOST_PP_IF(BOOST_PP_LESS(BOOST_PP_TUPLE_SIZE(args), 4), \
        BOOST_PP_TUPLE_PUSH_BACK(args, 0), \
        args)

#endif //joewu_HAOKAN_REC_GRAPH_ENGINE_GRAPH_VERTEX_H

#include <joewu/graph/engine/vertex.hpp>
