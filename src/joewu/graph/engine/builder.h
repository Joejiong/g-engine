#ifndef joewu_HAOKAN_REC_GRAPH_ENGINE_GRAPH_BUILDER_H 
#define joewu_HAOKAN_REC_GRAPH_ENGINE_GRAPH_BUILDER_H   

#include <joewu/graph/engine/on_emit.h>

#include <joewu/feed/mlarch/babylon/any.h>
#include <joewu/feed/mlarch/babylon/application_context.h>

#include <list>
#include <memory>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace joewu {
namespace feed {
namespace graph {
using ::joewu::feed::mlarch::babylon::Any;
using ::joewu::feed::mlarch::babylon::ApplicationContext;

class Graph;
class GraphExecutor;
class GraphProcessor;
class GraphVertexBuilder;
class GraphBuilder {
    using StringView = ::joewu::feed::mlarch::babylon::StringView;
public:
    inline GraphBuilder() noexcept;
    // 设置名字，用于日志打印
    inline GraphBuilder& name(StringView name) noexcept;
    inline const ::std::string& name() const noexcept;
    // 设置上下文，用于GraphProcessor组件组装
    inline GraphBuilder& application_context(ApplicationContext& context) noexcept;
    inline ApplicationContext& application_context() const noexcept;
    // 设置executor，用于支持实际图运行
    inline GraphBuilder& executor(GraphExecutor& executor) noexcept;
    // 加入一个processor，返回GraphVertexBuilder进行进一步依赖设置
    // processor支持直接设置实例或者使用名字从context中组装实例
    inline GraphVertexBuilder& add_vertex(GraphProcessor& processor) noexcept;
    inline GraphVertexBuilder& add_vertex(StringView processor_name) noexcept;
    // 遍历所有节点，做一些额外的操作
    // 比如提取其中的表达式依赖，进一步处理
    inline const ::std::list<GraphVertexBuilder>& vertexes() const noexcept;
    // 完成构建，检测整体正确性
    // 并将各个builder的string描述转为序号加速
    int32_t finish() noexcept;
    // 之后可以反复通过build获取Graph实例
    ::std::unique_ptr<Graph> build() const noexcept;

private:
    // 描述
    ::std::string _name;
    GraphExecutor* _executor;
    ::std::list<GraphVertexBuilder> _vertexes;
    ApplicationContext* _application_context = &ApplicationContext::instance();
    // 符号表
    ::std::unordered_map<::std::string, size_t> _data_index_by_name;
    ::std::unordered_map<size_t, const GraphVertexBuilder*> _producer_by_data_index;
};

class GraphData;
class GraphVertex;
class GraphDependency;
class GraphEmitBuilder;
class GraphDependencyBuilder;
class GraphVertexBuilder {
    using StringView = ::joewu::feed::mlarch::babylon::StringView;
    template <typename T>
    using ScopedComponent = ::joewu::feed::mlarch::babylon::ApplicationContext::ScopedComponent<T>;
public:
    inline GraphVertexBuilder(GraphVertexBuilder&&) noexcept = default;
    inline GraphVertexBuilder(const GraphBuilder& builder, size_t index) noexcept;
    inline size_t index() const noexcept;
	// 设置算子名称，用于日志打印
    inline GraphVertexBuilder& name(StringView name) noexcept;
	inline const std::string& name() const noexcept;
    // 设置processor实例
    inline GraphVertexBuilder& processor(GraphProcessor& processor) noexcept;
    // 设置processor name用来从context中组装组件实例
    inline GraphVertexBuilder& processor_name(StringView processor_name) noexcept;
    // 添加一个匿名依赖
    // 返回GraphDependencyBuilder做进一步操作 
    inline GraphDependencyBuilder& anonymous_depend() noexcept;
    // 添加一个名为depend_name的命名依赖
    // 返回GraphDependencyBuilder做进一步操作 
    inline GraphDependencyBuilder& named_depend(const ::std::string& depend_name) noexcept;
    // 添加一个匿名输出
    // 返回GraphEmitBuilder做进一步操作 
    inline GraphEmitBuilder& anonymous_emit() noexcept;
    // 添加一个名为emit_name的命名输出
    // 返回GraphEmitBuilder做进一步操作 
    inline GraphEmitBuilder& named_emit(const ::std::string& emit_name) noexcept;
    // 设置option，用于定制算子在vertex的行为
    // 最终被算子在setup阶段使用
    template <typename T>
    inline GraphVertexBuilder& option(T&& option) noexcept;
    template <typename T>
    inline const T* option() const noexcept;
    // 完成构建，传入data编号用于加速访问
    int32_t finish(::std::unordered_map<::std::string, size_t>& data_index_by_name,
        ::std::unordered_map<size_t, const GraphVertexBuilder*>& producer_by_data_index) noexcept;
    // 构造一个vertex，设定executor，并根据序号绑定上下游的data
    // 传入data是一个全集，内部依赖finish时固化的index按需获取
    int32_t build(GraphExecutor& executor,
        GraphVertex& vertex, ::std::vector<GraphData>& data) const noexcept;

    inline const ::std::vector<GraphDependencyBuilder>& named_dependencies() const noexcept;
    inline const ::std::vector<GraphDependencyBuilder>& anonymous_dependencies() const noexcept;
    inline const ::std::vector<GraphEmitBuilder>& named_emits() const noexcept;
    inline const ::std::vector<GraphEmitBuilder>& anonymous_emits() const noexcept;

    // 不再建议使用，改为anonymous_depend
    inline __attribute__((deprecated)) GraphDependencyBuilder& depend(
        const ::std::string& target_name) noexcept;
    // 不再建议使用，改为anonymous_emit
    inline __attribute__((deprecated)) GraphVertexBuilder& emit(
        const ::std::string& target_name) noexcept;

private:
    inline GraphDependency* named_dependency(const ::std::string& name,
        ::std::vector<GraphDependency>& dependencies) const noexcept;
    inline GraphDependency* anonymous_dependency(size_t index,
        ::std::vector<GraphDependency>& dependencies) const noexcept;
    inline size_t anonymous_dependency_size() const noexcept;
    inline GraphData* named_emit(const ::std::string& name,
        ::std::vector<GraphData*>& data) const noexcept;
    inline GraphData* anonymous_emit(size_t index,
        ::std::vector<GraphData*>& data) const noexcept;
    inline size_t anonymous_emit_size() const noexcept;

    // 描述
    const GraphBuilder* const _builder;
    const size_t _index;
	::std::string _name;
	::std::string _processor_name; 
    GraphProcessor* _processor = nullptr;
    Any _option;
    
    ::std::function<ScopedComponent<GraphProcessor>()> _processor_creator;
    ::std::unordered_map<::std::string, size_t> _dependency_index_by_name;
    ::std::vector<GraphDependencyBuilder> _named_dependencies;
    ::std::vector<GraphDependencyBuilder> _anonymous_dependencies;
    ::std::unordered_map<::std::string, size_t> _emit_index_by_name;
    ::std::vector<GraphEmitBuilder> _named_emits;
    ::std::vector<GraphEmitBuilder> _anonymous_emits;

    friend class GraphVertex;
    friend class GraphBuilder;
};

class GraphEmitBuilder {
public:
    // 不会被单独构造，仅供GraphVertexBuilder中的vector容器使用
    // 构造属于source的命名输出，输出名为emit_name
    inline GraphEmitBuilder(GraphVertexBuilder& source,
        const ::std::string& emit_name) noexcept;
    // 不会被单独构造，仅供GraphVertexBuilder中的vector容器使用
    // 构造属于source的匿名输出，序号为emit_index
    inline GraphEmitBuilder(GraphVertexBuilder& source,
        size_t emit_index) noexcept;
    // 不会被单独构造，仅供GraphVertexBuilder中的vector容器使用
    // 构造属于source匿名输出
    inline GraphEmitBuilder(GraphVertexBuilder& source) noexcept;
    // 输出到名为target的GraphData
    inline GraphEmitBuilder& to(const ::std::string& target) noexcept;
    
    //GraphData发布前调用，构造图的时候传入
    inline GraphEmitBuilder& on_emit(const OnEmitFunction& on_emit) noexcept;
    inline const OnEmitFunction& on_emit() const noexcept;
    // 获取输出目标GraphData的名字
    inline const ::std::string& target() const noexcept;
    // 完成构建，传入data编号用于加速访问
    int32_t finish(::std::unordered_map<::std::string, size_t>& data_index_by_name,
        ::std::unordered_map<size_t, const GraphVertexBuilder*>& producer_by_data_index) noexcept;

    inline const ::std::string& name() const noexcept;
    inline size_t index() const noexcept;
    inline size_t target_index() const noexcept;
private:
    // 描述
    const GraphVertexBuilder* _source;
    const ::std::string _name;
    const size_t _index {0};
    ::std::string _target;
    bool _mutable {false};

    // 序号表
    size_t _target_index {0};

    OnEmitFunction _on_emit;
};

class GraphDependency;
class GraphDependencyBuilder {
public:
    // 不会被单独构造，仅供GraphVertexBuilder中的vector容器使用
    // 构造属于source的命名依赖，依赖名为depend_name
    inline GraphDependencyBuilder(GraphVertexBuilder& source,
        const ::std::string& depend_name) noexcept;
    // 不会被单独构造，仅供GraphVertexBuilder中的vector容器使用
    // 构造属于source的匿名依赖，序号为depend_name
    inline GraphDependencyBuilder(GraphVertexBuilder& source, size_t depend_index) noexcept;
    // 依赖名为target的GraphData
    inline GraphDependencyBuilder& to(const ::std::string& target) noexcept;
    // 当名为condition的GraphData为true时成立
    inline GraphDependencyBuilder& on(const ::std::string& condition) noexcept;
    // 当名为condition的GraphData为false时成立
    inline GraphDependencyBuilder& unless(const ::std::string& condition) noexcept;
    // 设置是否可修改，串行处理同一对象可以设置为可修改
    // 利用Any的引用特性，可以在不拷贝的情况下，将修改统一为新增
    // 保持GraphData的不可变性
    inline GraphDependencyBuilder& set_mutable(bool is_mutable = true) noexcept;
    // 设置是否是强依赖
    inline GraphDependencyBuilder& set_essential(bool is_essential = true) noexcept;
    // 完成构建，传入data编号用于加速访问
    int32_t finish(::std::unordered_map<::std::string, size_t>& data_index_by_name) noexcept;
    // 构造一个dependency，设定所属vertex，并根据序号绑定data
    // 传入data是一个全集，内部依赖finish时固化的index按需获取
    void build(GraphDependency& dependency, GraphVertex& vertex,
        ::std::vector<GraphData>& data) const noexcept;

    inline const ::std::string& name() const noexcept;
    inline size_t index() const noexcept;
    // 供GraphBuilder使用，用来计算data总量
    // 也开放外围直接访问，用于表达式编织等操作中获取信息
    inline const ::std::string& target() const noexcept;
    inline const ::std::string& condition() const noexcept;

private:
    // 描述
    const GraphVertexBuilder* _source;
    const ::std::string _name;
    const size_t _index {0};
    ::std::string _target;
    ::std::string _condition;
    bool _mutable {false};
    bool _establish_value {false};
    bool _essential {false};

    // 序号表
    size_t _target_index {0};
    size_t _condition_index {0};

    friend class GraphBuilder;
};

} // graph
} // feed
} // joewu
#endif //joewu_HAOKAN_REC_GRAPH_ENGINE_GRAPH_BUILDER_H

#include <joewu/graph/engine/builder.hpp>
