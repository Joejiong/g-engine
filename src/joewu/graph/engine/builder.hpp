#ifndef joewu_HAOKAN_REC_GRAPH_ENGINE_GRAPH_BUILDER_HPP
#define joewu_HAOKAN_REC_GRAPH_ENGINE_GRAPH_BUILDER_HPP 

#include <joewu/graph/engine/builder.h>
#include <joewu/graph/engine/graph.h>

namespace joewu {
namespace feed {
namespace graph {

///////////////////////////////////////////////////////////////////////////////
// GraphBuilder begin
inline GraphBuilder::GraphBuilder() noexcept : _executor(&BthreadGraphExecutor::instance()) {}

inline GraphBuilder& GraphBuilder::name(StringView name) noexcept {
    _name = name;
    return *this;
}

inline const ::std::string& GraphBuilder::name() const noexcept {
    return _name;
}

inline GraphBuilder& GraphBuilder::application_context(ApplicationContext& context) noexcept {
    _application_context = &context;;
    return *this;
}

inline ApplicationContext& GraphBuilder::application_context() const noexcept {
    return *_application_context;
}

inline GraphBuilder& GraphBuilder::executor(GraphExecutor& executor) noexcept {
    _executor = &executor;
    return *this;
}

inline GraphVertexBuilder& GraphBuilder::add_vertex(GraphProcessor& processor) noexcept {
    _vertexes.emplace_back(*this, _vertexes.size());
    _vertexes.back().processor(processor);
    return _vertexes.back();
}

inline GraphVertexBuilder& GraphBuilder::add_vertex(StringView processor_name) noexcept {
    _vertexes.emplace_back(*this, _vertexes.size());
    _vertexes.back().name(processor_name);
    _vertexes.back().processor_name(processor_name);
    return _vertexes.back();
}

inline const ::std::list<GraphVertexBuilder>& GraphBuilder::vertexes() const noexcept {
    return _vertexes;
}

inline ::std::ostream& operator<<(::std::ostream& os, const GraphBuilder& builder) noexcept {
    os << "graph[";
    if (!builder.name().empty()) {
        os << builder.name();
    } else {
        os << &builder;
    }
    return os << "]";
}
// GraphBuilder end
///////////////////////////////////////////////////////////////////////////////

///////////////////////////////////////////////////////////////////////////////
// GraphVertexBuilder begin
inline GraphVertexBuilder::GraphVertexBuilder(const GraphBuilder& builder, size_t index) noexcept :
    _builder(&builder), _index(index) {}

size_t GraphVertexBuilder::index() const noexcept {
    return _index;
}

inline GraphVertexBuilder& GraphVertexBuilder::name(StringView name) noexcept {
    _name = name;
    return *this;
}

inline const std::string& GraphVertexBuilder::name() const noexcept {
    return _name;
}

inline GraphVertexBuilder& GraphVertexBuilder::processor(GraphProcessor& processor) noexcept {
    _processor = &processor;
    return *this;
}

inline GraphVertexBuilder& GraphVertexBuilder::processor_name(StringView processor_name) noexcept {
    _processor_name = processor_name;
    return *this;
}

GraphDependencyBuilder& GraphVertexBuilder::named_depend(
    const ::std::string& depend_name) noexcept {
    auto index = _named_dependencies.size();
    auto result = _dependency_index_by_name.emplace(
        ::std::make_pair(depend_name, index));
    if (result.second) {
        _named_dependencies.emplace_back(*this, depend_name);
    }
    return _named_dependencies[result.first->second];
}

GraphDependencyBuilder& GraphVertexBuilder::anonymous_depend() noexcept {
    auto index = _anonymous_dependencies.size();
    _anonymous_dependencies.emplace_back(*this, index);
    return _anonymous_dependencies.back();
}

GraphDependencyBuilder& GraphVertexBuilder::depend(const ::std::string& target_name) noexcept {
    return anonymous_depend().to(target_name);
}

GraphEmitBuilder& GraphVertexBuilder::named_emit(
    const ::std::string& emit_name) noexcept {
    auto index = _named_emits.size();
    auto result = _emit_index_by_name.emplace(
        ::std::make_pair(emit_name, index));
    if (result.second) {
        _named_emits.emplace_back(*this, emit_name);
    }
    return _named_emits[result.first->second];
}

GraphEmitBuilder& GraphVertexBuilder::anonymous_emit() noexcept {
    auto index = _anonymous_emits.size();
    _anonymous_emits.emplace_back(*this, index);
    return _anonymous_emits.back();
}

GraphVertexBuilder& GraphVertexBuilder::emit(const ::std::string& target_name) noexcept {
    anonymous_emit().to(target_name);
    return *this;
}

template <typename T>
GraphVertexBuilder& GraphVertexBuilder::option(T&& option) noexcept {
    _option = ::std::move(option);
    return *this;
}

template <>
inline const Any* GraphVertexBuilder::option<Any>() const noexcept {
    return &_option;
}

template <typename T>
const T* GraphVertexBuilder::option() const noexcept {
    return _option.get<T>();
}

GraphDependency* GraphVertexBuilder::named_dependency(const ::std::string& name,
    ::std::vector<GraphDependency>& dependencies) const noexcept {
    auto it = _dependency_index_by_name.find(name);
    if (likely(it != _dependency_index_by_name.end())) {
        return &dependencies[it->second];
    }
    return nullptr;
}

GraphDependency* GraphVertexBuilder::anonymous_dependency(size_t index,
    ::std::vector<GraphDependency>& dependencies) const noexcept {
    auto real_index = _named_dependencies.size() + index;
    if (likely(real_index < dependencies.size())) {
        return &dependencies[real_index];
    }
    return nullptr;
}

size_t GraphVertexBuilder::anonymous_dependency_size() const noexcept {
    return _anonymous_dependencies.size();
}

GraphData* GraphVertexBuilder::named_emit(const ::std::string& name,
    ::std::vector<GraphData*>& emits) const noexcept {
    auto it = _emit_index_by_name.find(name);
    if (likely(it != _emit_index_by_name.end())) {
        return emits[it->second];
    }
    return nullptr;
}

GraphData* GraphVertexBuilder::anonymous_emit(size_t index,
    ::std::vector<GraphData*>& data) const noexcept {
    auto real_index = _named_emits.size() + index;
    if (likely(real_index < data.size())) {
        return data[real_index];
    }
    return nullptr;
}

size_t GraphVertexBuilder::anonymous_emit_size() const noexcept {
    return _anonymous_emits.size();
}

const ::std::vector<GraphDependencyBuilder>& GraphVertexBuilder::named_dependencies() const noexcept {
    return _named_dependencies;
}

const ::std::vector<GraphDependencyBuilder>& GraphVertexBuilder::anonymous_dependencies() const noexcept {
    return _anonymous_dependencies;
}

const ::std::vector<GraphEmitBuilder>& GraphVertexBuilder::named_emits() const noexcept {
    return _named_emits;
}

const ::std::vector<GraphEmitBuilder>& GraphVertexBuilder::anonymous_emits() const noexcept {
    return _anonymous_emits;
}

inline ::std::ostream& operator<<(::std::ostream& os, const GraphVertexBuilder& vertex) noexcept {
    os << "vertex[" << vertex.name() << "][" << vertex.index() << "]";
    return os;
}
// GraphVertexBuilder end
///////////////////////////////////////////////////////////////////////////////


///////////////////////////////////////////////////////////////////////////////
// GraphEmitBuilder begin
GraphEmitBuilder::GraphEmitBuilder(GraphVertexBuilder& source,
    const ::std::string& emit_name) noexcept :
    _source(&source), _name(emit_name) {}

GraphEmitBuilder::GraphEmitBuilder(GraphVertexBuilder& source,
    size_t emit_index) noexcept :
    _source(&source), _index(emit_index) {}

const ::std::string& GraphEmitBuilder::name() const noexcept {
    return _name;
}

size_t GraphEmitBuilder::index() const noexcept {
    return _index;
}

GraphEmitBuilder& GraphEmitBuilder::to(const ::std::string& target) noexcept {
    _target = target;
    return *this;
}

inline GraphEmitBuilder& GraphEmitBuilder::on_emit(const OnEmitFunction& on_emit) noexcept{
    _on_emit = on_emit;
    return *this;
}

inline const OnEmitFunction& GraphEmitBuilder::on_emit() const noexcept{
    return _on_emit;
}

size_t GraphEmitBuilder::target_index() const noexcept {
    return _target_index;
}

const ::std::string& GraphEmitBuilder::target() const noexcept {
    return _target;
}
// GraphEmitBuilder end
///////////////////////////////////////////////////////////////////////////////

///////////////////////////////////////////////////////////////////////////////
// GraphDependencyBuilder begin
GraphDependencyBuilder::GraphDependencyBuilder(GraphVertexBuilder& source,
    const ::std::string& depend_name) noexcept :
    _source(&source), _name(depend_name) {}

GraphDependencyBuilder::GraphDependencyBuilder(GraphVertexBuilder& source,
    size_t depend_index) noexcept :
    _source(&source), _index(depend_index) {}

const ::std::string& GraphDependencyBuilder::name() const noexcept {
    return _name;
}

size_t GraphDependencyBuilder::index() const noexcept {
    return _index;
}

GraphDependencyBuilder& GraphDependencyBuilder::to(const ::std::string& target) noexcept {
    _target = target;
    return *this;
}

GraphDependencyBuilder& GraphDependencyBuilder::on(const ::std::string& condition) noexcept {
    _condition = condition;
    _establish_value = true;
    return *this;
}

GraphDependencyBuilder& GraphDependencyBuilder::unless(const ::std::string& condition) noexcept {
    _condition = condition;
    _establish_value = false;
    return *this;
}

GraphDependencyBuilder& GraphDependencyBuilder::set_mutable(bool is_mutable) noexcept {
    _mutable = is_mutable;
    return *this;
}

GraphDependencyBuilder& GraphDependencyBuilder::set_essential(bool is_essential) noexcept {
    _essential = is_essential;
    return *this;
}

const ::std::string& GraphDependencyBuilder::target() const noexcept {
    return _target;
}

const ::std::string& GraphDependencyBuilder::condition() const noexcept {
    return _condition;
}
// GraphDependencyBuilder end
///////////////////////////////////////////////////////////////////////////////
    
} // graph
} // feed
} // joewu
#endif //joewu_HAOKAN_REC_GRAPH_ENGINE_GRAPH_BUILDER_HPP
