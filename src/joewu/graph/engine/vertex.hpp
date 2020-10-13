#ifndef joewu_HAOKAN_REC_GRAPH_ENGINE_GRAPH_VERTEX_HPP
#define joewu_HAOKAN_REC_GRAPH_ENGINE_GRAPH_VERTEX_HPP

#include <joewu/graph/engine/vertex.h>
#include <joewu/graph/engine/builder.h>
#include <joewu/graph/engine/closure.h>
#include <joewu/graph/engine/dependency.h>
#include <joewu/graph/engine/graph.h>
#include <joewu/graph/engine/data.h>

namespace joewu {
namespace feed {
namespace graph {


///////////////////////////////////////////////////////////////////////////////
// GraphVertexClosure begin
GraphVertexClosure::GraphVertexClosure(ClosureContext& closure, const GraphVertex& vertex) noexcept :
    _closure(&closure), _vertex(&vertex) {
    _closure->depend_vertex_add();
}

GraphVertexClosure::GraphVertexClosure(GraphVertexClosure&& other) noexcept :
    _closure(other._closure), _vertex(other._vertex) {
    other._closure = nullptr;
    other._vertex = nullptr;
}

GraphVertexClosure::~GraphVertexClosure() noexcept {
    done();
}

GraphVertexClosure& GraphVertexClosure::operator=(GraphVertexClosure&& other) noexcept {
    ::std::swap(_closure, other._closure);
    ::std::swap(_vertex, other._vertex);
    other.done();
    return *this;
}

void GraphVertexClosure::done(int32_t error_code) noexcept {
    if (_closure != nullptr) {
        if (error_code != 0) {
            LOG(WARNING) << *_vertex << " done with " << error_code;
            _closure->finish(error_code);
        } else {
            LOG(DEBUG) << *_vertex << " done with " << error_code;
        }
        _closure->depend_vertex_sub();
        _closure = nullptr;
        _vertex = nullptr;
    }
}
// GraphVertexClosure end
///////////////////////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////////////////////////
// GraphFunction begin
inline GraphVertex& GraphFunction::vertex() noexcept {
    return *_vertex;
}
// GraphFunction end
////////////////////////////////////////////////////////////////////////////////

///////////////////////////////////////////////////////////////////////////////
// GraphVertex begin
void GraphVertex::builder(const GraphVertexBuilder& builder) noexcept {
    _builder = &builder;
}

void GraphVertex::executor(GraphExecutor& executor) noexcept {
    _executor = &executor;
}

void GraphVertex::index(size_t index) noexcept {
    _index = index;
}

size_t GraphVertex::index() const noexcept {
    return _index;
}

inline void GraphVertex::processor(ScopedComponent<GraphProcessor>&& processor) noexcept {
    _processor = ::std::move(processor);
} 

::std::vector<GraphDependency>& GraphVertex::dependencies() noexcept {
    return _dependencies;
}

const ::std::vector<GraphDependency>& GraphVertex::dependencies() const noexcept {
    return _dependencies;
}

::std::vector<GraphData*>& GraphVertex::emits() noexcept {
    return _emits;
}

void GraphVertex::reset() noexcept {
    _activated.store(false, ::std::memory_order_relaxed);
    _waiting_num.store(0, ::std::memory_order_relaxed);
    _closure = nullptr;
    for (auto& denpendency : _dependencies) {
        denpendency.reset();
    }
    _runnable_vertexes = nullptr;
    _processor->reset(*this);
    _log.clear();
    _static_mem_manager.clear();
}

const ::std::string& GraphVertex::clog() const noexcept {
    return _log;
}

::std::string& GraphVertex::log() noexcept {
    return _log;
}

GraphDependency* GraphVertex::named_dependency(const ::std::string& name) noexcept {
    return _builder->named_dependency(name, _dependencies);
}

GraphDependency* GraphVertex::anonymous_dependency(size_t index) noexcept {
    return _builder->anonymous_dependency(index, _dependencies);
}

size_t GraphVertex::anonymous_dependency_size() const noexcept {
    return _builder->anonymous_dependency_size();
}

GraphDependency* GraphVertex::dependency(size_t index) noexcept {
    return anonymous_dependency(index);
}

size_t GraphVertex::dependency_size() const noexcept {
    return anonymous_dependency_size();
}

GraphData* GraphVertex::named_emit(const ::std::string& name) noexcept {
    return _builder->named_emit(name, _emits);
}

GraphData* GraphVertex::anonymous_emit(size_t index) noexcept {
    return _builder->anonymous_emit(index, _emits);
}

size_t GraphVertex::anonymous_emit_size() const noexcept {
    return _builder->anonymous_emit_size();
}

GraphData* GraphVertex::emit(size_t index) noexcept {
    return anonymous_emit(index);
}

size_t GraphVertex::emit_size() const noexcept {
    return anonymous_emit_size();
}

template <typename T>
const T* GraphVertex::option() const noexcept {
    return _builder->option<T>();
}

const ::std::string& GraphVertex::name() const noexcept {
    return _builder->name();
}

template <typename T, typename ::std::enable_if<!::std::is_move_constructible<T>::value, int32_t>::type>
T* GraphVertex::context() noexcept {
    if (unlikely(!_context)) {
        _context = ::std::unique_ptr<T>(new T);
    }
    return _context.get<T>();
}

template <typename T, typename ::std::enable_if<::std::is_move_constructible<T>::value, int32_t>::type>
T* GraphVertex::context() noexcept {
    if (unlikely(!_context)) {
        _context = T();
    }
    return _context.get<T>();
}

void GraphVertex::trivial(bool trivial) noexcept {
    _trivial = trivial;
}

inline int32_t GraphVertex::setup() noexcept {
    return _processor->setup(*this);
}

bool GraphVertex::activated() const noexcept {
    return _activated.load(::std::memory_order_relaxed);
}

const GraphExecutor* GraphVertex::executor() const noexcept {
    return _executor;
}

const GraphProcessor* GraphVertex::processor() const noexcept {
    return _processor.get();
}

ClosureContext* GraphVertex::closure() noexcept {
    return _closure;
}

void GraphVertex::invoke(Stack<GraphVertex*>& runnable_vertexes) noexcept {
    bool essential_failed = false;
    for (auto& dependency : _dependencies) {
       if(dependency.is_essential() && (!dependency.ready() || dependency.empty())) {
           essential_failed = true;
           break;
       } 
    }
    if(!essential_failed) {
        if (_trivial) {
            LOG(TRACE) << "inplace run " << *this;
            // todo: emit中可以记录一下是否来自trivial的vertex
            // 否则trivial的vertex不支持外部并发注入data
            // 虽然一般没这种情况，不过可以更完备
            _runnable_vertexes = &runnable_vertexes;
            run(GraphVertexClosure(*_closure, *this));
        } else {
            LOG(TRACE) << "invoke " << *this;
            _executor->run(this, GraphVertexClosure(*_closure, *this));
        }
    } else {
        LOG(TRACE) << "essential_failed skip " << *this;
        _runnable_vertexes = &runnable_vertexes;
        for(auto data : _emits) {  // 不运行算子，直接发布emits
            auto commiter = data->emit<Any>();
        }
    }
}

Stack<GraphVertex*>* GraphVertex::runnable_vertexes() noexcept {
    return _runnable_vertexes;
}

void GraphVertex::run(GraphVertexClosure&& closure) noexcept {
    _processor->process(*this, ::std::move(closure));
}

bool GraphVertex::ready(GraphDependency*) noexcept {
    int64_t waiting_num = _waiting_num.load(::std::memory_order_acquire);
    waiting_num = _waiting_num.fetch_sub(1, ::std::memory_order_acq_rel) - 1;
    return waiting_num == 0 ? true : false;
}

void GraphVertex::set_graph(Graph* graph) noexcept {
    _graph = graph;
}

template <typename T>
const T* GraphVertex::get_graph_context() const noexcept {
    return _graph->context<T>();
}

template <typename T>
T* GraphVertex::get_graph_mutable_context() noexcept {
    return _graph->mutable_context<T>();
}

#ifdef GOOGLE_PROTOBUF_HAS_ARENAS
template <typename T, typename... Args>
inline ReusableAccessor<T> GraphVertex::create(Args&&... args) {
    return _graph->create<T>(::std::forward<Args>(args)...);
}
#endif // GOOGLE_PROTOBUF_HAS_ARENAS

template <typename T, typename... Args>
inline ReusableAccessor<T> GraphVertex::create_local(Args&&... args) {
    return _static_mem_manager.create<T>(::std::forward<Args>(args)...);
}

#ifdef GOOGLE_PROTOBUF_HAS_ARENAS
inline ReusableManager<::google::protobuf::Arena>& GraphVertex::memory_manager() noexcept {
    return _graph->memory_manager();
}
#endif //GOOGLE_PROTOBUF_HAS_ARENAS

inline ::std::ostream& operator<<(::std::ostream& os, const GraphVertex& vertex) noexcept {
    os << "vertex[" << vertex.name() << "][" << vertex.index() << "]";
    return os;
}

// GraphVertex end
///////////////////////////////////////////////////////////////////////////////

} // graph
} // feed
} // joewu
#endif //joewu_HAOKAN_REC_GRAPH_ENGINE_GRAPH_VERTEX_HPP
