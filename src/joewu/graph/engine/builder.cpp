#include <joewu/graph/engine/builder.h>
#include <joewu/graph/engine/graph.h>
#include <joewu/graph/engine/data.h>
#include <joewu/graph/engine/vertex.h>

namespace joewu {
namespace feed {
namespace graph {

static size_t add_data_if_not_exist(::std::unordered_map<::std::string, size_t>& data_index_by_name,
    const ::std::string& name) {
    size_t data_index = data_index_by_name.size();
    auto result = data_index_by_name.emplace(::std::make_pair(name, data_index));
    if (!result.second) {
        data_index = result.first->second;
    } else {
        LOG(NOTICE) << "add data[" << name << "] at " << data_index;
    }
    return data_index;
}

int32_t GraphBuilder::finish() noexcept {
    LOG(TRACE) << "analyzing " << *this;
    _producer_by_data_index.clear();
    _data_index_by_name.clear();
    for (auto& vertex : _vertexes) {
        if (unlikely(0 != vertex.finish(_data_index_by_name, _producer_by_data_index))) {
            LOG(WARNING) << "finish " << vertex << " failed";
            return -1;
        }
        LOG(NOTICE) << "add " << vertex;
    }
    LOG(NOTICE) << "finish analyze " << this << " with "
        << _vertexes.size() << " vertexes and "
        << _data_index_by_name.size() << " data";
    return 0;
}

::std::unique_ptr<Graph> GraphBuilder::build() const noexcept {
    LOG(TRACE) << "building " << *this;
    ::std::unique_ptr<Graph> graph(
        new Graph(*_executor, _vertexes.size(), _data_index_by_name));
    auto& vertexes = graph->vertexes();
    size_t i = 0;
    for (auto& builder : _vertexes) {
        vertexes[i].set_graph(graph.get());
        if (unlikely(0 != builder.build(*_executor, vertexes[i], graph->data()))) {
            LOG(WARNING) << "build " << vertexes[i] << " failed";
            return ::std::unique_ptr<Graph>();
        }
        ++i;
    }
    for (const auto& one_data : graph->data()) {
        if (unlikely(0 != one_data.error_code())) {
            LOG(WARNING) << one_data << " build failed";
            return ::std::unique_ptr<Graph>();
        }
    }
    return graph;
}

int32_t GraphVertexBuilder::finish(
    ::std::unordered_map<::std::string, size_t>& data_index_by_name,
    ::std::unordered_map<size_t, const GraphVertexBuilder*>& producer_by_data_index) noexcept {
    LOG(TRACE) << "analyzing " << *this;
    for (auto& dependency : _named_dependencies) {
        if (unlikely(0 != dependency.finish(data_index_by_name))) {
            LOG(WARNING) << "analyze " << *this << " failed at named dependency["
                << dependency.name() << "]";
            return -1;
        }
    }
    for (size_t i = 0; i < _anonymous_dependencies.size(); ++i) {
        auto& dependency = _anonymous_dependencies[i];
        if (unlikely(0 != dependency.finish(data_index_by_name))) {
            LOG(WARNING) << "analyze " << *this << " failed at anonymous dependency["
                << i << "]";
            return -1;
        }
    }
    for (auto& emit : _named_emits) {
        if (unlikely(0 != emit.finish(data_index_by_name, producer_by_data_index))) {
            LOG(WARNING) << "analyze " << *this << " failed at named emit["
                << emit.name() << "]";
            return -1;
        }
    }
    for (size_t i = 0; i < _anonymous_emits.size(); ++i) {
        auto& emit = _anonymous_emits[i];
        if (unlikely(0 != emit.finish(data_index_by_name, producer_by_data_index))) {
            LOG(WARNING) << "analyze " << *this << " failed at anonymous emit["
                << i << "]";
            return -1;
        }
    }
    if (_processor != nullptr) {
        auto* processor = _processor;
        _processor_creator = [processor] {
            return ScopedComponent<GraphProcessor>(processor, nullptr);
        };
    } else {
        auto* context = &_builder->application_context();
        auto processor_name = _processor_name;
        _processor_creator = [context, processor_name] {
            auto component = context->get_or_create<GraphProcessor>(processor_name);
            if (!component) {
                LOG(WARNING) << "get_or_create compnent named [" << processor_name << "] failed";
            }
            return component;
        };
    }
    return 0;
}


int32_t GraphVertexBuilder::build(GraphExecutor& executor,
    GraphVertex& vertex, ::std::vector<GraphData>& data) const noexcept {
    LOG(TRACE) << "building vertex[" << _index << "]";
    vertex.builder(*this);
    vertex.executor(executor);
    vertex.index(_index);
    auto processor = _processor_creator();
    if (!processor) {
        LOG(WARNING) << *this << " build failed for no valid processor";
        return -1;
    }
    vertex.processor(::std::move(processor));

    auto& dependencies = vertex.dependencies();
    dependencies.resize(_named_dependencies.size() + _anonymous_dependencies.size());
    {
        size_t i = 0;
        for (auto& builder : _named_dependencies) {
            auto& dependency = dependencies[i++];
            builder.build(dependency, vertex, data);
        }
        for (auto& builder : _anonymous_dependencies) {
            auto& dependency = dependencies[i++];
            builder.build(dependency, vertex, data);
        }
    }

    auto& emits = vertex.emits();
    emits.resize(_named_emits.size() + _anonymous_emits.size());
    {
        size_t i = 0;
        for (auto& builder : _named_emits) {
            auto& one_data = data[builder.target_index()];
            one_data.producer(vertex);
            if (builder.on_emit()){
                one_data.on_emit(builder.on_emit());
            }
            emits[i++] = &one_data;
        }
        for (auto& builder : _anonymous_emits) {
            auto& one_data = data[builder.target_index()];
            one_data.producer(vertex);
            if (builder.on_emit()){
                one_data.on_emit(builder.on_emit());
            }
            emits[i++] = &one_data;
        }
    }

    auto ret = vertex.setup();
    if (unlikely(ret != 0)) {
        LOG(WARNING) << "set up vertex[" << _index << "] failed";
    }
    return ret;
}

int32_t GraphDependencyBuilder::finish(
    ::std::unordered_map<::std::string, size_t>& data_index_by_name) noexcept {
    LOG(TRACE) << _source << " analyzing dependency[" << _name << "] of vertex["
        << _source->index() << "] to data[" << _target << "]"
        << (_condition.empty() ? "" : " on data[") << _condition
        << (_condition.empty() ? "" : "]");
    _target_index = add_data_if_not_exist(data_index_by_name, _target);
    LOG(DEBUG) << "resolve target to data[" << _target << "][" << _target_index << "]";
    if (!_condition.empty()) {
        _condition_index = add_data_if_not_exist(data_index_by_name, _condition);
        LOG(DEBUG) << "resolve condition to data[" << _condition << "][" << _condition_index << "]";
    }
    return 0;
}

void GraphDependencyBuilder::build(GraphDependency& dependency, GraphVertex& vertex,
    ::std::vector<GraphData>& data) const noexcept {
    LOG(TRACE) << "building dependency vertex[" << _source->index()
        << "] to data[" << _target_index << "]"
        << (_condition.empty() ? "" : " on data[")
        << (_condition.empty() ? "" : ::std::to_string(_condition_index))
        << (_condition.empty() ? "" : "]");
    GraphData* target = &data[_target_index];
    target->add_successor(dependency);
    dependency.source(vertex);
    dependency.target(*target, _mutable, _essential);

    GraphData* condition = nullptr;
    if (!_condition.empty()) {
        condition = &data[_condition_index];
        condition->add_successor(dependency);
        dependency.condition(*condition, _establish_value);
    }
}

///////////////////////////////////////////////////////////////////////////////
// GraphEmitBuilder begin
int32_t GraphEmitBuilder::finish(
    ::std::unordered_map<::std::string, size_t>& data_index_by_name,
    ::std::unordered_map<size_t, const GraphVertexBuilder*>& producer_by_data_index) noexcept {
    LOG(TRACE) << "analyzing emit[" << _name << "][" << _index << "] of vertex["
        << _source->index() << "]";
    _target_index = add_data_if_not_exist(data_index_by_name, _target);
    auto result = producer_by_data_index.emplace(_target_index, _source);
    if (unlikely(!result.second)) {
        LOG(WARNING) << "vertex[" << _source->index() << "] emit[" << _name << "][" << _index
            << "] to data[" << _target << "] conflict with vertex["
            << result.first->second->index() << "]";
        return -1;
    }
    LOG(DEBUG) << "resolve target to data[" << _target << "][" << _target_index << "]";
    return 0;
}
// GraphEmitBuilder end
///////////////////////////////////////////////////////////////////////////////

} // graph
} // feed
} // joewu
