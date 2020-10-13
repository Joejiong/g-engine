#ifndef joewu_HAOKAN_REC_GRAPH_ENGINE_GRAPH_DATA_HPP
#define joewu_HAOKAN_REC_GRAPH_ENGINE_GRAPH_DATA_HPP

#include <joewu/graph/engine/data.h>
#include <joewu/graph/engine/vertex.h>
#include <joewu/graph/engine/closure.h>
#include <joewu/graph/engine/dependency.h>

namespace joewu {
namespace feed {
namespace graph {

///////////////////////////////////////////////////////////////////////////////
// Commiter begin
template <typename T>
inline Commiter<T>::Commiter(GraphData& data) noexcept :
    _data(&data), _valid(data.acquire()), _keep_reference(data.has_preset_value()) {}

template <typename T>
inline Commiter<T>::Commiter(Commiter&& other) noexcept :
    _data(other._data), _valid(other._valid), _keep_reference(other._keep_reference) {
    other._data = nullptr;
    other._valid = false;
    other._keep_reference = false;
}

template <typename T>
inline Commiter<T>& Commiter<T>::operator=(Commiter&& other) noexcept {
    ::std::swap(_data, other._data);
    ::std::swap(_valid, other._valid);
    ::std::swap(_keep_reference, other._keep_reference);
    other.release();
    return *this;
}

template <typename T>
inline Commiter<T>::operator bool() const noexcept {
    return valid();
}

template <typename T>
inline bool Commiter<T>::valid() const noexcept {
    return _valid;
}

template <typename T>
inline Commiter<T>::~Commiter() noexcept {
    release();
}

template <typename T>
inline T* Commiter<T>::get() noexcept {
    if (likely(_valid)) {
        _data->empty(false);
        if (_keep_reference) {
            return _data->mutable_value<T>();
        } else {
            return _data->certain_type_non_reference_mutable_value<T>();
        }
    } else {
        return nullptr;
    }
}

template <typename T>
inline T* Commiter<T>::operator->() noexcept {
    return get();
}

template <typename T>
inline T& Commiter<T>::operator*() noexcept {
    return *get();
}

template <typename T>
inline void Commiter<T>::ref(T& value) noexcept {
    _data->empty(false);
    _data->ref(value);
    _keep_reference = true;
}

template <typename T>
inline void Commiter<T>::ref(const T& value) noexcept {
    _data->empty(false);
    _data->ref(value);
    _keep_reference = true;
}

template <typename T>
inline void Commiter<T>::cref(const T& value) noexcept {
    _data->empty(false);
    _data->cref(value);
    _keep_reference = true;
}

template <typename T>
inline void Commiter<T>::clear() noexcept {
    if (likely(_valid)) {
        return _data->empty(true);
    }
    _keep_reference = false;
}

template <typename T>
inline void Commiter<T>::release() noexcept {
    if (likely(_valid)) {
        _data->release();
        _valid = false;
    }
}

template <typename T>
inline void Commiter<T>::cancel() noexcept {
    if (likely(_valid)) {
        _valid = false;
        _data = nullptr;
    }
}
// Commiter end
///////////////////////////////////////////////////////////////////////////////

///////////////////////////////////////////////////////////////////////////////
// OutputData begin
template <typename T>
inline Commiter<T> OutputData<T>::emit() noexcept {
    return _data->emit<T>();
}

template <typename T>
inline OutputData<T>::OutputData(GraphData& data) noexcept : _data(&data) {}
// OutputData end
///////////////////////////////////////////////////////////////////////////////

/////////////////////////////////////////////////////////////
// GraphData begin
inline bool GraphData::ready() const noexcept {
    return SEALED_CLOSURE == _closure.load(::std::memory_order_acquire);
}

inline bool GraphData::empty() const noexcept {
    return _empty || !_data;
}

inline void GraphData::name(const ::std::string& name) noexcept {
    _name = name;
}

inline const ::std::string& GraphData::name() const noexcept {
    return _name;
}

inline int32_t GraphData::error_code() const noexcept {
    return _error_code;
}

inline void GraphData::reset() noexcept {
    _acquired.store(false, ::std::memory_order_relaxed);
    _empty = true;
    _has_preset_value = false;
    _active = false;
    _closure.store(nullptr, ::std::memory_order_relaxed);
    _depend_state.store(0, ::std::memory_order_relaxed);
}

inline GraphVertex* GraphData::producer() noexcept {
    return _producer;
}

inline const GraphVertex* GraphData::producer() const noexcept {
    return _producer;
}

inline void GraphData::producer(GraphVertex& producer) noexcept {
    _producer = &producer;
}

inline void GraphData::on_emit(const OnEmitFunction& on_emit) noexcept{
    _on_emit = &(on_emit);
}

inline bool GraphData::bind(ClosureContext& closure) noexcept {
    // todo: 多次bind进行链式挂载
    ClosureContext* expected = nullptr;
    closure.depend_data_add();
    closure.add_waiting_data(this);
    if (unlikely(!_closure.compare_exchange_strong(expected, &closure,
                ::std::memory_order_acq_rel))) {
        closure.depend_data_sub();
        return false;
    }
    return true;
}

inline void GraphData::data_num(size_t num) noexcept {
    _data_num = num;
}

inline void GraphData::vertex_num(size_t num) noexcept {
    _vertex_num = num;
}

inline void GraphData::executer(GraphExecutor& executer) noexcept {
    _executer = &executer;
}

inline void GraphData::add_successor(GraphDependency& successor) noexcept {
    _successors.push_back(&successor);
}

inline bool GraphData::acquire() noexcept {
    bool expected = false;
    if (likely(_acquired.compare_exchange_strong(expected, true,
                ::std::memory_order_acq_rel))) {
        return true;
    }
    return false;
}

template <>
inline const Any* GraphData::cvalue<Any>() const noexcept {
    if (unlikely(_empty)) {
        return nullptr;
    }
    return &_data;
}

template <typename T>
inline const T* GraphData::cvalue() const noexcept {
    if (unlikely(_empty)) {
        return nullptr;
    }
    return _data.get<T>();
}

template <typename T>
inline const T* GraphData::value() const noexcept {
    return cvalue<T>();
}

template <typename T>
inline T GraphData::as() const noexcept {
    if (unlikely(_empty)) {
        return static_cast<T>(0);
    }
    return _data.as<T>();
}

template <typename T>
inline Commiter<T> GraphData::emit() noexcept {
    return Commiter<T>(*this);
}

template <>
inline OutputData<Any> GraphData::declare_type<Any>() noexcept {
    return OutputData<Any>(*this);
}

template <typename T>
inline OutputData<T> GraphData::declare_type() noexcept {
    if (_declare_type == nullptr) {
        _declare_type = &TypeId<T>().ID;
        return OutputData<T>(*this);
    } else if (_declare_type == &TypeId<T>().ID) {
        return OutputData<T>(*this);
    } else {
        LOG(WARNING) << *this << " declare type[" << TypeId<T>().get_type_name()
                     << "] conflict with previous type[" << *_declare_type << "]";
        _error_code = -1;
        return OutputData<T>();
    }
}

inline bool GraphData::forward(GraphDependency& dependency) noexcept {
    if (unlikely(!dependency.ready())) {
        return false;
    }
    if (unlikely(!acquire())) {
        return false;
    }
    auto& other = dependency.target()->_data;
    if (dependency.target()->need_mutable()) {
        if (dependency.is_mutable() && !other.is_const_reference()) {
            _data.ref(other);
        } else {
            _data = other;
        }
    } else if (dependency.is_mutable()) {
        _data.ref(dependency.target()->_data);
    } else {
        _data.cref(dependency.target()->_data);
    }
    _empty = false;
    release();
    return true;
}

template <typename T>
inline void GraphData::preset(T& value) noexcept {
    _data.ref(value);
    _has_preset_value = true;
}

inline bool GraphData::has_preset_value() const noexcept {
    return _has_preset_value;
}

inline bool GraphData::mark_active() noexcept {
    bool already_active = _active;
    _active = true;
    return already_active;
}

inline bool GraphData::acquire_immutable_depend() noexcept {
    auto state = _depend_state.exchange(1, ::std::memory_order_relaxed);
    return state != 2;
}

inline bool GraphData::acquire_mutable_depend() noexcept {
    auto state = _depend_state.exchange(2, ::std::memory_order_relaxed);
    return state == 0;
}

inline bool GraphData::need_mutable() const noexcept {
    return 2 == _depend_state.load(::std::memory_order_relaxed);
}

template <>
inline Any* GraphData::mutable_value<Any>() noexcept {
    if (unlikely(_empty)) {
        return nullptr;
    }
    return &_data;
}

template <typename T>
inline T* GraphData::mutable_value() noexcept {
    if (unlikely(_empty)) {
        return nullptr;
    }
    return _data.get<T>();
}

template <>
inline Any* GraphData::certain_type_non_reference_mutable_value<Any>() noexcept {
    if (unlikely(_data.is_reference())) {
        _data.clear();
    }
    return &_data;
}

template <typename T, typename ::std::enable_if<!::std::is_move_constructible<T>::value, int32_t>::type>
inline T* GraphData::certain_type_non_reference_mutable_value() noexcept {
    if (unlikely(_data.is_reference())) {
        _data = ::std::unique_ptr<T>(new T);
        return _data.get<T>();
    }
    auto result = _data.get<T>();
    if (unlikely(result == nullptr)) {
        _data = ::std::unique_ptr<T>(new T);
        result = _data.get<T>();
    }
    return result;
}

template <typename T, typename ::std::enable_if<::std::is_move_constructible<T>::value, int32_t>::type>
inline T* GraphData::certain_type_non_reference_mutable_value() noexcept {
    if (unlikely(_data.is_reference())) {
        _data = T();
        return _data.get<T>();
    }
    auto result = _data.get<T>();
    if (unlikely(result == nullptr)) {
        _data = T();
        result = _data.get<T>();
    }
    return result;
}

template <typename T>
inline void GraphData::ref(T& value) noexcept {
    _data.ref(value);
}

template <typename T>
inline void GraphData::cref(const T& value) noexcept {
    _data.cref(value);
}

inline void GraphData::empty(bool empty) noexcept {
    _empty = empty;
}

inline void GraphData::trigger(Stack<GraphData*>& activating_data) noexcept {
    LOG(TRACE) << "triggering data " << _name;
    if (!mark_active()) {
        if (!ready()) {
            LOG(DEBUG) << "data " << _name << " triggered";
            activating_data.emplace(this);
        }
    }
}

inline int32_t GraphData::activate(Stack<GraphData*>& activating_data,
    Stack<GraphVertex*>& runnable_vertexes, ClosureContext* closure) noexcept {
    LOG(TRACE) << "activating data " << _name;
    // trigger时检测过一次ready，送给activate的如果没有producer直接报错
    if (unlikely(_producer == nullptr)) {
        LOG(WARNING) << "can not activate data[" << _name << "] with no producer";
        return -1;
    }
    if (unlikely(0 != _producer->activate(activating_data, runnable_vertexes, closure))) {
        LOG(WARNING) << "activate producer vertex["
            << _producer->index() << "] of data[" << _name << "] failed";
        return -1;
    }
    return 0;
}

inline ::std::ostream& operator<<(::std::ostream& os, const ::joewu::feed::graph::GraphData& data) {
    os << "data[" << data._name << "]";
    return os;
}
// GraphData end
/////////////////////////////////////////////////////////////

} // graph
} // feed
} // joewu

#endif //joewu_HAOKAN_REC_GRAPH_ENGINE_GRAPH_DATA_HPP
