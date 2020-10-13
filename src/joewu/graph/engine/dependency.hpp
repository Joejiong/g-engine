#ifndef joewu_HAOKAN_REC_GRAPH_ENGINE_GRAPH_DEPENDENCY_HPP
#define joewu_HAOKAN_REC_GRAPH_ENGINE_GRAPH_DEPENDENCY_HPP

#include <joewu/graph/engine/dependency.h>
#include <joewu/graph/engine/vertex.h>
#include <joewu/graph/engine/data.h>

namespace joewu {
namespace feed {
namespace graph {

void GraphDependency::source(GraphVertex& vertex) noexcept {
    _source = &vertex;
}

void GraphDependency::target(GraphData& data, bool is_mutable, bool is_essential) noexcept {
    _target = &data;
    _mutable = is_mutable;
    _essential = is_essential;
}

void GraphDependency::condition(GraphData& data, bool establish_value) noexcept {
    _condition = &data;
    _establish_value = establish_value;
}

void GraphDependency::declare_mutable(bool is_mutable) noexcept {
    _mutable = is_mutable;
}

template <typename T>
inline void GraphDependency::declare_type() noexcept {
    _target->declare_type<T>();
}

bool GraphDependency::is_mutable() const noexcept {
    return _mutable;
}

void GraphDependency::declare_essential(bool is_essential) noexcept {
    _essential = is_essential;
}

bool GraphDependency::is_essential() const noexcept {
    return _essential;
}

template <typename T>
inline InputChannel<T> GraphDependency::declare_channel() noexcept {
    return InputChannel<T>(*this);
}

template <typename T>
inline MutableInputChannel<T> GraphDependency::declare_mutable_channel() noexcept {
    return MutableInputChannel<T>(*this);
}

void GraphDependency::reset() noexcept {
    _waiting_num.store(0, ::std::memory_order_relaxed);
    _established = false;
    _ready = false;
}

bool GraphDependency::ready() const noexcept {
    return _ready;
}

bool GraphDependency::established() const noexcept {
    return _established;
}

bool GraphDependency::empty() const noexcept {
    return _target->empty();
}

template <typename T>
const T* GraphDependency::value() const noexcept {
    if (unlikely(!_ready || _target->empty())) {
        return nullptr;
    }
    return _target->cvalue<T>();
}

template <typename T>
T GraphDependency::as() const noexcept {
    if (unlikely(!_ready || _target->empty())) {
        return static_cast<T>(0);
    }
    return _target->as<T>();
}

template <typename T>
T* GraphDependency::mutable_value() noexcept {
    if (unlikely(!_ready || !_mutable)) {
        return nullptr;
    }
    return _target->mutable_value<T>();
}

GraphData* GraphDependency::target() noexcept {
    return _target; 
}

const GraphData* GraphDependency::target() const noexcept {
    return _target; 
}

const GraphData* GraphDependency::inner_target() const noexcept {
    return _target; 
}

GraphData* GraphDependency::mutable_target() noexcept {
    if (unlikely(!_mutable)) {
        return nullptr;
    }
    return _target; 
}

const GraphData* GraphDependency::condition() const noexcept {
    return _condition; 
}

const GraphData* GraphDependency::inner_condition() const noexcept {
    return _condition; 
}

inline GraphDependency::GraphDependency(GraphDependency&& other) noexcept :
        _source(other._source),
        _target(other._target),
        _condition(other._condition),
        _establish_value(other._establish_value),
        _mutable(other._mutable),
        _waiting_num(other._waiting_num.load()),
        _established(other._established),
        _ready(other._ready),
        _essential(other._essential) {
    ::std::swap(_source, other._source);
    ::std::swap(_target, other._target);
    ::std::swap(_condition, other._condition);
    ::std::swap(_establish_value, other._establish_value);
    ::std::swap(_mutable, other._mutable);
    auto tmp = _waiting_num.load();
    _waiting_num.store(other._waiting_num.load());
    other._waiting_num.store(tmp);
    ::std::swap(_established, other._established);
    ::std::swap(_ready, other._ready);
    ::std::swap(_essential, other._essential);
}

GraphDependency::GraphDependency(const GraphDependency&) noexcept {
    LOG(FATAL) << "can not copy a dependency";
    abort();
}

bool GraphDependency::check_established() noexcept {
    if (_condition == nullptr) {
        _established = true;
    } else {
        bool value = _condition->as<bool>();
        if (value == _establish_value) {
            _established = true;
        }
    }
    return _established;
}

int32_t GraphDependency::activate(Stack<GraphData*>& activating_data) noexcept {
    // 根据是否是条件依赖，对waiting_num进行+1或者+2
    int64_t waiting_num = _condition == nullptr ? 1 : 2;
    waiting_num = _waiting_num.fetch_add(waiting_num, ::std::memory_order_acq_rel) + waiting_num;
    LOG(TRACE) << "activing dependency source " << *_source << " -> " << *_target << " waiting num after activation is " << waiting_num;
    // waiting_num终值域[-1, 0, 1, 2]
    // [-1, 0]均表达激活前已经就绪，其余终值等待后续data可用来出发就绪
    // 包括condition不满足[-2 0|-1]和condition和target都就绪[-1 -1]
    // [1]需要检测condition，如果成立（条件不存在也是成立）则尝试激活target
    switch (waiting_num) {
    // 激活时已经就绪，且条件不成立
    case -1: {
                 return 1;
             }
    // 激活时已经就绪，且条件可能成立
    case 0: {
                if (check_established()) {
                    auto acquired_depend = !_mutable ?
                        _target->acquire_immutable_depend() : _target->acquire_mutable_depend();
                    if (unlikely(!acquired_depend)) {
                        LOG(WARNING) << "dependency " << _source << " to "
                            << *_target << " can not be mutable for other already depend it";
                        return -1;
                    }
                    _ready = _target->ready();
                }
                return 1;
            }
    case 1: {
                // 无condition，激活target
                if (_condition == nullptr) {
                    _established = true;
                    auto acquired_depend = !_mutable ?
                        _target->acquire_immutable_depend() : _target->acquire_mutable_depend();
                    if (unlikely(!acquired_depend)) {
                        LOG(WARNING) << "dependency " << _source << " to "
                            << *_target << " can not be mutable for other already depend it";
                        return -1;
                    }
                    _target->trigger(activating_data);
                // condition未就绪，激活condition
                } else if (!_condition->ready()) {
                    _condition->trigger(activating_data);
                // condition成立，激活target
                } else if (check_established()) {
                    auto acquired_depend = !_mutable ?
                        _target->acquire_immutable_depend() : _target->acquire_mutable_depend();
                    if (unlikely(!acquired_depend)) {
                        LOG(WARNING) << "dependency vertex[" << _source->index() << "] to "
                            << *_target << " on "
                            << *_condition
                            << " can not be mutable for other already mutate it";
                        return -1;
                    }
                    LOG(DEBUG) << "condition " << *_condition
                        << " satisfied try activate target "
                        << *_target;
                    _target->trigger(activating_data);
                }
                // else condition不成立，但是waiting_num == 1
                // 说明condition正在【失败过程中】，即两次-1之间
                // 等待另一次-1到来即可，这里不做处理
                break;
            }
    // condition未就绪，激活condition
    case 2: {
                _condition->trigger(activating_data);
                break;
            }
    default: {
                 LOG(WARNING) << "unexpected waiting num " << waiting_num << " met, maybe a bug?";
                 break;
             }
    }
    return 0;
}

void GraphDependency::ready(GraphData* data, Stack<GraphVertex*>& runnable_vertexes) noexcept {
    LOG(TRACE) << "dependency " << *_source << " -> " << *data << " is ready";
    int64_t waiting_num = _waiting_num.fetch_sub(1, ::std::memory_order_acq_rel) - 1;
    // condition完成时检测条件是否成立
    if (data == _condition) {
        // 成立时
        if (check_established()) {
            // 如果waiting num是1，，则激活target
            if (waiting_num == 1) {
                auto acquired_depend = !_mutable ?
                    _target->acquire_immutable_depend() : _target->acquire_mutable_depend();
                if (unlikely(!acquired_depend)) {
                    LOG(WARNING) << "dependency " << _source << " to "
                        << *_target << " can not be mutable for other already depend it";
                    _source->closure()->finish(-1);
                    return;
                }

                LOG(DEBUG) << "condition " << *_condition
                    << " satisfied try activate target "
                    << *_target;
                int32_t rec_ret = _target->recursive_activate(runnable_vertexes,
                    _source->closure());
                if (0 != rec_ret) {
                    LOG(WARNING) << "recursive_activate from "
                        << *_target << " failed";
                    _source->closure()->finish(rec_ret);
                    return;
                }
            }
        // 不成立，waiting num再减1，由于target可以从别的渠道完成，有击穿的可能
        // 通过边沿触发和激活时[-1, 0]双终态解决
        } else if (waiting_num != 0) {
            waiting_num = _waiting_num.fetch_sub(1, ::std::memory_order_acq_rel) - 1;
        }
    }
    // 无condition的target就绪
    // 或者condition满足时target已经就绪
    // 或者condition不满足时
    // 当condition和target的就绪以及激活操作并发时
    // 就绪的终态[0]和激活的终态[-1, 0]确保不重不漏
    if (waiting_num == 0 && nullptr != _source) {
        LOG(DEBUG) << "dependency vertex[" << _source->index() << "] -> "
            << *data << " ready";
        if (data == _target) {
            _ready = check_established();
        } else {
            _ready = established() && _target->ready();
        }
        if (_source->ready(this)) {
            LOG(DEBUG) << "dependency vertex[" << _source->index() << "] is ready to run";
            runnable_vertexes.emplace(_source);
        }
    }
}
int GraphDependency::activated_vertex_name(std::string& vertex_name) const noexcept {
    int err = 0;
    if (_ready) {
        if (_target->producer()) {
            vertex_name = _target->producer()->name();
        } else {
            err = 1;
        }
    } else {
        err = -1;
    }
    return err;
}

} // graph
} // feed
} // joewu
#endif //joewu_HAOKAN_REC_GRAPH_ENGINE_GRAPH_DEPENDENCY_HPP
