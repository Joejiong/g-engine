#ifndef joewu_HAOKAN_REC_GRAPH_ENGINE_GRAPH_CLOSURE_HPP 
#define joewu_HAOKAN_REC_GRAPH_ENGINE_GRAPH_CLOSURE_HPP

#include <joewu/graph/engine/closure.h>
#include <joewu/graph/engine/executor.h>

namespace joewu {
namespace feed {
namespace graph {

template <typename M>
class ClosureContextImplement : public ClosureContext {
public:
    inline ClosureContextImplement(GraphExecutor& executor) noexcept;
    virtual ~ClosureContextImplement() noexcept;

protected:
    virtual void wait_finish() noexcept override;
    virtual void notify_finish() noexcept override;
    virtual void wait_flush() noexcept override;
    virtual void notify_flush() noexcept override;

private:
    M _mutex_finish;
    M _mutex_flush;
};

///////////////////////////////////////////////////////////////////////////////
// Closure begin
Closure& Closure::operator=(Closure&& closure) {
    _context = std::move(closure._context);
    return *this;
}

Closure::Closure(Closure&& closure) noexcept :
    _context(::std::move(closure._context)) {}

Closure::Closure(ClosureContext* context) noexcept :
    _context(context) {}

bool Closure::finished() const noexcept {
    return _context->finished();
}

int32_t Closure::get() noexcept {
    return _context->get();
}

void Closure::wait() noexcept {
    return _context->wait();
}

int32_t Closure::error_code() const noexcept {
    return _context->error_code();
}

template <typename C>
void Closure::on_finish(C&& callback) noexcept {
    auto context = _context.release();
    context->on_finish(::std::move(callback));
}

template <typename M>
Closure Closure::create(GraphExecutor& executor) noexcept {
    return Closure(new ClosureContextImplement<M>(executor));
}
// Closure end
///////////////////////////////////////////////////////////////////////////////

///////////////////////////////////////////////////////////////////////////////
// ClosureContext begin
ClosureContext* Closure::context() noexcept {
    return _context.get();
}

ClosureContext::ClosureContext(GraphExecutor& executor) noexcept {
    _executor = &executor;
}

void ClosureContext::finish(int32_t error_code) noexcept {
    ClosureCallback* callback = nullptr;
    if (mark_finished(error_code, callback)) {
        if (callback != nullptr && unlikely(0 != invoke(callback))) {
            LOG(WARNING) << "closure[" << this << "] invoke callback[" << callback
                << "] failed delay to invoke on flush";
            _flush_callback = callback;
        }
    }
}

bool ClosureContext::mark_finished(int32_t error_code, ClosureCallback*& callback) noexcept {
    callback = _callback.load(::std::memory_order_relaxed);
    do {
        if (unlikely(callback == SEALED_CALLBACK)) {
            return false;
        }
    } while (unlikely(!_callback.compare_exchange_weak(callback,
                SEALED_CALLBACK, ::std::memory_order_acq_rel)));
    LOG(DEBUG) << "closure[" << this << "] finish with code[" << error_code
        << "] with callback[" << callback << "]";
    _error_code = error_code;
    notify_finish();
    return true;
}

void ClosureContext::run(ClosureCallback* callback) noexcept {
    (*callback)(Closure(this));
    delete callback;
}

template <typename C>
void ClosureContext::on_finish(C&& callback) noexcept {
    using ::joewu::feed::mlarch::babylon::wrap_moveable_function;
    auto new_callback = new ClosureCallback(
        wrap_moveable_function(::std::move(callback)));
    ClosureCallback* expected = nullptr;
    if (!_callback.compare_exchange_strong(expected, new_callback, 
        ::std::memory_order_acq_rel)) {
        // 直接callback，有可能error_code还没生效
        // todo：这里有微概率阻塞一会儿，可以想想更好的同步方法
        wait_finish();
        run(new_callback);
    }
}

void ClosureContext::fire() noexcept {
    depend_data_sub();
    depend_vertex_sub();
}

bool ClosureContext::finished() const noexcept {
    return _callback.load(::std::memory_order_relaxed);
}

void ClosureContext::depend_vertex_add() noexcept {
    int64_t waiting_num = _waiting_vertex_num.fetch_add(1, ::std::memory_order_acq_rel);
    LOG(DEBUG) << "waiting vertex num after add 1:" <<  waiting_num + 1;
}

void ClosureContext::depend_vertex_sub() noexcept {
    int64_t waiting_num = _waiting_vertex_num.fetch_sub(1, ::std::memory_order_acq_rel) - 1;
    LOG(DEBUG) << "waiting vertex num after sub 1:" << waiting_num;
    if (unlikely(waiting_num == 0)) {
        ClosureCallback* callback = nullptr;
        if (mark_finished(-1, callback)) {
            log_unfinished_data();
            if (callback != nullptr && unlikely(0 != invoke(callback))) {
                LOG(WARNING) << "closure[" << this << "] invoke callback[" << callback
                    << "] failed delay to invoke on flush";
                _flush_callback = callback;
            }
        }

        if (_flush_callback != nullptr) {
            notify_flush();
            callback = _flush_callback;
            _flush_callback = nullptr;
            run(callback);
        } else {
            notify_flush();
        }
    }
}

void ClosureContext::depend_data_add() noexcept {
    int64_t waiting_num = _waiting_data_num.fetch_add(1, ::std::memory_order_acq_rel);
    LOG(DEBUG) << "waiting data num after add 1:" <<  waiting_num + 1;
}

void ClosureContext::add_waiting_data(GraphData* data) noexcept {
    _waiting_data.emplace_back(data);
}

void ClosureContext::depend_data_sub() noexcept {
    int64_t waiting_num = _waiting_data_num.fetch_sub(1, ::std::memory_order_acq_rel) - 1;
    LOG(DEBUG) << "waiting data num after sub 1:" << waiting_num;
    if (waiting_num == 0) {
        ClosureCallback* callback = nullptr;
        if (mark_finished(0, callback)) {
            if (callback != nullptr && unlikely(0 != invoke(callback))) {
                _flush_callback = callback;
            }
        }
    }
}

void ClosureContext::all_data_num(size_t num) noexcept {
    _all_data_num = num;
}

int32_t ClosureContext::get() noexcept {
    LOG(TRACE) << "waiting data ready";
    wait_finish();
    LOG(DEBUG) << "wait data finished with code " << _error_code;
    return _error_code;
}

int32_t ClosureContext::error_code() const noexcept {
    return _error_code;
}

void ClosureContext::wait() noexcept {
    wait_flush();
}

int32_t ClosureContext::invoke(ClosureCallback* callback) noexcept {
    LOG(TRACE) << "invoking closure[" << this << "] callback[" << callback << "]";
    return _executor->run(this, callback);
}
// ClosureContext end
///////////////////////////////////////////////////////////////////////////////

///////////////////////////////////////////////////////////////////////////////
// ClosureContextImplement begin
template <typename M>
ClosureContextImplement<M>::ClosureContextImplement(GraphExecutor& executor) noexcept :
    ClosureContext(executor) {
    _mutex_finish.lock();
    _mutex_flush.lock();
}

template <typename M>
ClosureContextImplement<M>::~ClosureContextImplement() noexcept {
    wait_flush();
}

template <typename M>
void ClosureContextImplement<M>::wait_finish() noexcept {
    std::unique_lock<M> lock(_mutex_finish);
}

template <typename M>
void ClosureContextImplement<M>::notify_finish() noexcept {
    _mutex_finish.unlock();
}

template <typename M>
void ClosureContextImplement<M>::wait_flush() noexcept {
    std::unique_lock<M> lock(_mutex_flush);
}

template <typename M>
void ClosureContextImplement<M>::notify_flush() noexcept {
    _mutex_flush.unlock();
}
// ClosureContextImplement end
///////////////////////////////////////////////////////////////////////////////

} // graph
} // feed
} // joewu
#endif //joewu_HAOKAN_REC_GRAPH_ENGINE_GRAPH_CLOSURE_HPP
