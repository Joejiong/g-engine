#include <tuple>
#include <bthread.h>
#include <bthread/mutex.h>
#include <joewu/graph/engine/executor.h>
#include <joewu/graph/engine/vertex.h>

namespace joewu {
namespace feed {
namespace graph {

void* execute_invoke_vertex(void* args) {
    auto param = reinterpret_cast<::std::tuple<GraphVertex*, GraphVertexClosure>*>(args);
    auto vertex = ::std::get<0>(*param);
    auto& closure = ::std::get<1>(*param);
    vertex->run(::std::move(closure));
    delete param;
    return NULL;
}

static void* execute_invoke_closure(void* args) {
    auto param = reinterpret_cast<::std::tuple<ClosureContext*, ClosureCallback*>*>(args);
    auto closure = ::std::get<0>(*param);
    auto callback = ::std::get<1>(*param);
    closure->run(callback);
    delete param;
    return NULL;
}

Closure BthreadGraphExecutor::create_closure() noexcept {
    return Closure::create<::bthread::Mutex>(*this);
}

int32_t BthreadGraphExecutor::run(GraphVertex* vertex,
    GraphVertexClosure&& closure) noexcept {
    bthread_t th;
    auto param = new ::std::tuple<GraphVertex*, GraphVertexClosure>(
        vertex, ::std::move(closure));
    if (0 != bthread_start_background(&th, NULL, execute_invoke_vertex, param)) {
        LOG(WARNING) << "start bthread to run vertex failed";
        closure = ::std::move(::std::get<1>(*param));
        delete param;
        return -1;
    }
    return 0;
}

int32_t BthreadGraphExecutor::run(ClosureContext* closure,
    ClosureCallback* callback) noexcept {
    bthread_t th;
    auto param = new ::std::tuple<ClosureContext*, ClosureCallback*>(
        closure, callback);
    if (0 != bthread_start_background(&th, NULL, execute_invoke_closure, param)) {
        LOG(WARNING) << "start bthread to run closure failed";
        delete param;
        return -1;
    }
    return 0;
}

} // graph
} // feed
} // joewu
