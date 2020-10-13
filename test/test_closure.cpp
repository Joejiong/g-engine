#include <thread>
#include <future>
#include <inttypes.h>
#include <gtest/gtest.h>
#include <base/logging.h>
#include <bthread/mutex.h>
#include <joewu/graph/engine/closure.h>
#include <joewu/graph/engine/executor.h>

using ::joewu::feed::graph::Closure;
using ::joewu::feed::graph::BthreadGraphExecutor;

BthreadGraphExecutor executor;

TEST(closure, create_with_mutex) {
    auto closure = Closure::create<::bthread::Mutex>(executor);
    closure.context()->fire();
}

TEST(closure, finish_when_data_ready) {
    auto closure = Closure::create<::bthread::Mutex>(executor);
    auto context = closure.context();
    context->depend_data_add();
    context->depend_vertex_add();
    ::std::thread(
        [context] () {
            usleep(100000);
            context->depend_data_sub();
        }).detach();
    context->fire();
    ASSERT_FALSE(closure.finished());
    ASSERT_EQ(0, closure.get());
    ASSERT_EQ(0, closure.error_code());
    ASSERT_TRUE(closure.finished());
    context->depend_vertex_sub();
}

TEST(closure, finish_when_error_occur) {
    auto closure = Closure::create<::bthread::Mutex>(executor);
    auto context = closure.context();
    context->depend_data_add();
    context->depend_vertex_add();
    ::std::thread(
        [context] () {
            usleep(100000);
            context->finish(-10086);
        }).detach();
    context->fire();
    ASSERT_FALSE(closure.finished());
    ASSERT_EQ(-10086, closure.get());
    ASSERT_EQ(-10086, closure.error_code());
    ASSERT_TRUE(closure.finished());
    context->depend_vertex_sub();
}

TEST(closure, finish_when_run_over_even_data_is_not_ready) {
    auto closure = Closure::create<::bthread::Mutex>(executor);
    auto context = closure.context();
    context->depend_data_add();
    context->depend_vertex_add();
    ::std::thread(
        [context] () {
            usleep(100000);
            context->depend_vertex_sub();
        }).detach();
    context->fire();
    ASSERT_FALSE(closure.finished());
    ASSERT_EQ(-1, closure.get());
    ASSERT_EQ(-1, closure.error_code());
    ASSERT_TRUE(closure.finished());
}

TEST(closure, wait_until_run_over) {
    auto closure = Closure::create<::bthread::Mutex>(executor);
    auto context = closure.context();
    ::std::atomic<bool> done(false);
    context->depend_data_add();
    context->depend_vertex_add();
    ::std::thread(
        [context, &done] () mutable {
            usleep(100000);
            context->depend_data_sub();
            usleep(100000);
            done.store(true);
            context->depend_vertex_sub();
        }).detach();
    context->fire();
    ASSERT_FALSE(closure.finished());
    ASSERT_EQ(0, closure.get());
    ASSERT_EQ(0, closure.error_code());
    ASSERT_TRUE(closure.finished());
    ASSERT_FALSE(done.load());
    closure.wait();
    ASSERT_TRUE(done.load());
}

TEST(closure, destruct_automatically_wait) {
    ::std::atomic<bool> done(false);
    {
        auto closure = Closure::create<::bthread::Mutex>(executor);
        auto context = closure.context();
        context->depend_data_add();
        context->depend_vertex_add();
        ::std::thread(
            [context, &done] () mutable {
                usleep(100000);
                context->depend_data_sub();
                usleep(100000);
                done.store(true);
                context->depend_vertex_sub();
            }).detach();
        context->fire();
        ASSERT_FALSE(closure.finished());
        ASSERT_EQ(0, closure.get());
        ASSERT_EQ(0, closure.error_code());
        ASSERT_TRUE(closure.finished());
        ASSERT_FALSE(done.load());
    }
    ASSERT_TRUE(done.load());
}

TEST(closure, callback_invoke_on_finish) {
    ::std::atomic<bool> done(false);
    ::std::promise<bool> finish_promise;
    ::std::promise<void> flush_promise;
    auto finish_future = finish_promise.get_future();
    auto flush_future = flush_promise.get_future();
    auto closure = Closure::create<::bthread::Mutex>(executor);
    auto context = closure.context();
    context->depend_data_add();
    context->depend_vertex_add();
    ::std::thread(
        [context, &done] () mutable {
            usleep(100000);
            context->depend_data_sub();
            usleep(100000);
            done.store(true);
            context->depend_vertex_sub();
        }).detach();
    context->fire();
    ASSERT_FALSE(closure.finished());
    closure.on_finish(::std::bind([] (::std::promise<bool>& finish_promise,
                ::std::promise<void>& flush_promise, Closure&& closure) {
            finish_promise.set_value(closure.finished());
            closure.wait();
            flush_promise.set_value();
        }, ::std::move(finish_promise), ::std::move(flush_promise), ::std::placeholders::_1));
    // finish
    ASSERT_TRUE(finish_future.get());
    // 但是还未flush
    ASSERT_FALSE(done.load());
    flush_future.get();
    // callback中的closure可以用来wait
    ASSERT_TRUE(done.load());
}

TEST(closure, callback_invoke_in_place_after_finish) {
    ::std::atomic<bool> done(false);
    auto closure = Closure::create<::bthread::Mutex>(executor);
    auto context = closure.context();
    context->fire();
    ASSERT_TRUE(closure.finished());
    closure.on_finish(
        [&done] (Closure&&) {
            usleep(100000);
            done.store(true);
        });
    // on_finish返回时，callback已经执行
    ASSERT_TRUE(done.load());
}

TEST(closure, default_constructor_and_move_assignment) {
    ::std::atomic<bool> done(false);
    auto closure = Closure::create<::bthread::Mutex>(executor);
    auto context = closure.context();
    context->fire();
    Closure default_closure;
    default_closure = std::move(closure);
    ASSERT_TRUE(default_closure.finished());
    default_closure.on_finish(
        [&done] (Closure&&) {
            usleep(100000);
            done.store(true);
        });
    // on_finish返回时，callback已经执行
    ASSERT_TRUE(done.load());
}
