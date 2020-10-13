#ifndef joewu_HAOKAN_REC_GRAPH_ENGINE_GRAPH_DATA_H
#define joewu_HAOKAN_REC_GRAPH_ENGINE_GRAPH_DATA_H

#include <joewu/feed/mlarch/babylon/any.h>
#include <joewu/feed/mlarch/babylon/stack.h>
#include <joewu/feed/mlarch/babylon/concurrent/transient_queue.h>
#include <joewu/graph/engine/on_emit.h>

namespace joewu {
namespace feed {
namespace graph {
using Id = ::joewu::feed::mlarch::babylon::Id;
template <typename T>
using TypeId = ::joewu::feed::mlarch::babylon::TypeId<T>;

// 包装data的写访问器，竞争data使用权
// 胜者可以写操作数据并发布
// 控制data在完整生命周期中只能发布一次
class GraphData;
template <typename T>
class Commiter {
public:
    // 从data创建commiter，创建时会竞争data，只有唯一valid
    inline Commiter(GraphData& data) noexcept;
    // 析构时候自动提交，不可复制
    inline Commiter(const Commiter& other) = delete;
    // 可以移动构造，拆函数，异步等场景，控制延迟提交
    inline Commiter(Commiter&& other) noexcept;
    inline Commiter& operator=(Commiter&& other) noexcept;
    // 析构时自动提交
    inline ~Commiter() noexcept;
    // 可用性判断，访问对象前先判断一下
    // 只有valid的commiter可以访问数据
    inline operator bool() const noexcept;
    inline bool valid() const noexcept;
    // 可用时可以操作数据
    inline T* get() noexcept;
    inline T* operator->() noexcept;
    inline T& operator*() noexcept;
    // 引用输出value
    // 尽量保持持续使用引用，或者持续使用本体
    // 两者之间切换会造成本体被清空，无法复用
    inline void ref(T& value) noexcept;
    inline void ref(const T& value) noexcept;
    inline void cref(const T& value) noexcept;
    // 默认发布数据非空，如果需要发布空数据
    // 需要主动调用clear，不会实际清除底层数据
    inline void clear() noexcept;
    // 主动提交data，之后无法再进行操作
    inline void release() noexcept;
    // 取消发布
    inline void cancel() noexcept;

private:
    GraphData* _data {nullptr};
    bool _valid {false};
    bool _keep_reference {false};
};

template <typename T>
class OutputData {
public:
    inline OutputData() noexcept = default;
    inline OutputData(OutputData&&) noexcept = default;
    inline OutputData(const OutputData&) noexcept = default;
    inline OutputData& operator=(OutputData&&) noexcept = default;
    inline OutputData& operator=(const OutputData&) noexcept = default;

    inline operator bool() const noexcept {
        return _data != nullptr;
    }

    inline Commiter<T> emit() noexcept;

private:
    inline OutputData(GraphData& data) noexcept;

    GraphData* _data {nullptr};

    friend class GraphData;
};

// 连接vertex的中间环节，也是之间交互数据的存放位置
// 对processor承诺，当检测ready之后，可以安全使用数据而不会有竞争风险
// 限定data的写入发布只有一次，因此整个过程无需上锁
// 保证release-acquire衔接即可
//
// 独立的emply标记支持保留data对象的前提下，表达空语义
//
// 声明为可变依赖时稍微特殊，ready之后还会由依赖者修改数据
// 通过保证此时只有唯一依赖者，来满足承诺
class GraphVertex;
class GraphExecutor;
class GraphDependency;
class ClosureContext;
template <typename T>
class OutputChannel;
class GraphData {
public:
    typedef ::joewu::feed::mlarch::babylon::Any Any;
    typedef ::joewu::feed::mlarch::babylon::Stack<GraphData*> DataStack;
    typedef ::joewu::feed::mlarch::babylon::Stack<GraphVertex*> VertexStack;

    // 禁用拷贝和移动构造
    inline GraphData(GraphData&&) = delete;
    inline GraphData(const GraphData&) = delete;
    inline GraphData& operator=(GraphData&&) = delete;
    inline GraphData& operator=(const GraphData&) = delete;

    // 【GraphProcessor::setup】阶段使用
    // 声明变量类型，如果发生声明不一致，返回false
    // 而且，最终builder的build方法会返回失败
    // T == Any 特化：无效果，恒返回true
    template <typename T>
    inline OutputData<T> declare_type() noexcept;

    // 【GraphProcessor::setup】阶段使用
    // 声明为类型T的输出流，得到的输出流可以存在context中供运行时使用
    template <typename T>
    inline OutputChannel<T> declare_channel() noexcept;

    // 【GraphProcessor::on_activate】【GraphProcessor::process】阶段使用
    // 检查是否需要可变发布，即被下游可变依赖
    inline bool need_mutable() const noexcept;

    // 【Graph::run】之前
    // 【GraphProcessor::process】阶段使用
    // 尝试写操作并发布，获得操作权会返回valid的Commiter
    // T == Any 特化：返回包装底层容器的Commiter，用于高级操作
    template <typename T>
    inline Commiter<T> emit() noexcept;

    // 【GraphProcessor::process】阶段使用
    // 转发一个依赖，优先直接引用输出
    // 仅在需要输出可变，即有下游声明了可变依赖
    // 且dependency不支持时采用拷贝
    inline bool forward(GraphDependency& dependency) noexcept;

    // 【Graph::run】之前
    // 设置data的预置值，主要用于从外部提供构造好的实例，供真正emit时复用
    // 典型如RPC场景下，response已经由RPC框架构造好，可以通过preset注入进来避免拷贝
    template <typename T>
    inline void preset(T& value) noexcept;
    inline bool has_preset_value() const noexcept;

    // 【Graph::run】前后
    // 获得只读的value，发布者应该使用emit进行写操作
    template <typename T>
    inline const T* value() const noexcept;
    template <typename T>
    inline const T* cvalue() const noexcept;
    // 对基本类型进行隐式转换，相当于static_cast
    template <typename T>
    inline T as() const noexcept;

    // 【Graph::run】前后
    // data是否已经发布
    inline bool ready() const noexcept;
    // data是否为空
    inline bool empty() const noexcept;

    inline const ::std::string& name() const noexcept;

private:
    // 仅用于Graph中使用vector构建，藏起来避免使用者误用
    inline GraphData() = default;

    inline void name(const ::std::string& name) noexcept;
    inline void executer(GraphExecutor& executer) noexcept;
    inline void producer(GraphVertex& producer) noexcept;
    inline void on_emit(const OnEmitFunction& on_emit) noexcept;
    inline void add_successor(GraphDependency& successor) noexcept;
    inline void data_num(size_t num) noexcept;
    inline void vertex_num(size_t num) noexcept;
    inline GraphVertex* producer() noexcept;
    inline const GraphVertex* producer() const noexcept;

    inline int32_t error_code() const noexcept;
    // 重置状态，但是保留data空间
    inline void reset() noexcept;
    // 获取发布权，竞争下只有第一次返回true，此时进入非空状态
    // 只有获得发布权后才可以进一步写操作value，以及release
    // 如果要发布空value，需要主动标记empty(true)
    // 整套操作封装成Commiter呈现
    inline bool acquire() noexcept;
    // 发布data，递减等待data的closure的计数
    // 之后依次通知successor
    void release() noexcept;
    // 获取可写value指针，通过GraphDependency同名函数间接提供
    // 写入者要通过使用commiter来遵守流程
    template <typename T>
    inline T* mutable_value() noexcept;
    // 获取除了可写value指针，确保底层存储为T类型的非引用对象
    // 如果不是，则重新使用T()创建对象并置入底层容器
    // 写入者要通过使用commiter来遵守流程
    template <typename T, typename ::std::enable_if<!::std::is_move_constructible<T>::value, int32_t>::type = 0>
    inline T* certain_type_non_reference_mutable_value() noexcept;
    template <typename T, typename ::std::enable_if<::std::is_move_constructible<T>::value, int32_t>::type = 0>
    inline T* certain_type_non_reference_mutable_value() noexcept;
    template <typename T>
    inline void ref(T& value) noexcept;
    template <typename T>
    inline void cref(const T& value) noexcept;
    // 标记data为空，不会清除实际数据
    inline void empty(bool empty) noexcept;
    // 绑定一个closure
    // 尚未ready时，增加closure的等待数
    // 并记录closure，之后返回true，当data ready时进行通知
    // 如果data已经ready，则直接返回false
    inline bool bind(ClosureContext& closure) noexcept;

    // 由GraphDependency使用
    // 调用后进入不可变依赖状态
    // true：调用前无依赖或者不可变依赖
    // false：调用前可变依赖
    inline bool acquire_immutable_depend() noexcept;
    // 由GraphDependency使用
    // 调用后进入可变依赖状态
    // true：调用前无依赖
    // false：调用前可变依赖或者不可变依赖
    inline bool acquire_mutable_depend() noexcept;
    // 激活标记，只用于推导阶段去重，没有使用原子操作
    // 因此，多线程下不保证只会active一次，只用于初筛
    inline bool mark_active() noexcept;
    // 以当前data为根，按照依赖路径递归激活
    // 最终新增需要发起执行的vertex会收集在runnable_vertexes中
    int32_t recursive_activate(VertexStack& runnable_vertexes, ClosureContext* closure) noexcept;
    // 尝试触发到激活状态，需要激活的会将自身加入activating_data
    inline void trigger(DataStack& activating_data) noexcept;
    // 激活当前data，如果producer依赖就绪，则加入runnable_vertexes
    // 如果producer还有依赖，则进入激活状态，并将进一步需要激活的节点
    // 加入activating_data
    inline int32_t activate(DataStack& activating_data,
        VertexStack& runnable_vertexes, ClosureContext* closure) noexcept;

    static ClosureContext* SEALED_CLOSURE;

    // 静态信息
    ::std::string _name;
    GraphVertex* _producer {nullptr};
    ::std::vector<GraphDependency*> _successors;
    GraphExecutor* _executer {nullptr};
    size_t _data_num {0};
    size_t _vertex_num {0};
    const Any::Id* _declare_type {nullptr};
    bool _error_code {0};

    // 数据信息
    ::std::atomic<bool> _acquired {false};
    Any _data;
    bool _empty {true};
    bool _has_preset_value {false};

    // 推导信息
    bool _active {false};
    ::std::atomic<ClosureContext*> _closure {nullptr};
    ::std::atomic<int32_t> _depend_state {0};
    //数据发布前调用
    const OnEmitFunction* _on_emit{nullptr};

    template <typename T>
    friend class Commiter;
    friend void ::std::_Construct<GraphData>(GraphData*);
    friend ::std::ostream& operator<<(::std::ostream&, const GraphData&);
    friend class Graph;
    friend class GraphBuilder;
    friend class ClosureContext;
    friend class GraphDependency;
    friend class GraphVertexBuilder;
    friend class GraphDependencyBuilder;
};

template <typename T>
class ChannelPublisher {
private:
    typedef ::joewu::feed::mlarch::babylon::ConcurrentTransientQueue<T> Queue;

public:
    // Queue的轻量级包装，可以默认构造和拷贝移动
    inline ChannelPublisher() noexcept = default;
    inline ChannelPublisher(ChannelPublisher&& other) noexcept :
            _queue(other._queue) {
        other._queue = nullptr;
    }
    inline ChannelPublisher(const ChannelPublisher&) noexcept = delete;
    inline ChannelPublisher& operator=(ChannelPublisher&& other) noexcept {
        ::std::swap(_queue, other._queue);
        return *this;
    }
    inline ChannelPublisher& operator=(const ChannelPublisher&) noexcept = delete;

    // 仿Queue指针，用于使用Queue接口进行数据发布
    inline Queue* get() noexcept {
        return _queue;
    }
    inline Queue* operator->() noexcept {
        return get();
    }
    inline Queue& operator*() noexcept {
        return *get();
    }

    // 析构时自动停止
    inline ~ChannelPublisher() {
        if (_queue != nullptr) {
            _queue->close();
            _queue = nullptr;
        }
    }

private:
    inline ChannelPublisher(Queue& queue) noexcept : _queue(&queue) {}

    Queue* _queue {nullptr};

    friend class OutputChannel<T>;
};

template <typename T>
class OutputChannel {
private:
    typedef ::joewu::feed::mlarch::babylon::ConcurrentTransientQueue<T> Queue;

public:
    // GraphData的轻量级包装，可以默认构造和拷贝移动
    inline OutputChannel() noexcept = default;
    inline OutputChannel(OutputChannel&&) noexcept = default;
    inline OutputChannel(const OutputChannel&) noexcept = default;
    inline OutputChannel& operator=(OutputChannel&&) noexcept = default;
    inline OutputChannel& operator=(const OutputChannel&) noexcept = default;

    // 开启发布流，可以通过返回的发布器进一步完成流式发布
    // 返回的发布器在析构时自动关闭发布流
    inline ChannelPublisher<T> open() {
        auto committer = _data->emit<Queue>();
        auto* queue = committer.get();
        queue->clear();
        return ChannelPublisher<T>(*queue);
    }

    // 转发一个发布流，用于和其他框架打通
    // 转发后自行操作queue即可发布数据
    inline void forward(Queue& queue) {
        auto committer = _data->emit<Queue>();
        committer.ref(queue);
    }

    inline void forward(const Queue& queue) {
        auto committer = _data->emit<Queue>();
        committer.cref(queue);
    }

    inline operator bool() const noexcept {
        return _data != nullptr;
    }

private:
    inline OutputChannel(GraphData& data) noexcept : _data(&data) {
        _data->declare_type<Queue>();
    }

    GraphData* _data {nullptr};

    friend class GraphData;
};

template <typename T>
inline OutputChannel<T> GraphData::declare_channel() noexcept {
    return OutputChannel<T>(*this);
}

} // graph
} // feed
} // joewu
#endif //joewu_HAOKAN_REC_GRAPH_ENGINE_GRAPH_DATA_H

#include <joewu/graph/engine/data.hpp>
