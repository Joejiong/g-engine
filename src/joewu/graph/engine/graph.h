#ifndef joewu_HAOKAN_REC_GRAPH_ENGINE_GRAPH_GRAPH_H 
#define joewu_HAOKAN_REC_GRAPH_ENGINE_GRAPH_GRAPH_H  

#include <vector>
#include <unordered_map>
#include <joewu/feed/mlarch/babylon/stack.h>
#include <joewu/graph/engine/closure.h>
#include <joewu/feed/mlarch/babylon/any.h>
#include <joewu/feed/mlarch/babylon/reusable/manager.h>

namespace joewu {
namespace feed {
namespace graph {
using ::joewu::feed::mlarch::babylon::Stack;
typedef ::joewu::feed::mlarch::babylon::Any Any;
using ::joewu::feed::mlarch::babylon::ReusableManager;
using ::joewu::feed::mlarch::babylon::ReusableAccessor;
class GraphLog;
class GraphData;
class GraphVertex;
class GraphExecutor;
class Graph {
public:
    inline size_t data_size() const noexcept;
    inline size_t vertex_size() const noexcept;
    // 清理执行状态，但是保留data空间
    void reset() noexcept;
    // 通过name找到GraphData
    // 用于直接向data赋值，或发起求值
    GraphData* find_data(const ::std::string& name) noexcept ;
    // 以指定的一系列GraphData为目的开始求值
    // 以指定的一系列GraphData为目的开始求值
    // 先检测这些data是否就绪，未就绪的找到producer，没有producer的报错退出
    // 检测producer的depends是否ready，以及对应的condition确定是否有效
    // 如果没有有效，且未ready的depends，将vertex指针记入临时数组，并通过辅助数组序号去重
    // 如果存在有效且未就绪的depends，记录waiting num，并继续检测这些data的producer
    // 检测完成后统一发起一轮run
    // 执行完成后如果没有fatal且data都已经就绪，则成功否则返回失败
    // 同一张graph可以选不同根节点执行多次，但不可重入
    // 可以支持先根据请求和小流量产出condition，之后根据condition形成真实依赖图
    template <typename ...D>
    inline Closure run(D... data) noexcept;
    Closure run(GraphData* data[], size_t size) noexcept;

    // 对Graph中每个GraphVertex执行一次func调用
    int func_each_vertex(std::function<int(GraphVertex&)> func) noexcept;
    
    //获取graph的context
    template <typename T>
    inline T* context() noexcept;

    template <typename T>
    inline T* mutable_context() noexcept;

    //graph级别内存复用，线程安全
    #ifdef GOOGLE_PROTOBUF_HAS_ARENAS
    template <typename T, typename... Args>
    inline ReusableAccessor<T> create(Args&&... args);
    #endif // GOOGLE_PROTOBUF_HAS_ARENAS

    #ifdef GOOGLE_PROTOBUF_HAS_ARENAS
    inline ReusableManager<::google::protobuf::Arena>& memory_manager() noexcept {
        return _arena_mem_manager;
    }
    #endif //GOOGLE_PROTOBUF_HAS_ARENAS
private:
    // 单测使用
    inline Graph() = default;
    inline Graph(const Graph&) = delete;
    Graph(GraphExecutor&, size_t,
        const ::std::unordered_map<::std::string, size_t>&) noexcept;
    ::std::vector<GraphVertex>& vertexes() noexcept;
    ::std::vector<GraphData>& data() noexcept;
    // 以求解data为目的，对图进行推导
    // 1、对graph中对data的产出有直接或者间接依赖的vertex和data做求解标记
    //    无依赖vertex和已经ready的data视为推导终止节点
    // 2、有非条件路径（存在一条从data出发的依赖路径，其中所有dependency都已经满足条件）可达做活跃标记
    // 3、将产出具有活跃标记data的vertex记录在runnable_vertexes当中
    int32_t activate(Stack<GraphVertex*>& runnable_vertexes,
        ::std::vector<GraphData*>& data) noexcept;

    GraphExecutor* _executor {nullptr};
    ::std::vector<GraphVertex> _vertexes;
    ::std::vector<GraphData> _data;
    ::std::unordered_map<::std::string, GraphData*> _data_by_name;
    //graph级别context，graph运行期间不能进行context内容修改
    Any _context;
    //graph级别context，graph运行期间可以进行对context内容修改
    Any _mutable_context;
    //graph级别内存复用，采用pb arenaf分配器
    #ifdef GOOGLE_PROTOBUF_HAS_ARENAS
    ReusableManager<::google::protobuf::Arena> _arena_mem_manager;
    #endif // GOOGLE_PROTOBUF_HAS_ARENAS
    friend class GraphBuilder;
    friend class GraphData;
    friend class GraphLog;
};

class GraphLog {
    public:
        inline GraphLog(Graph* graph) noexcept {
            if (graph != nullptr) {
                _vertexes_p = &graph->vertexes();
            }
        }  
        const ::std::vector<GraphVertex>* vertexes() const noexcept {
            return _vertexes_p;
        }

        friend ::std::ostream& operator<<(::std::ostream& out, const GraphLog& graph_log) noexcept;

    private:
        ::std::vector<GraphVertex>* _vertexes_p {nullptr};
};


}  // graph
}  // feed
}  // joewu
#endif // joewu_HAOKAN_REC_GRAPH_ENGINE_GRAPH_GRAPH_H

#include <joewu/graph/engine/graph.hpp>
