#include <joewu/graph/builtin/expression.h>
#include <joewu/graph/builtin/select.h>
#include <joewu/graph/builtin/const.h>
#include <joewu/graph/builtin/alias.h>
#include <joewu/graph/engine/builder.h>

#include <base/logging.h>

#include <boost/spirit/include/qi.hpp>
#include <boost/fusion/include/boost_tuple.hpp>
#include <boost/preprocessor/facilities/empty.hpp>
#include <boost/preprocessor/control/if.hpp>

namespace joewu {
namespace feed {
namespace graph {
namespace builtin {

namespace expression {

using ::boost::spirit::qi::rule;
using ::boost::spirit::qi::space_type;
using ::boost::spirit::qi::grammar;
using ::boost::spirit::qi::raw;
using ::boost::spirit::qi::as_string;
using ::boost::spirit::qi::lit;
using ::boost::spirit::qi::char_;
using ::boost::spirit::qi::string;
using ::boost::spirit::qi::bool_;
using ::boost::spirit::qi::long_;
using ::boost::spirit::qi::real_parser;
using ::boost::spirit::qi::strict_real_policies;
using ::boost::spirit::qi::alpha;
using ::boost::spirit::qi::alnum;
using ::joewu::feed::mlarch::babylon::Any;

class Operator {
public:
    typedef ::std::tuple<size_t, size_t> ValueIndex;

    // 一元运算符
    inline Operator(const ValueIndex& result, const ValueIndex& operand) noexcept:
        _result(result), _operands({operand}) {}

    // 二元运算符
    inline Operator(const ValueIndex& result,
        const ValueIndex& left, const ValueIndex& right) noexcept:
        _result(result), _operands({left, right}) {}

    virtual ~Operator() noexcept {}

    // 抽象的运算函数
    inline int32_t evaluate(::std::vector<Any>& variables,
        const ::std::vector<Any>& constants) noexcept {
        switch (_operands.size()) {
        case 1: {
                    // 一元运算符result = compute(operand)
                    Any* result = get_result(variables);
                    const Any* operand = get_operand(0, variables, constants);
                    if (operand != nullptr && result != nullptr) {
                        return compute(*result, *operand);
                    }
                    break;
                }
        case 2: {
                    // 二元运算符result = compute(loperand, roperand)
                    Any* result = get_result(variables);
                    const Any* loperand = get_operand(0, variables, constants);
                    const Any* roperand = get_operand(1, variables, constants);
                    if (result != nullptr && loperand != nullptr && roperand != nullptr) {
                        return compute(*result, *loperand, *roperand);
                    }
                    break;
                }
        default: {
                     LOG(WARNING) << "do not support operator more than two operand";
                     return -1;
                 }
        }
        LOG(WARNING) << "insufficient operand for operator";
        return -2;
    }

protected:
    // 一元运算符实现
    virtual int32_t compute(Any& result, const Any& operand) noexcept = 0;
    // 二元运算符实现
    virtual int32_t compute(Any& result, const Any& loperand, const Any& roperand) noexcept = 0;
    // 获取二元运算符的计算类型，运算前会都转成同样的计算类型
    inline Any::Type get_caculation_type(const Any& loperand, const Any& roperand) noexcept {
        const ::std::tuple<size_t, Any::Type>& llevel_and_type = TYPE_LEVEL[static_cast<size_t>(loperand.type())];
        const ::std::tuple<size_t, Any::Type>& rlevel_and_type = TYPE_LEVEL[static_cast<size_t>(roperand.type())];
        if (::std::get<0>(llevel_and_type) >= ::std::get<0>(rlevel_and_type)) {
            return ::std::get<1>(llevel_and_type);
        } else {
            return ::std::get<1>(rlevel_and_type);
        }
    }

private:
    inline const Any* get_operand(size_t index, const ::std::vector<Any>& variables,
        const ::std::vector<Any>& constants) noexcept {
        switch (::std::get<0>(_operands[index])) {
        case 0: {
                    auto pos = ::std::get<1>(_operands[index]);
                    if (pos < variables.size()) {
                        return &variables[pos];
                    }
                    LOG(FATAL) << "error variable index";
                    break;
                }
        case 1: {
                    auto pos = ::std::get<1>(_operands[index]);
                    if (pos < constants.size()) {
                        return &constants[pos];
                    }
                    LOG(FATAL) << "error constant index";
                    break;
                }
        default: {
                     LOG(FATAL) << "error index type";
                 }
        }
        return nullptr;
    }

    inline Any* get_result(::std::vector<Any>& variables) noexcept {
        switch (::std::get<0>(_result)) {
        case 0: {
                    auto pos = ::std::get<1>(_result);
                    if (pos < variables.size()) {
                        return &variables[pos];
                    }
                    LOG(FATAL) << "error index";
                    break;
                }
        default: {
                     LOG(FATAL) << "error index type";
                 }
        }
        return nullptr;
    }

    inline static ::std::vector<::std::tuple<size_t, Any::Type>> generate_type_level() {
        ::std::vector<::std::tuple<size_t, Any::Type>> result(
            static_cast<size_t>(Any::Type::FLOAT) + 1);
        result[size_t(Any::Type::BOOLEAN)] = ::std::make_tuple(1, Any::Type::BOOLEAN);
        result[size_t(Any::Type::INT8)] = ::std::make_tuple(2, Any::Type::INT8);
        result[size_t(Any::Type::UINT8)] = ::std::make_tuple(3, Any::Type::UINT8);
        result[size_t(Any::Type::INT16)] = ::std::make_tuple(4, Any::Type::INT16);
        result[size_t(Any::Type::UINT16)] = ::std::make_tuple(5, Any::Type::UINT16);
        result[size_t(Any::Type::INT32)] = ::std::make_tuple(6, Any::Type::INT32);
        result[size_t(Any::Type::UINT32)] = ::std::make_tuple(7, Any::Type::UINT32);
        result[size_t(Any::Type::INT64)] = ::std::make_tuple(8, Any::Type::INT64);
        result[size_t(Any::Type::UINT64)] = ::std::make_tuple(9, Any::Type::UINT64);
        result[size_t(Any::Type::FLOAT)] = ::std::make_tuple(10, Any::Type::FLOAT);
        result[size_t(Any::Type::DOUBLE)] = ::std::make_tuple(11, Any::Type::DOUBLE);
        result[size_t(Any::Type::INSTANCE)] = ::std::make_tuple(12, Any::Type::INSTANCE);
        return result;
    }

    ValueIndex _result;
    ::std::vector<ValueIndex> _operands;
    static ::std::vector<::std::tuple<size_t, Any::Type>> TYPE_LEVEL;
};
::std::vector<::std::tuple<size_t, Any::Type>> Operator::TYPE_LEVEL = Operator::generate_type_level();

// 一元运算符基类，自带注册表
class UnaryOperator : public Operator {
public:
    using Operator::Operator;
    UnaryOperator(const ValueIndex& result,
        const ValueIndex& loperand, const ValueIndex& roperand) noexcept = delete;

    static ::std::unique_ptr<Operator> create(const ::std::string& op,
        const Operator::ValueIndex& result, const Operator::ValueIndex& operand) noexcept {
        auto it = _s_registry.find(op);
        if (it != _s_registry.end()) {
            return it->second(result, operand);
        }
        return ::std::unique_ptr<Operator>();
    }

    template <typename T>
    static void register_operator(const ::std::string& op) noexcept {
        _s_registry[op] = [] (const ValueIndex& result, const ValueIndex& operand) {
            return ::std::unique_ptr<Operator>(new T(result, operand));
        };
    }

protected:
    template <typename T>
    struct Register {
        Register(const ::std::string& op) noexcept {
            UnaryOperator::register_operator<T>(op);
        }
    };

    virtual int32_t compute(Any&, const Any&, const Any&) noexcept override {
        LOG(WARNING) << "UnaryOperator do not support two operand";
        return -1;
    }

private:
    static ::std::unordered_map<::std::string, ::std::function<::std::unique_ptr<Operator>(
        const ValueIndex&, const ValueIndex&)>> _s_registry;
};
::std::unordered_map<::std::string, ::std::function<::std::unique_ptr<Operator>(
        const Operator::ValueIndex&, const Operator::ValueIndex&)>> UnaryOperator::_s_registry;

// 二元运算符基类，自带注册表
class BinaryOperator : public Operator {
public:
    using Operator::Operator;
    BinaryOperator(const ValueIndex& result, const ValueIndex& operand) noexcept = delete;

    static ::std::unique_ptr<Operator> create(const ::std::string& op,
        const ValueIndex& result, const ValueIndex& loperand, const ValueIndex& roperand) noexcept {
        auto it = _s_registry.find(op);
        if (it != _s_registry.end()) {
            return it->second(result, loperand, roperand);
        }
        return ::std::unique_ptr<Operator>();
    }

    template <typename T>
    static void register_operator(const ::std::string& op) noexcept {
        _s_registry[op] = [] (const ValueIndex& result,
            const ValueIndex& loperand, const ValueIndex& roperand) {
            return ::std::unique_ptr<Operator>(new T(result, loperand, roperand));
        };
    }

protected:
    template <typename T>
    struct Register {
        Register(const ::std::string& op) noexcept {
            BinaryOperator::register_operator<T>(op);
        }
    };

    virtual int32_t compute(Any&, const Any&) noexcept override {
        LOG(WARNING) << "BinaryOperator do not support only one operand";
        return -1;
    }

private:
    static ::std::unordered_map<::std::string, ::std::function<::std::unique_ptr<Operator>(
        const ValueIndex&, const ValueIndex&, const ValueIndex&)>> _s_registry;
};
::std::unordered_map<::std::string, ::std::function<::std::unique_ptr<Operator>(
    const Operator::ValueIndex&, const Operator::ValueIndex&,
    const Operator::ValueIndex&)>> BinaryOperator::_s_registry;

// 定义并注册一个一元运算符
#define __BABYLON_DEFINE_UNARY_OPERATOR(name, op) \
class name##Operator : public UnaryOperator { \
public: \
    using UnaryOperator::UnaryOperator; \
protected: \
    using UnaryOperator::compute; \
    virtual int32_t compute(Any& result, const Any& operand) noexcept override { \
        switch (operand.type()) { \
        case Any::Type::BOOLEAN: \
            result = op operand.as<bool>(); \
            break; \
        case Any::Type::INT8: \
            result = op operand.as<int8_t>(); \
            break; \
        case Any::Type::UINT8: \
            result = op operand.as<uint8_t>(); \
            break; \
        case Any::Type::INT16: \
            result = op operand.as<int16_t>(); \
            break; \
        case Any::Type::UINT16: \
            result = op operand.as<uint16_t>(); \
            break; \
        case Any::Type::INT32: \
            result = op operand.as<int32_t>(); \
            break; \
        case Any::Type::UINT32: \
            result = op operand.as<uint32_t>(); \
            break; \
        case Any::Type::INT64: \
            result = op operand.as<int64_t>(); \
            break; \
        case Any::Type::UINT64: \
            result = op operand.as<uint64_t>(); \
            break; \
        case Any::Type::FLOAT: \
            result = op operand.as<float>(); \
            break; \
        case Any::Type::DOUBLE: \
            result = op operand.as<double>(); \
            break; \
        default: \
            const auto value = operand.get<::std::string>(); \
            if (likely(value != nullptr)) { \
                result = op !value->empty(); \
            } else { \
                result = op (bool) operand; \
            } \
            break; \
        } \
        return 0; \
    } \
    static Register<name##Operator> _s_register; \
}; \
UnaryOperator::Register<name##Operator> name##Operator::_s_register(#op);

// 定义并注册一个二元运算符
#define __BABYLON_DEFINE_BINARY_OPERATOR(name_prefix, op, support_string) \
class name_prefix##Operator : public BinaryOperator { \
public: \
    using BinaryOperator::BinaryOperator; \
protected: \
    using BinaryOperator::compute; \
    virtual int32_t compute(Any& result, \
        const Any& loperand, const Any& roperand) noexcept override { \
        switch (get_caculation_type(loperand, roperand)) { \
        case Any::Type::BOOLEAN: \
            result = loperand.as<bool>() op roperand.as<bool>(); \
            break; \
        case Any::Type::INT8: \
            result = loperand.as<int8_t>() op roperand.as<int8_t>(); \
            break; \
        case Any::Type::UINT8: \
            result = loperand.as<uint8_t>() op roperand.as<uint8_t>(); \
            break; \
        case Any::Type::INT16: \
            result = loperand.as<int16_t>() op roperand.as<int16_t>(); \
            break; \
        case Any::Type::UINT16: \
            result = loperand.as<uint16_t>() op roperand.as<uint16_t>(); \
            break; \
        case Any::Type::INT32: \
            result = loperand.as<int32_t>() op roperand.as<int32_t>(); \
            break; \
        case Any::Type::UINT32: \
            result = loperand.as<uint32_t>() op roperand.as<uint32_t>(); \
            break; \
        case Any::Type::INT64: \
            result = loperand.as<int64_t>() op roperand.as<int64_t>(); \
            break; \
        case Any::Type::UINT64: \
            result = loperand.as<uint64_t>() op roperand.as<uint64_t>(); \
            break; \
        case Any::Type::FLOAT: \
            result = loperand.as<float>() op roperand.as<float>(); \
            break; \
        case Any::Type::DOUBLE: \
            result = loperand.as<double>() op roperand.as<double>(); \
            break; \
        default: \
            BOOST_PP_IF(support_string, \
            const auto lvalue = loperand.get<::std::string>(); \
            const auto rvalue = roperand.get<::std::string>(); \
            if (likely(lvalue != nullptr && rvalue != nullptr)) { \
                result = *lvalue op *rvalue; \
                return 0; \
            }, BOOST_PP_EMPTY()); \
            LOG(WARNING) << "can not apply op " << #op << " on " \
                << loperand.instance_type().name << " and " \
                << roperand.instance_type().name; \
            return -1; \
        } \
        return 0; \
    } \
    static Register<name_prefix##Operator> _s_register; \
}; \
BinaryOperator::Register<name_prefix##Operator> name_prefix##Operator::_s_register(#op);

// 定义这些运算符
__BABYLON_DEFINE_UNARY_OPERATOR(Not, !);
__BABYLON_DEFINE_UNARY_OPERATOR(Neg, -);
__BABYLON_DEFINE_BINARY_OPERATOR(Add, +, 1);
__BABYLON_DEFINE_BINARY_OPERATOR(Sub, -, 0);
__BABYLON_DEFINE_BINARY_OPERATOR(Mul, *, 0);
__BABYLON_DEFINE_BINARY_OPERATOR(Div, /, 0);
__BABYLON_DEFINE_BINARY_OPERATOR(Gt, >, 1);
__BABYLON_DEFINE_BINARY_OPERATOR(Ge, >=, 1);
__BABYLON_DEFINE_BINARY_OPERATOR(Lt, <, 1);
__BABYLON_DEFINE_BINARY_OPERATOR(Le, <=, 1);
__BABYLON_DEFINE_BINARY_OPERATOR(Eq, ==, 1);
__BABYLON_DEFINE_BINARY_OPERATOR(Ne, !=, 1);
__BABYLON_DEFINE_BINARY_OPERATOR(And, &&, 0);
__BABYLON_DEFINE_BINARY_OPERATOR(Or, ||, 0);

#undef __BABYLON_DEFINE_UNARY_OPERATOR
#undef __BABYLON_DEFINE_BINARY_OPERATOR

// 每个实例需要有不同的中间数据，存在context中
struct Context {
    ::std::vector<Any> variables;
};

// 节点配置数据
struct Option {
    Option() = default;
    Option(const Option&) = delete;
    Option(Option&&) = default;
    // 执行表达式所需的变量数目
    // 用于存放从dependency读取到的值，以及中间计算结果
    size_t variable_num {0};
    // 执行表达式所需要的常量
    ::std::vector<Any> constants;
    // 依赖到变量的对应关系
    // variables[n] = dependencies[variable_indexes[n]]
    ::std::vector<size_t> variable_index_for_dependency;
    // 变量到输出的对应关系
    // emit = variables[variable_index_for_emit]
    size_t variable_index_for_emit {0};
    // 进行每一步表达式计算的基础运算
    ::std::vector<::std::unique_ptr<Operator>> operators;
};

static struct QuotedString : public grammar<const char*, ::std::string()> {
    inline QuotedString() noexcept : base_type(start, "quoted_string") {
        start = '"' >> *(escaped_slash | escaped_quote | common_char) >> '"';
        escaped_slash = '\\' >> char_('\\');
        escaped_quote = '\\' >> char_('"');
        common_char = char_ - '\\' - '"';
    }

    rule<const char*, ::std::string()> start;
    rule<const char*, char()> escaped_slash;
    rule<const char*, char()> escaped_quote;
    rule<const char*, char()> common_char;
} quoted_string;

struct Variable : public grammar<const char*, ::std::string()> {
    inline Variable() noexcept : base_type(start, "variable") {
        start = (alpha | char_('_')) >> *(alnum | char_('_'));
    }

    rule<const char*, ::std::string()> start;
};

struct Expression : public grammar<const char*, space_type> {
    inline Expression(size_t level = 0) noexcept : base_type(start, "expression") {
        start = expression[level].alias();
        expression[0] = expression[13].alias();
        expression[13] = expression[12] >> -('?' >> expression[12] >> ':' >> expression[12]);
        expression[12] = expression[11] >> *("||" >> expression[11]);
        expression[11] = expression[7] >> *("&&" >> expression[7]);
        expression[7] = expression[6] >> *((lit("==") | "!=") >> expression[6]);
        expression[6] = expression[4] >> *((lit(">=") | '>' | "<=" | '<') >> expression[4]);
        expression[4] = expression[3] >> *((lit('+') | '-') >> expression[3]);
        expression[3] = expression[2] >> *((lit('*') | '/') >> expression[2]);
        expression[2] = -(lit('!') | '-') >> expression[1];
        expression[1] = bool_ | real_parser<double, strict_real_policies<double>>()
            | long_ | variable | quoted_string | ('(' >> expression[0] >> ')');
    }

    rule<const char*, space_type> start;
    rule<const char*, space_type> expression[14];
    Variable variable;
};

struct ConditionalExpression : public grammar<const char*, space_type,
        ::std::tuple<::std::string, ::std::string, ::std::string>()> {
    inline ConditionalExpression() noexcept : base_type(start, "conditional_expression") {
        start = conditional_expression | ('(' >> start >> ')');
        conditional_expression = (raw[condition_expression_operand] >> '?' 
                >> raw[condition_expression_operand] >> ':' >> raw[condition_expression_operand]) [
            on_conditional_expression];
    }

    inline static void on_conditional_expression(
            ::boost::fusion::vector3<
                ::boost::iterator_range<const char*>,
                ::boost::iterator_range<const char*>,
                ::boost::iterator_range<const char*>>& args,
            ::boost::spirit::context<
                ::boost::fusion::cons<
                    ::std::tuple<::std::string, ::std::string, ::std::string>&,
                        ::boost::fusion::nil_>,
                ::boost::fusion::vector0<>>& context) noexcept {
        auto& result = ::boost::fusion::at_c<0>(context.attributes);
        ::std::get<0>(result).assign(::boost::fusion::at_c<0>(args).begin(),
            ::boost::fusion::at_c<0>(args).end());
        ::std::get<1>(result).assign(::boost::fusion::at_c<1>(args).begin(),
            ::boost::fusion::at_c<1>(args).end());
        ::std::get<2>(result).assign(::boost::fusion::at_c<2>(args).begin(),
            ::boost::fusion::at_c<2>(args).end());
    }

    rule<const char*, space_type,
        ::std::tuple<::std::string, ::std::string, ::std::string>()> start;
    rule<const char*, space_type,
        ::std::tuple<::std::string, ::std::string, ::std::string>()> conditional_expression;
    Expression condition_expression_operand {12};
};

struct NonConditionalExpression : public grammar<const char*, space_type,
        ::std::tuple<size_t, size_t>()> {
    inline NonConditionalExpression(expression::Option& option,
            ::std::unordered_map<::std::string, size_t>& variable_indexes) noexcept :
            base_type(start, "non_conditional_expression"),
            option(option),
            variable_indexes(variable_indexes) {
        start = expression[0].alias();
        expression[0] = expression[12].alias();
        expression[12] = (expression[11] >> *(string("||") >> expression[11]))[
            ::std::bind(&NonConditionalExpression::on_binary_operator, this,
                ::std::placeholders::_1, ::std::placeholders::_2, ::std::placeholders::_3)];
        expression[11] = (expression[7] >> *(string("&&") >> expression[7]))[
            ::std::bind(&NonConditionalExpression::on_binary_operator, this,
                ::std::placeholders::_1, ::std::placeholders::_2, ::std::placeholders::_3)];
        expression[7] = (expression[6] >> *((string("==") | string("!=")) >> expression[6]))[
            ::std::bind(&NonConditionalExpression::on_binary_operator, this,
                ::std::placeholders::_1, ::std::placeholders::_2, ::std::placeholders::_3)];
        expression[6] = (expression[4] >>
            *((string(">=") | string(">") | string("<=") | string("<")) >> expression[4]))[
            ::std::bind(&NonConditionalExpression::on_binary_operator, this,
                ::std::placeholders::_1, ::std::placeholders::_2, ::std::placeholders::_3)];
        expression[4] = (expression[3] >> *((string("+") | string("-")) >> expression[3]))[
            ::std::bind(&NonConditionalExpression::on_binary_operator, this,
                ::std::placeholders::_1, ::std::placeholders::_2, ::std::placeholders::_3)];
        expression[3] = (expression[2] >> *((string("*") | string("/")) >> expression[2]))[
            ::std::bind(&NonConditionalExpression::on_binary_operator, this,
                ::std::placeholders::_1, ::std::placeholders::_2, ::std::placeholders::_3)];
        expression[2] = (-(string("!") | string("-")) >> expression[1])[::std::bind(
            &NonConditionalExpression::on_unary_operator, this,
            ::std::placeholders::_1, ::std::placeholders::_2, ::std::placeholders::_3)];
        expression[1] = const_bool | const_double | const_long | captured_variable | const_quoted_string
            | conditional_expression_as_variable | ('(' >> expression[0] >> ')');
        const_bool = bool_[::std::bind(&NonConditionalExpression::on_bool, this,
            ::std::placeholders::_1, ::std::placeholders::_2)];
        const_double = real_parser<double, strict_real_policies<double>>()[::std::bind(
            &NonConditionalExpression::on_double, this,
            ::std::placeholders::_1, ::std::placeholders::_2)];
        const_long = long_[::std::bind(&NonConditionalExpression::on_long, this,
            ::std::placeholders::_1, ::std::placeholders::_2)];
        const_quoted_string = quoted_string[::std::bind(&NonConditionalExpression::on_string, this,
            ::std::placeholders::_1, ::std::placeholders::_2)];
        captured_variable = variable[::std::bind(
            &NonConditionalExpression::on_variable, this,
            ::std::placeholders::_1, ::std::placeholders::_2)];
        conditional_expression_as_variable = as_string[raw[conditional_expression]][::std::bind(
            &NonConditionalExpression::on_variable, this,
            ::std::placeholders::_1, ::std::placeholders::_2)];
    }

    inline void on_bool(bool& value, ::boost::spirit::context<
            ::boost::fusion::cons<
                ::std::tuple<size_t, size_t>&,
                ::boost::fusion::nil_>,
            ::boost::fusion::vector0<>>& context) noexcept {
        LOG(DEBUG) << "val[" << option.constants.size() << "] = " << (value ? "true" : "false");
        ::boost::fusion::at_c<0>(context.attributes) =
            ::std::make_tuple(1, option.constants.size());
        option.constants.emplace_back(value);
    }

    inline void on_double(double& value, ::boost::spirit::context<
            ::boost::fusion::cons<
                ::std::tuple<size_t, size_t>&,
                ::boost::fusion::nil_>,
            ::boost::fusion::vector0<>>& context) noexcept {
        LOG(DEBUG) << "val[" << option.constants.size() << "] = " << value;
        ::boost::fusion::at_c<0>(context.attributes) =
            ::std::make_tuple(1, option.constants.size());
        option.constants.emplace_back(value);
    }

    inline void on_long(int64_t& value, ::boost::spirit::context<
            ::boost::fusion::cons<
                ::std::tuple<size_t, size_t>&,
                ::boost::fusion::nil_>,
            ::boost::fusion::vector0<>>& context) noexcept {
        LOG(DEBUG) << "val[" << option.constants.size() << "] = " << value;
        ::boost::fusion::at_c<0>(context.attributes) =
            ::std::make_tuple(1, option.constants.size());
        option.constants.emplace_back(value);
    }

    inline void on_string(::std::string& value, ::boost::spirit::context<
            ::boost::fusion::cons<
                ::std::tuple<size_t, size_t>&,
                ::boost::fusion::nil_>,
            ::boost::fusion::vector0<>>& context) noexcept {
        LOG(DEBUG) << "val[" << option.constants.size() << "] = " << value;
        ::boost::fusion::at_c<0>(context.attributes) =
            ::std::make_tuple(1, option.constants.size());
        option.constants.emplace_back(value);
    }

    inline void on_variable(::std::string& value, ::boost::spirit::context<
            ::boost::fusion::cons<
                ::std::tuple<size_t, size_t>&,
                ::boost::fusion::nil_>,
            ::boost::fusion::vector0<>>& context) noexcept {
        auto result = variable_indexes.emplace(value, option.variable_num);
        if (result.second) {
            LOG(DEBUG) << "var[" << option.variable_num << "] = data[" << value << "]";
            ++option.variable_num;
        }
        ::boost::fusion::at_c<0>(context.attributes) =
            ::std::make_tuple(0, result.first->second);
    };

    inline void on_unary_operator(
            ::boost::fusion::vector2<
                ::boost::optional<::std::string>,
                ::std::tuple<size_t, size_t>>& args,
            ::boost::spirit::context<
                ::boost::fusion::cons<::std::tuple<size_t, size_t>&, boost::fusion::nil_>,
                ::boost::fusion::vector0<>>& context, bool& success) noexcept {
        auto& op = ::boost::fusion::at_c<0>(args);
        auto& operand = ::boost::fusion::at_c<1>(args);
        if (op) {
            LOG(DEBUG) << "var[" << option.variable_num << "] = " << *op << (::std::get<0>(operand) == 0 ? "var[" : "val[")
                << ::std::get<1>(operand) << "]";
            auto result = ::std::make_tuple(0, option.variable_num++);
            auto unary_operator = UnaryOperator::create(*op, result, operand);
            if (!unary_operator) {
                success = false;
                return;
            }
            option.operators.emplace_back(::std::move(unary_operator));
            ::boost::fusion::at_c<0>(context.attributes) = result;
        } else {
            ::boost::fusion::at_c<0>(context.attributes) = operand;
        }
    }

    inline void on_binary_operator(
            ::boost::fusion::vector2<
                ::std::tuple<size_t, size_t>,
                ::std::vector<
                    ::boost::fusion::vector2<
                        ::std::string,
                        ::std::tuple<size_t, size_t>>>>& args,
            ::boost::spirit::context<
                ::boost::fusion::cons<::std::tuple<size_t, size_t>&, boost::fusion::nil_>,
                ::boost::fusion::vector0<>>& context, bool& success) noexcept {
        auto& result = ::boost::fusion::at_c<0>(context.attributes);
        result = ::boost::fusion::at_c<0>(args);
        auto& op_and_operands = ::boost::fusion::at_c<1>(args);
        if (op_and_operands.empty()) {
            return;
        }
            
        for (auto& op_and_operand : op_and_operands) {
            auto& op = ::boost::fusion::at_c<0>(op_and_operand);
            auto& operand = ::boost::fusion::at_c<1>(op_and_operand);
            LOG(DEBUG) << "var[" << option.variable_num << "] = " << (::std::get<0>(result) == 0 ? "var[" : "val[")
                << ::std::get<1>(result) << "] " << op << " " << (::std::get<0>(operand) == 0 ? "var[" : "val[")
                << ::std::get<1>(operand) << "]";
            auto next_result = ::std::make_tuple(0, option.variable_num++);
            auto binary_operator = BinaryOperator::create(op, next_result, result, operand);
            if (!binary_operator) {
                success = false;
                return;
            }
            option.operators.emplace_back(::std::move(binary_operator));
            result = next_result;
        }
    }

    // 算子配置
    Option& option;

    // 变量符号表，对应关系为
    // variable[variable_indexes[name]] = data[name]
    ::std::unordered_map<::std::string, size_t>& variable_indexes;

    rule<const char*, space_type, ::std::tuple<size_t, size_t>()> start;
    // 按优先级区分的表达式
    rule<const char*, space_type, ::std::tuple<size_t, size_t>()> expression[14];
    // 匹配变量，变量对应GraphData的名字
    rule<const char*, ::std::tuple<size_t, size_t>()> captured_variable;
    // 条件表达式在这里不展开，被视为一个一般变量
    ConditionalExpression conditional_expression;
    rule<const char*, space_type, ::std::tuple<size_t, size_t>()> conditional_expression_as_variable;
    // 基础类型转换成分配槽位的常量
    rule<const char*, ::std::tuple<size_t, size_t>()> const_bool;
    rule<const char*, ::std::tuple<size_t, size_t>()> const_double;
    rule<const char*, ::std::tuple<size_t, size_t>()> const_long;
    rule<const char*, ::std::tuple<size_t, size_t>()> const_quoted_string;
    Variable variable;
};

} // expression

///////////////////////////////////////////////////////////////////////////////
// ExpressionProcessor begin
int32_t ExpressionProcessor::setup(GraphVertex& vertex) const noexcept {
    auto option = vertex.option<expression::Option>();
    auto context = vertex.context<expression::Context>();
    // 根据配置项，创建所需运算缓冲
    context->variables.resize(option->variable_num);
    // 校验依赖数目
    if (vertex.anonymous_dependency_size() != option->variable_index_for_dependency.size()) {
        LOG(WARNING) << "dependency num[" << vertex.anonymous_dependency_size()
            << "] != " << option->variable_index_for_dependency.size() << " for " << vertex;
        return -1;
    }
    // 校验输出数目
    if (vertex.anonymous_emit_size() != 1) {
        LOG(WARNING) << "emit num[" << vertex.anonymous_emit_size()
            << "] != 1 for " << vertex;
        return -1;
    }
    // 标记非并发执行
    vertex.trivial();
    return 0;
}

int32_t ExpressionProcessor::process(GraphVertex& vertex) noexcept {
    auto option = vertex.option<expression::Option>();
    auto context = vertex.context<expression::Context>();
    // 获取依赖，并填充初始变量
    for (size_t i = 0; i < vertex.anonymous_dependency_size(); ++i) {
        auto index = option->variable_index_for_dependency[i];
        auto dependency_value = vertex.anonymous_dependency(i)->value<Any>();
        if (unlikely(dependency_value == nullptr)) {
            if (vertex.anonymous_dependency(i)->ready()) {
                LOG(WARNING) << "dependency[" << i << "] empty for " << vertex;
            } else {
                LOG(WARNING) << "dependency[" << i << "] not ready for " << vertex;
            }
            return -1;
        }
        context->variables[index] = *dependency_value;
    }
    // 依次执行运算符
    for (auto& op : option->operators) {
        if (0 != op->evaluate(context->variables, option->constants)) {
            LOG(WARNING) << "evaluate failed for " << vertex;
            return -1;
        }
    }
    // 输出目标变量
    *vertex.anonymous_emit(0)->emit<Any>() = context->variables[option->variable_index_for_emit];
    return 0;
}

int32_t ExpressionProcessor::apply(GraphBuilder& builder) noexcept {
    ::std::unordered_set<::std::string> dependencies;
    ::std::unordered_set<::std::string> emits;
    for (const auto& vertex : builder.vertexes()) {
        for (const auto& dependency : vertex.anonymous_dependencies()) {
            dependencies.emplace(dependency.target());
            if (!dependency.condition().empty()) {
                dependencies.emplace(dependency.condition());
            }
        }
        for (const auto& dependency : vertex.named_dependencies()) {
            dependencies.emplace(dependency.target());
            if (!dependency.condition().empty()) {
                dependencies.emplace(dependency.condition());
            }
        }
        for (const auto& emit : vertex.anonymous_emits()) {
            emits.emplace(emit.target());
        }
        for (const auto& emit : vertex.named_emits()) {
            emits.emplace(emit.target());
        }
    }
    for (const auto& dependency : dependencies) {
        if (emits.count(dependency) == 0) {
            if (0 != expend_expression(builder, emits, dependency, dependency)) {
                LOG(WARNING) << "create ExpressionProcessor for [" << dependency << "] failed";
                return -1;
            }
        }
    }
    return 0;
}

int32_t ExpressionProcessor::apply(GraphBuilder& builder,
    const ::std::string& expression_string) noexcept {
    return apply(builder, expression_string, expression_string);
}

int32_t ExpressionProcessor::apply(GraphBuilder& builder,
    const ::std::string& result_name, const ::std::string& expression_string) noexcept {
    ::std::unordered_set<::std::string> emits;
    for (const auto& vertex : builder.vertexes()) {
        for (const auto& emit : vertex.anonymous_emits()) {
            emits.emplace(emit.target());
        }
        for (const auto& emit : vertex.named_emits()) {
            emits.emplace(emit.target());
        }
    }
    if (emits.count(result_name) > 0) {
        LOG(WARNING) << "result name for expression already exist " << result_name << " = " << expression_string;
        return -1;
    }
    if (0 != expend_expression(builder, emits, result_name, expression_string)) {
        LOG(WARNING) << "expend expression failed for " << result_name << " = " << expression_string;
        return -1;
    }
    return 0;
}

int32_t ExpressionProcessor::expend_expression(GraphBuilder& builder,
        ::std::unordered_set<::std::string>& emits,
        const ::std::string& result_name,
        const ::std::string& expression_string) noexcept {
    ::std::vector<::std::tuple<::std::string, ::std::string>> unsolved_expressions;
    unsolved_expressions.emplace_back(expression_string, result_name);
    while (!unsolved_expressions.empty()) {
        ::std::string unsolved_expression = ::std::get<0>(unsolved_expressions.back());
        ::std::string unsolved_name = ::std::get<1>(unsolved_expressions.back());
        unsolved_expressions.pop_back();
        if (emits.count(unsolved_name) > 0) {
            LOG(DEBUG) << "skip " << unsolved_name << " = " << unsolved_expression;
            continue;
        }

        auto ret =  expend_conditional_expression(builder, emits, unsolved_expressions,
            unsolved_name, unsolved_expression);
        if (ret < 0) {
            LOG(WARNING) << "solve expression failed {" << unsolved_name << "} = {" << unsolved_expression << "}";
            return -1;
        } else if (ret == 0) {
            LOG(DEBUG) << "solve conditional data[" << unsolved_name << "] = {" << unsolved_expression << "}";
            continue;
        }

        ret = expend_non_conditional_expression(builder, emits, unsolved_expressions,
            unsolved_name, unsolved_expression);
        if (ret != 0) {
            LOG(WARNING) << "solve expression failed {" << unsolved_name << "} = {" << unsolved_expression << "}";
            return -1;
        }
        LOG(DEBUG) << "solve non-conditional data[" << unsolved_name << "] = {" << unsolved_expression << "}";
    }
    return 0;
}

int32_t ExpressionProcessor::expend_conditional_expression(GraphBuilder& builder,
        ::std::unordered_set<::std::string>& emits,
        ::std::vector<::std::tuple<::std::string, ::std::string>>& unsolved_expressions,
        const ::std::string& result_name,
        const ::std::string& expression_string) noexcept {
    static expression::ConditionalExpression conditional_expression;
    static expression::Expression expression;

    ::std::tuple<::std::string, ::std::string, ::std::string> result;
    auto begin = expression_string.c_str();
    auto end = expression_string.c_str() + expression_string.size();
    auto ret = ::boost::spirit::qi::phrase_parse(
        begin,
        end,
        conditional_expression,
        ::boost::spirit::qi::space,
        result
    );

    // 不满足顶层是条件表达式的情况
    if (!ret || begin < end) {
        // 检测是不是一般表达式
        begin = expression_string.c_str();
        end = expression_string.c_str() + expression_string.size();
        ret = ::boost::spirit::qi::phrase_parse(
            begin,
            end,
            expression,
            ::boost::spirit::qi::space);
        // 也不是一般表达式则报错
        if (!ret || begin < end) {
            ::std::string pointer(begin - expression_string.c_str(), ' ');
            pointer += '^';
            LOG(WARNING) << "error exp = " << expression_string;
            LOG(WARNING) << "            " << pointer;
            return -1;
        }
        // 否则不做操作，并明确返回未做操作
        return 1;
    }

    const auto& condition_name = ::std::get<0>(result);
    const auto& true_name = ::std::get<1>(result);
    const auto& false_name = ::std::get<2>(result);

    SelectProcessor::apply(builder, result_name, condition_name, true_name, false_name);
    emits.emplace(result_name);
    LOG(DEBUG) << "expend SelectProcessor {" << result_name << "} = {" << condition_name << "} ? {"
        << true_name << "} : {" << false_name << "}";

    if (emits.count(condition_name) == 0) {
        unsolved_expressions.emplace_back(condition_name, condition_name);
        LOG(DEBUG) << "add to be solved expression {" << condition_name << "}";
    }
    if (emits.count(true_name) == 0) {
        unsolved_expressions.emplace_back(true_name, true_name);
        LOG(DEBUG) << "add to be solved expression {" << true_name << "}";
    }
    if (emits.count(false_name) == 0) {
        unsolved_expressions.emplace_back(false_name, false_name);
        LOG(DEBUG) << "add to be solved expression {" << false_name << "}";
    }
    return 0;
}

int32_t ExpressionProcessor::expend_non_conditional_expression(GraphBuilder& builder,
        ::std::unordered_set<::std::string>& emits,
        ::std::vector<::std::tuple<::std::string, ::std::string>>& unsolved_expressions,
        const ::std::string& result_name,
        const ::std::string& expression_string) noexcept {
    static ExpressionProcessor processor;

    // 算子配置
    expression::Option option;
    // 变量符号表，对应关系为
    // variable[variable_indexes[name]] = data[name]
    ::std::unordered_map<::std::string, size_t> variable_indexes;
    // 非条件表达式语法
    expression::NonConditionalExpression non_conditional_expression(option, variable_indexes);

    // 解析表达式
    auto begin = expression_string.c_str();
    auto end = expression_string.c_str() + expression_string.size();
    expression::Operator::ValueIndex result;
    auto ret = ::boost::spirit::qi::phrase_parse(
        begin,
        end,
        non_conditional_expression,
        ::boost::spirit::qi::space,
        result
    );

    // 完整解析才算成功
    if (!ret || begin < end) {
        ::std::string pointer(begin - expression_string.c_str(), ' ');
        pointer += '^';
        LOG(WARNING) << "error exp = " << expression_string;
        LOG(WARNING) << "            " << pointer;
        return -1;
    }

    // 单一常/变量生成专用算子解决
    if (option.operators.empty()) {
        auto iter = variable_indexes.begin();
        // 单一变量
        if (iter != variable_indexes.end()) {
            // 结果并不是变量本身，生成别名算子
            if (result_name != iter->first) {
                AliasProcessor::apply(builder, result_name, iter->first);
                emits.emplace(result_name);
                LOG(DEBUG) << "add AliasProcessor {" << result_name << "} = {"
                    << expression_string << "}";
            } // else 结果就是变量本身，无需处理
            return 0;
        // 单一常量
        } else if (likely(!option.constants.empty())) {
            // 生成常量算子
            ConstProcessor::apply(builder, result_name, ::std::move(option.constants[0]));
            emits.emplace(result_name);
            LOG(DEBUG) << "add ConstProcessor {" << result_name << "} = {"
                << expression_string << "}";
            return 0;
        }
        LOG(WARNING) << "expression without variable nor constant {" << result_name << "} = {"
        << expression_string << "} maybe a bug?";
        return -1;
    }

    // 创建节点，设置依赖，输出和配置
    option.variable_index_for_emit = ::std::get<1>(result);
    auto& vertex = builder.add_vertex(processor);
    vertex.name(std::string("ExpressionProcessor").append(std::to_string(++ExpressionProcessor::_g_idx)));
    for (const auto& pair : variable_indexes) {
        vertex.anonymous_depend().to(pair.first);
        option.variable_index_for_dependency.emplace_back(pair.second);
        if (emits.count(pair.first) == 0) {
            unsolved_expressions.emplace_back(pair.first, pair.first);
        }
    }
    vertex.anonymous_emit().to(result_name);
    vertex.option(::std::move(option));
    emits.emplace(result_name);

    LOG(DEBUG) << "add ExpressionProcessor " << noflush;
    if (result_name != expression_string) {
        LOG(DEBUG) << "data[" << result_name << "] = " << noflush;
    }
    LOG(DEBUG) << "{" << expression_string << "}";

    return 0;
}
std::atomic<size_t> ExpressionProcessor::_g_idx;
// ExpressionProcessor end
///////////////////////////////////////////////////////////////////////////////

} // builtin
} // graph
} // feed
} // joewu
