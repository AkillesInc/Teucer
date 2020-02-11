/*
 * File:   cpp_code_generator.hpp
 * Author: sebastian
 *
 * Created on 19. Juli 2015, 18:54
 */

#ifndef CPP_CODE_GENERATOR_HPP
#define	CPP_CODE_GENERATOR_HPP

#include <ostream>
#include <sstream>
#include <list>
#include <query_compilation/code_generator.hpp>
#include <core/table.hpp>
#include <core/attribute_reference.hpp>
#include <query_compilation/pipeline.hpp>
#include <dlfcn.h>
#include <boost/filesystem.hpp>

#include <query_compilation/predicate_expression.hpp>
#include <query_compilation/aggregate_specification.hpp>

namespace CoGaDB {

    const std::string toCPPType(const AttributeType& attr);

    const std::string getVarName(const AttributeReference& attr_ref);

    const std::string getTupleIDVarName(const TablePtr table, uint32_t version);

    const std::string getTupleIDVarName(const AttributeReference& attr_ref);

    const std::string getInputArrayVarName(const AttributeReference& attr_ref);

    const std::string getResultArrayVarName(const AttributeReference& attr_ref);

    const std::string getHashTableVarName(const AttributeReference& attr_ref);

    const std::string getTableVarName(const AttributeReference& attr_ref);

    const std::string getTableVarName(const TablePtr table, uint32_t version);

    const std::string getCPPExpression(LogicalOperation log_op);

    const std::string getCPPExpression(ValueComparator x);

    const std::string toCPPExpression(const boost::any constant);

    const std::string getElementAccessExpression(const AttributeReference& attr_ref);

    const PipelinePtr compileFile(const std::string& path);

    class CPPCodeGenerator : public CodeGenerator {
    public:
        CPPCodeGenerator(const ProjectionParam& param,
                const TablePtr table,
                uint32_t version = 1);

        CPPCodeGenerator(const ProjectionParam& param);

        void init();

        virtual bool consumeSelection_impl(const PredicateExpressionPtr pred_expr);

        virtual bool consumeBuildHashTable_impl(const AttributeReference& attr);

        virtual bool consumeProbeHashTable_impl(const AttributeReference& hash_table_attr,
            const AttributeReference& probe_attr);

        virtual bool consumeHashGroupAggregate_impl(const GroupByAggregateParam& param);

        virtual bool consumeAggregate_impl(const AggregateSpecifications& param);

        virtual const std::pair<bool,AttributeReference> consumeAlgebraComputation_impl(const AttributeReference& left_attr,
            const AttributeReference& right_attr,
            const ColumnAlgebraOperation& alg_op);

        virtual const std::pair<bool,AttributeReference>
            consumeAlgebraComputation_impl(const AttributeReference& left_attr,
            const boost::any constant,
            const ColumnAlgebraOperation& alg_op);

        virtual const std::pair<bool,AttributeReference>
            consumeAlgebraComputation_impl(const boost::any constant,
            const AttributeReference& right_attr,
            const ColumnAlgebraOperation& alg_op);

        virtual void printCode(std::ostream& out);

        virtual const PipelinePtr compile();

    protected:
        virtual bool createForLoop_impl(const TablePtr table, uint32_t version);
        virtual bool createHashTable(const AttributeReference& attr);

        virtual const std::string getCodeAllocateResultTable() const;
        virtual const std::string getCodeWriteResult() const;
        virtual const std::string getCodeWriteResultFromHashTable() const;
        virtual const std::string createResultTable() const;

        void generateCode_BitpackedGroupingKeyComputation(const GroupingAttributes& grouping_attrs);
        void generateCode_GenericGroupingKeyComputation(const GroupingAttributes& grouping_attrs);
        
        const std::string getCPPExpression(ValueComparator x) const;
        const std::string getCPPExpression(const Predicate& pred) const;
        const std::string getAggregationCode(const GroupingAttributes& grouping_columns,
            const AggregateSpecifications& aggregation_specs,
            const std::string access_ht_entry_expression) const;

        const AttributeReference getAttributeReference(const std::string& column_name) const;
        bool produceTuples(const ScanParam& param);

        std::stringstream header_and_types_block;
        std::stringstream fetch_input_code_block;
        std::stringstream generated_code;
        std::list<std::string> upper_code_block;
        std::list<std::string> lower_code_block;
        std::stringstream after_for_loop_block;
        std::stringstream create_result_table_code_block;
        std::stringstream cleanup_code;
    };

}; //end namespace CoGaDB

#endif	/* CPP_CODE_GENERATOR_HPP */
