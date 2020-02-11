/*
 * File:   code_generator.hpp
 * Author: sebastian
 *
 * Created on 19. Juli 2015, 18:38
 */

#ifndef CODE_GENERATOR_HPP
#define	CODE_GENERATOR_HPP

#include <string>
#include <vector>
#include <core/attribute_reference.hpp>
#include <query_compilation/aggregate_specification.hpp>
#include <query_compilation/pipeline.hpp>

/** @defgroup pipeline_breakers Pipeline Breakers
 *  Pipeline breakers are operators that require that a result is materialized
 *  before the result is passed to the next operator.
 *
 *  Examples for pipeline breakers are building a hash table (either for joins
 *  or aggregations or sorting a table).
 *
 *  In general, this function is a pipeline breaker. That means after this
 *  function was called, no further operations can be added to the pipeline!
 */

namespace CoGaDB {

    class PredicateExpression;
    typedef boost::shared_ptr<PredicateExpression> PredicateExpressionPtr;

    typedef std::vector<AttributeReference> ProjectionParam;
    typedef std::vector<AttributeReference> ScanParam;

    enum CodeGeneratorType {
        CPP_CODE_GENERATOR,
        CUDA_C_CODE_GENERATOR
    };

    std::string getComputedAttributeVarName(const AttributeReference& left_attr,
            const AttributeReference& right_attr,
            const ColumnAlgebraOperation& alg_op);

    std::string getComputedAttributeVarName(const AttributeReference& attr,
            const AggregationFunction agg_func);

    AttributeReference createComputedAttribute(const AttributeReference& left_attr,
            const AttributeReference& right_attr,
            const ColumnAlgebraOperation& alg_op);

    AttributeReference createComputedAttribute(const AttributeReference& left_attr,
            const boost::any& constant,
            const ColumnAlgebraOperation& alg_op);

    AttributeReference createComputedAttribute(const boost::any& constant,
            const AttributeReference& right_attr,
            const ColumnAlgebraOperation& alg_op);

    AttributeReference createComputedAttribute(const AttributeReference& attr,
            const AggregationFunction& agg_func);
    
    AttributeReference createComputedAttribute(const AttributeReference& attr,
            const AggregationFunction& agg_func, const std::string& result_name);

    class CodeGenerator;
    typedef boost::shared_ptr<CodeGenerator> CodeGeneratorPtr;

    const CodeGeneratorPtr createCodeGenerator(const CodeGeneratorType coge_gen,
            const ProjectionParam& param,
            const TablePtr input_table,
            const boost::any& generic_code_gen_param = boost::any(),
            uint32_t table_version = 1);


    const CodeGeneratorPtr createCodeGenerator(const CodeGeneratorType coge_gen,
            const ProjectionParam& param,
            const boost::any& generic_code_gen_param = boost::any());


    /*! \brief A code generator is the central abstraction for generating
     * an iterative program for relational queries.
     *  \detail The code generator provides methods that allow to generate code
     * for relational operators in a fixed order (e.g., it allows finde grained
     * control about the order the operators are processed).
     */
    class CodeGenerator {
    protected:
        CodeGenerator(const ProjectionParam& param,
                const TablePtr table,
                uint32_t version = 1);
        CodeGenerator(const ProjectionParam& param);

        friend const CodeGeneratorPtr createCodeGenerator(const CodeGeneratorType coge_gen,
                const ProjectionParam& param,
                const TablePtr input_table,
                uint32_t version);
        friend const CodeGeneratorPtr createCodeGenerator(const CodeGeneratorType coge_gen,
            const ProjectionParam& param,
            const boost::any& generic_code_gen_param);
    public:
        /*! \brief include attribute \ref attr in result table produced by this
            pipeline */
        bool addAttributeProjection(const AttributeReference& attr);

        /*! \brief add the attribute to the set of columns that are accessed
         * by the pipeline.
         */
        bool addToScannedAttributes(const AttributeReference& attr);

        AttributeReferencePtr getProjectionAttributeByName(const std::string& name) const;
        AttributeReferencePtr getScannedAttributeByName(const std::string& name) const;

        /*! \brief signals when no operators were inserted in the pipeline, 
         * so we can skip a compilation step */
        bool isEmpty() const;
        
        void print() const;

        bool dropProjectionAttributes();
        /*! \brief create a for loop for input table */
        bool createForLoop();
        bool createForLoop(const TablePtr table, uint32_t version);
        /*! \brief generates code for a selection predicate expression */
        bool consumeSelection(const PredicateExpressionPtr pred_expr);
        /*! \brief generates code to build a hash table that is used
         * by a later pipeline for probing (e.g., for hash joins)
         *  \ingroup pipeline_breaker
         */
        bool consumeBuildHashTable(const AttributeReference& attr);
        /*! \brief generates code that probes attribute \ref probe_attr to the
         *  hash table of attribute \ref hash_table_attr.
         */
        bool consumeProbeHashTable(const AttributeReference& hash_table_attr,
                const AttributeReference& probe_attr);
        /*! \brief generates code that performs a hash-based aggregation
         *  that involves grouping.
         *  \ingroup pipeline_breaker
         */
        bool consumeHashGroupAggregate(const GroupByAggregateParam& param);
        /*! \brief generates code that performs a hash-based aggregation
         *  that does not require grouping.
         *  \ingroup pipeline_breaker
         */
        bool consumeAggregate(const AggregateSpecifications& param);
        /*! \brief generates code for algebra operation, e.g., add to columns
         */
        const std::pair<bool, AttributeReference> consumeAlgebraComputation(
                const AttributeReference& left_attr,
                const AttributeReference& right_attr,
                const ColumnAlgebraOperation& alg_op);
        /*! \brief generates code for algebra operation when a column and a
         * constant is involved
         */
        const std::pair<bool, AttributeReference>
        consumeAlgebraComputation(const AttributeReference& left_attr,
                const boost::any constant,
                const ColumnAlgebraOperation& alg_op);
        /*! \brief generates code for algebra operation when a constant and a
         * column is involved
         */
        const std::pair<bool, AttributeReference>
        consumeAlgebraComputation(const boost::any constant,
                const AttributeReference& right_attr,
                const ColumnAlgebraOperation& alg_op);

    protected:
        bool canOmitCompilation() const;
        
        virtual bool createForLoop_impl(const TablePtr table, uint32_t version) = 0;

        virtual bool consumeSelection_impl(const PredicateExpressionPtr pred_expr) = 0;

        virtual bool consumeBuildHashTable_impl(const AttributeReference& attr) = 0;

        virtual bool consumeProbeHashTable_impl(const AttributeReference& hash_table_attr,
                const AttributeReference& probe_attr) = 0;

        virtual bool consumeHashGroupAggregate_impl(const GroupByAggregateParam& param) = 0;

        virtual bool consumeAggregate_impl(const AggregateSpecifications& param) = 0;

        virtual const std::pair<bool, AttributeReference> consumeAlgebraComputation_impl(const AttributeReference& left_attr,
                const AttributeReference& right_attr,
                const ColumnAlgebraOperation& alg_op) = 0;

        virtual const std::pair<bool, AttributeReference>
        consumeAlgebraComputation_impl(const AttributeReference& left_attr,
                const boost::any constant,
                const ColumnAlgebraOperation& alg_op) = 0;

        virtual const std::pair<bool, AttributeReference>
        consumeAlgebraComputation_impl(const boost::any constant,
                const AttributeReference& right_attr,
                const ColumnAlgebraOperation& alg_op) = 0;

    public:

        /*! \brief */
        virtual void printCode(std::ostream& out) = 0;
        /*! \brief */
        virtual const PipelinePtr compile() = 0;

        virtual ~CodeGenerator();


    protected:
        ProjectionParam param;
        ScanParam scanned_attributes;
        PipelineEndType pipe_end;
        TablePtr input_table;
        uint32_t input_table_version;
        bool tuples_produced;
        /* \brief signals when no operators were inserted in the pipeline, so we can
        * skip a compilation step
        * \detail this happens e.g. if our pipeline just consists of a projection,
         * and the child operator is a pipeline breaker */
        bool is_empty_pipeline;
    };

    
    bool isEquivalent(const ProjectionParam& projected_attributes,
        const TableSchema& input_schema);
    
}; //end namespace CoGaDB



#endif	/* CODE_GENERATOR_HPP */
