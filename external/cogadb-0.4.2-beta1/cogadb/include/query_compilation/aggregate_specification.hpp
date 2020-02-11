/* 
 * File:   aggregate_specification.hpp
 * Author: sebastian
 *
 * Created on 24. August 2015, 11:06
 */

#ifndef AGGREGATE_SPECIFICATION_HPP
#define	AGGREGATE_SPECIFICATION_HPP


//#include <query_compilation/predicate_specification.hpp>
//#include <query_compilation/code_generators/cpp_code_generator.hpp>

#include <core/global_definitions.hpp>

#include <core/attribute_reference.hpp>
#include <core/operator_parameter_types.hpp>

#include "predicate_expression.hpp"

namespace CoGaDB{

//    class AttributeReference;
//    typedef boost::shared_ptr<AttributeReference> AttributeReferencePtr;    
    
    struct AggregateSpecification{
        
        AggregateSpecification(const AttributeReference& scan_attr,
            const AttributeReference& result_attr,
            const AggregationFunction& agg_func);
        AttributeReference scan_attr;
        AttributeReference result_attr;
        AggregationFunction agg_func;
    };
    
//    AggregateSpecification::AggregateSpecification(const AttributeReference& _scan_attr,
//         const AttributeReference& _result_attr,
//         const AggregationFunction& _agg_func)
//    : scan_attr(_scan_attr), result_attr(_result_attr), agg_func(_agg_func)
//    {    
//    }
 
    const AggregateSpecification createAggregateSpecification(const AttributeReference& attr, 
            const AggregationFunction& agg_func); 
    
    const AggregateSpecification createAggregateSpecification(const AttributeReference& attr, 
            const AggregationFunction& agg_func,
            const std::string& result_name);    
    
    typedef std::vector<AggregateSpecification> AggregateSpecifications;

    typedef std::vector<AttributeReference> GroupingAttributes;

    struct GroupByAggregateParam {
        GroupByAggregateParam(const ProcessorSpecification& proc_spec,
                const GroupingAttributes& grouping_attrs,
                const AggregateSpecifications& aggregation_specs);
        ProcessorSpecification proc_spec;
        GroupingAttributes grouping_attrs;
        AggregateSpecifications aggregation_specs;
    };

    typedef std::pair<AttributeReferencePtr,SortOrder> SortStep;
    typedef std::vector<SortStep> SortSpecification;
    
//    GroupByAggregateParam::GroupByAggregateParam(const ProcessorSpecification& _proc_spec,
//            const GroupingAttributes& _grouping_attrs,
//            const AggregateSpecifications& _aggregation_specs)
//    : proc_spec(_proc_spec), grouping_attrs(_grouping_attrs),
//    aggregation_specs(_aggregation_specs) {
//    }  
    
}; //end namespace CoGaDB


#endif	/* AGGREGATE_SPECIFICATION_HPP */

