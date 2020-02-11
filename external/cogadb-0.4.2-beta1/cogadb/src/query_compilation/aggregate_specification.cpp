
#include <query_compilation/aggregate_specification.hpp>
#include <query_compilation/code_generators/cpp_code_generator.hpp>

namespace CoGaDB{

//    class AttributeReference;
//    typedef boost::shared_ptr<AttributeReference> AttributeReferencePtr;    
    
    
    AggregateSpecification::AggregateSpecification(const AttributeReference& _scan_attr,
         const AttributeReference& _result_attr,
         const AggregationFunction& _agg_func)
    : scan_attr(_scan_attr), result_attr(_result_attr), agg_func(_agg_func)
    {    
    }
 

    GroupByAggregateParam::GroupByAggregateParam(const ProcessorSpecification& _proc_spec,
            const GroupingAttributes& _grouping_attrs,
            const AggregateSpecifications& _aggregation_specs)
    : proc_spec(_proc_spec), grouping_attrs(_grouping_attrs),
    aggregation_specs(_aggregation_specs) {
    }  
    
    const AggregateSpecification createAggregateSpecification(const AttributeReference& attr_ref, 
            const AggregationFunction& agg_func, 
            const std::string& result_name){
            AttributeReference computed_attr = createComputedAttribute(
                    AttributeReference(attr_ref.getVersionedAttributeName(),DOUBLE,result_name,1), agg_func, result_name);            
            
            return AggregateSpecification(attr_ref, computed_attr, agg_func);
    }    

    const AggregateSpecification createAggregateSpecification(const AttributeReference& attr_ref, 
            const AggregationFunction& agg_func){
            AttributeReference computed_attr = createComputedAttribute(
                    AttributeReference(attr_ref.getVersionedAttributeName(),DOUBLE,attr_ref.getVersionedAttributeName(),1), agg_func);            
            
            return AggregateSpecification(attr_ref, computed_attr, agg_func);
    }       
    
}; //end namespace CoGaDB