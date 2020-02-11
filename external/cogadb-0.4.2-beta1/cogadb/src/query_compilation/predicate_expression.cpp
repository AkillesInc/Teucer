
#include <query_compilation/predicate_expression.hpp>
#include <query_compilation/predicate_specification.hpp>

namespace CoGaDB{
    
    PredicateExpression::~PredicateExpression(){
        
    }    
    
    const PredicateExpressionPtr createColumnColumnComparisonPredicateExpression(
            const AttributeReferencePtr& left,
            const AttributeReferencePtr& right,
            ValueComparator comp){
        PredicateExpressionPtr ptr(new PredicateSpecification(
                ValueValuePredicateSpec, 
                left, 
                right, 
                comp));
        return ptr;
    }        
        
    const PredicateExpressionPtr createColumnConstantComparisonPredicateExpression(
            const AttributeReferencePtr left_attr, 
            const boost::any& right_constant, 
            ValueComparator comp){
        PredicateExpressionPtr ptr(new PredicateSpecification(
                ValueConstantPredicateSpec, 
                left_attr, 
                right_constant, 
                comp));
        return ptr;
    }  
    

    const PredicateExpressionPtr createRegularExpressionPredicateExpression(
                const AttributeReferencePtr left_attr, 
                const std::string& regular_expression, 
                ValueComparator comp){
        PredicateExpressionPtr ptr(new PredicateSpecification(
                ValueRegularExpressionPredicateSpec, 
                left_attr, 
                regular_expression, 
                comp));
        return ptr;
    }  
    
    const PredicateExpressionPtr createPredicateExpression(
            const std::vector<PredicateExpressionPtr>& pred_expression,
            const LogicalOperation& log_op){
        PredicateExpressionPtr ptr(new PredicateCombination(
                pred_expression, log_op));
        return ptr;
    }  
    
    
}; //end namespace CoGaDB
