/* 
 * File:   predicate_expression.hpp
 * Author: sebastian
 *
 * Created on 24. August 2015, 10:48
 */

#ifndef PREDICATE_EXPRESSION_HPP
#define	PREDICATE_EXPRESSION_HPP

#include <boost/shared_ptr.hpp>
#include <core/global_definitions.hpp>

namespace CoGaDB{
    
    enum PredicateSpecificationType {
        ValueValuePredicateSpec, 
        ValueConstantPredicateSpec, 
        ValueRegularExpressionPredicateSpec
    };    
    
    enum LogicalOperation{
        LOGICAL_AND, LOGICAL_OR
    };    
    
//    class PredicateSpecification;
//    typedef boost::shared_ptr<PredicateSpecification> PredicateSpecificationPtr;

    class PredicateExpression;
    typedef boost::shared_ptr<PredicateExpression>  PredicateExpressionPtr;    

    class AttributeReference;
    typedef boost::shared_ptr<AttributeReference> AttributeReferencePtr;
    
    class PredicateExpression{
    public:
        virtual const std::string getCPPExpression() const = 0;
        virtual const std::vector<AttributeReferencePtr> getScannedAttributes() const = 0;
        
        virtual ~PredicateExpression();
    private:
        
//        std::vector<PredicateExpressionPtr> predicate_expressions_;
//        LogicalOperation log_op_;
    };
    
    const PredicateExpressionPtr createColumnColumnComparisonPredicateExpression(
            const AttributeReferencePtr& left,
            const AttributeReferencePtr& right,
            ValueComparator comp);    
    
    const PredicateExpressionPtr createColumnConstantComparisonPredicateExpression(
            const AttributeReferencePtr left_attr, 
            const boost::any& right_constant, 
            ValueComparator comp);
    
        
//        PredicateSpecification(PredicateSpecificationType pred_t,
//                const boost::any& left_constant, 
//                const AttributeReferencePtr right_attr, 
//                ValueComparator comp);  
        
    const PredicateExpressionPtr createRegularExpressionPredicateExpression(
                const AttributeReferencePtr left_attr, 
                const std::string& regular_expression, 
                ValueComparator comp); 
    
    const PredicateExpressionPtr createPredicateExpression(const std::vector<PredicateExpressionPtr>&,
            const LogicalOperation&);    
    
    


}; //end namespace CoGaDB


#endif	/* PREDICATE_EXPRESSION_HPP */

