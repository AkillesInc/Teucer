
#include <query_compilation/predicate_specification.hpp>
#include <query_compilation/code_generators/cpp_code_generator.hpp>
#include <util/types.hpp>

namespace CoGaDB{
    
        PredicateSpecification::PredicateSpecification(PredicateSpecificationType pred_t, 
                const AttributeReferencePtr left_attr, 
                const AttributeReferencePtr right_attr, 
                ValueComparator comp)
        : pred_t_(pred_t), left_attr_(left_attr), right_attr_(right_attr),
        constant_(), reg_ex_(), comp_(comp)
        {
            
        }
        
        PredicateSpecification::PredicateSpecification(PredicateSpecificationType pred_t, 
                const AttributeReferencePtr left_attr, 
                const boost::any& right_constant, 
                ValueComparator comp)
        : pred_t_(pred_t), left_attr_(left_attr), right_attr_(),
        constant_(right_constant), reg_ex_(), comp_(comp)        
        {
            
        }
        
//        PredicateSpecification::PredicateSpecification(PredicateSpecificationType pred_t, 
//                const boost::any& left_constant, 
//                const AttributeReferencePtr right_attr, 
//                ValueComparator comp)
//        : pred_t_(pred_t), left_attr_(), right_attr_(right_attr),
//        constant_(left_constant), reg_ex_(), comp_(comp)          
//        {
//            
//        }
        
        PredicateSpecification::PredicateSpecification(PredicateSpecificationType pred_t,
                const AttributeReferencePtr left_attr, 
                const std::string& regular_expression, 
                ValueComparator comp)
        : pred_t_(pred_t), left_attr_(left_attr), right_attr_(),
        constant_(), reg_ex_(regular_expression), comp_(comp)         
        {
            
        }
            
        const std::string PredicateSpecification::getCPPExpression() const{
            std::stringstream expr;
            
            if(this->pred_t_==ValueValuePredicateSpec){
                COGADB_FATAL_ERROR("No Code Generation for Column Column Expressions yet!","");
            }else if(this->pred_t_==ValueConstantPredicateSpec){
                
                expr << getElementAccessExpression(*left_attr_) << " "
                     << ::CoGaDB::getCPPExpression(comp_) << " ";
                if(constant_.type()==typeid(std::string) 
                        && this->left_attr_->getAttributeType()==VARCHAR){
                    expr << "\"" << ::CoGaDB::toCPPExpression(constant_) << "\"";
                }else if(constant_.type()==typeid(std::string) 
                        && this->left_attr_->getAttributeType()==DATE){
                    uint32_t val = 0;
                    if(!convertStringToInternalDateType(boost::any_cast<std::string>(constant_),val)){
                        COGADB_FATAL_ERROR("The string '" << boost::any_cast<std::string>(constant_) << "' is not representing a DATE!" << std::endl
                                           << "Typecast Failed!","");
                    }
                    boost::any new_constant(val);
                    expr << ::CoGaDB::toCPPExpression(new_constant);
                }else if(this->left_attr_->getAttributeType()==FLOAT){
                    /* special care for float attributes, because we need to 
                     * generate float constants for them (by default floating point 
                     * numbers are double in C++ and comparing the same value in a 
                     * float and a double results in incorrect results) */
                    
                    /* not that we might have a float column, but the predicate is 
                     always converted into a double values first! */
                    if(constant_.type()==typeid(float)){
                        expr << ::CoGaDB::toCPPExpression(constant_);
//                        float f = boost::any_cast<float>(constant_);
//                        if(ceil(f) == f){
//                            /* is integer, add ".0f" as suffix */
//                            expr << ::CoGaDB::toCPPExpression(constant_) << ".0f";   
//                        }else{
//                            /* is not integer, add "f" as suffix */ 
//                            expr << ::CoGaDB::toCPPExpression(constant_) << "f";
//                        }    
                        
                    }else if(constant_.type()==typeid(double)){
                        double f = boost::any_cast<double>(constant_);
                        if(ceil(f) == f){
                            /* is integer, add ".0f" as suffix */
                            expr << ::CoGaDB::toCPPExpression(constant_) << ".0f";   
                        }else{
                            /* is not integer, add "f" as suffix */ 
                            expr << ::CoGaDB::toCPPExpression(constant_) << "f";
                        }   
                    }else if(constant_.type()==typeid(int32_t)){
                        int32_t i = boost::any_cast<int32_t>(constant_);
                        float f = boost::numeric_cast<float>(i);
                        if(ceil(f) == f){
                            /* is integer, add ".0f" as suffix */
                            expr << f << ".0f";   
                        }else{
                            /* is not integer, add "f" as suffix */ 
                            expr << f << "f";
                        }  

                    }else{
                        COGADB_FATAL_ERROR("Unhandled Type: " << constant_.type().name() ,"");
                    }
                }else{
                    expr << ::CoGaDB::toCPPExpression(constant_);
                }
                
            }else if(this->pred_t_==ValueRegularExpressionPredicateSpec){                
                COGADB_FATAL_ERROR("No Code Generation for Regular Expressions yet!","");
            }else{
                COGADB_FATAL_ERROR("Invalid PredicateSpecificationType!","");
            }
            return expr.str();
        }
        
        const std::vector<AttributeReferencePtr> PredicateSpecification::getScannedAttributes() const{
            std::vector<AttributeReferencePtr> ret;
            
            if(this->pred_t_==ValueValuePredicateSpec){
                ret.push_back(left_attr_);
                ret.push_back(right_attr_);
            }else if(this->pred_t_==ValueConstantPredicateSpec){
                ret.push_back(left_attr_);
            }else if(this->pred_t_==ValueRegularExpressionPredicateSpec){                
                ret.push_back(left_attr_);
            }else{
                COGADB_FATAL_ERROR("Invalid PredicateSpecificationType!","");
            }
            return ret;
        }        
        

        PredicateSpecificationType PredicateSpecification::getPredicateSpecificationType() const{
            return this->pred_t_;
        }
        
        const AttributeReferencePtr PredicateSpecification::getLeftAttribute() const{
            assert(this->pred_t_==ValueValuePredicateSpec || 
                   this->pred_t_==ValueConstantPredicateSpec);
            return this->left_attr_;
        }
        const AttributeReferencePtr PredicateSpecification::getRightAttribute() const{
            assert(this->pred_t_==ValueValuePredicateSpec);            
            return this->right_attr_;
        }
        const boost::any& PredicateSpecification::getConstant() const{
//            assert(this->pred_t_==ValueLeftConstantRightPredicateSpec
//                  || this->pred_t_==ConstantLeftValueRightPredicateSpec);
            return this->constant_;
        }
        const std::string& PredicateSpecification::getRegularExpression() const{
            assert(this->pred_t_==ValueRegularExpressionPredicateSpec);
            return this->reg_ex_;
        }
        ValueComparator PredicateSpecification::getValueComparator() const{
            return this->comp_;
        }
        void PredicateSpecification::print() const{
            std::cout << this->toString() << std::endl;
        }
        std::string PredicateSpecification::toString() const{
            return std::string();
        }
        /*! \brief inverts the order of Value Value Predicates
                    e.g., Attr1>Attr2 -> Attr2<Attr1 
                    this is especially useful for join path optimization*/
        void PredicateSpecification::invertOrder(){
            if(this->pred_t_==ValueValuePredicateSpec){
                std::swap(this->left_attr_,this->right_attr_);
                if(this->comp_==LESSER){
                    this->comp_=GREATER;
                }else if(this->comp_==GREATER){
                    this->comp_=LESSER;           
                }else if(this->comp_==LESSER_EQUAL){
                    this->comp_=GREATER_EQUAL;
                }else if(this->comp_==GREATER_EQUAL){
                    this->comp_=LESSER_EQUAL;
                    
                }  
            }
        }
    
    
    PredicateCombination::PredicateCombination(const std::vector<PredicateExpressionPtr>& predicate_expressions,
        LogicalOperation log_op)
    : predicate_expressions_(predicate_expressions), log_op_(log_op)
    {
    }
    
    const std::string PredicateCombination::getCPPExpression() const{
        std::stringstream expr;
        
        expr << "(";
        for(size_t i=0;i<predicate_expressions_.size();++i){
            expr << predicate_expressions_[i]->getCPPExpression();
            if(i+1<predicate_expressions_.size()){
                expr << " " << ::CoGaDB::getCPPExpression(log_op_) << " ";
            }
        }
        expr << ")";
        return expr.str();
    }    
    
    const std::vector<AttributeReferencePtr> PredicateCombination::getScannedAttributes() const{
        
        std::vector<AttributeReferencePtr> ret;
        
        for(size_t i=0;i<predicate_expressions_.size();++i){
            std::vector<AttributeReferencePtr> tmp = predicate_expressions_[i]->getScannedAttributes();
            ret.insert(ret.end(),tmp.begin(), tmp.end());
        }        
        
        return ret;
    }
    
}; //end namespace CoGaDB
