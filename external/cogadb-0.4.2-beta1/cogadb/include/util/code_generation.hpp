/* 
 * File:   code_generation.hpp
 * Author: sebastian
 *
 * Created on 27. September 2015, 20:16
 */

#ifndef CODE_GENERATION_HPP
#define	CODE_GENERATION_HPP

#include <string>
#include <boost/shared_ptr.hpp>
#include <core/global_definitions.hpp>

namespace CoGaDB{
    class AttributeReference;
    typedef boost::shared_ptr<AttributeReference> AttributeReferencePtr;
    class CodeGenerator;
    typedef boost::shared_ptr<CodeGenerator> CodeGeneratorPtr;
    class QueryContext;
    typedef boost::shared_ptr<QueryContext> QueryContextPtr;
    
    AttributeReferencePtr getAttributeReference(const std::string& column_name, 
            CodeGeneratorPtr code_gen, QueryContextPtr context);
    
    const std::string toCPPOperator(const ColumnAlgebraOperation& op);

}; //end namespace CoGaDB


#endif	/* CODE_GENERATION_HPP */

