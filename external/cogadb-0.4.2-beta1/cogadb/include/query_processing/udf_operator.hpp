/* 
 * File:   udf_operator.hpp
 * Author: sebastian
 *
 * Created on 14. Mai 2015, 22:14
 */

#ifndef UDF_OPERATOR_HPP
#define	UDF_OPERATOR_HPP

#pragma once

#include <query_processing/definitions.hpp>
#include <core/runtime_configuration.hpp>


namespace CoGaDB {

    namespace query_processing {

        namespace physical_operator {

            class UDF_Operator : public hype::queryprocessing::UnaryOperator<TablePtr, TablePtr> {
            public:
                typedef hype::queryprocessing::OperatorMapper_Helper_Template<TablePtr>::TypedOperatorPtr TypedOperatorPtr;

                UDF_Operator(const hype::SchedulingDecision& sched_dec,
                        TypedOperatorPtr child,
                        const std::string& function_name,
                        const std::vector<boost::any>& function_parameters)
                : UnaryOperator<TablePtr, TablePtr>(sched_dec, child),
                function_name_(function_name),
                function_parameters_(function_parameters) {
                }

                virtual bool execute();

                virtual ~UDF_Operator() {
                }
            private:
                std::string function_name_;
                std::vector<boost::any> function_parameters_;
            };


            Physical_Operator_Map_Ptr map_init_function_udf_operator();
            TypedOperatorPtr create_udf_operator(TypedLogicalNode& logical_node, const hype::SchedulingDecision&, TypedOperatorPtr left_child, TypedOperatorPtr right_child);
        }//end namespace physical_operator

        namespace logical_operator {

            class Logical_UDF : public hype::queryprocessing::TypedNode_Impl<TablePtr, physical_operator::map_init_function_udf_operator> //init_function_sort_operator>
            {
            public:

                Logical_UDF(const std::string& _function_name,
                        const std::vector<boost::any>& _function_parameters,
                        hype::DeviceConstraint dev_constr = CoGaDB::RuntimeConfiguration::instance().getGlobalDeviceConstraint());
                //                : TypedNode_Impl<TablePtr, physical_operator::map_init_function_udf_operator>(false, dev_constr),
                //                function_name(_function_name),
                //                function_parameters(_function_parameters)
                //                {
                //                }

                //                virtual unsigned int getOutputResultSize() const {
                //                    return this->left_->getOutputResultSize();
                //                }
                //
                //                virtual double getCalculatedSelectivity() const {
                //                    return 1;
                //                }
                //
                //                virtual std::string getOperationName() const {
                //                    return "UDF";
                //                }
                //                
                //                std::string toString(bool verbose) const{
                //                    std::string result="UDF";
                //                    if(verbose){
                //                        std::stringstream ss;
                //                        ss << " (";
                //                        ss << function_name;
                //                        if(!function_parameters.empty()){
                //                            ss << ", ";
                //                            for(size_t i=0;i<function_parameters.size();++i){
                //                                ss << function_parameters[i];
                //                                if((i+1)<function_parameters.size())
                //                                    ss << ", ";
                //                            }
                //                        }
                //                        ss << ")";
                //                        result+=ss.str();  
                //                    }
                //                    return result;
                //
                //                }
                //                
                //                const std::string& getFunctionName() {
                //                    return function_name;
                //                }
                //
                //                const std::vector<boost::any>& getFunctionParameters() {
                //                    return function_parameters;
                //                }

                virtual unsigned int getOutputResultSize() const;

                virtual double getCalculatedSelectivity() const;

                virtual std::string getOperationName() const;

                std::string toString(bool verbose) const;

                const std::string& getFunctionName();

                const std::vector<boost::any>& getFunctionParameters();

                std::string function_name;
                std::vector<boost::any> function_parameters;
            };

        }//end namespace logical_operator

    }//end namespace query_processing

}; //end namespace CogaDB



#endif	/* UDF_OPERATOR_HPP */

