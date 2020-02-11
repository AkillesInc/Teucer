#pragma once

#include <query_processing/definitions.hpp>
#include <util/getname.hpp>

namespace CoGaDB {
    namespace query_processing {
        namespace physical_operator {

            class CPU_Groupby_Operator : public hype::queryprocessing::UnaryOperator<TablePtr, TablePtr> {
            public:
                typedef hype::queryprocessing::OperatorMapper_Helper_Template<TablePtr>::TypedOperatorPtr TypedOperatorPtr;

                CPU_Groupby_Operator(const hype::SchedulingDecision& sched_dec,
                        TypedOperatorPtr child,
                        const std::list<std::string>& grouping_columns,
                        const std::list<ColumnAggregation>& aggregation_functions,
                        MaterializationStatus mat_stat = MATERIALIZE) : UnaryOperator<TablePtr, TablePtr>(sched_dec, child),
                grouping_columns_(grouping_columns),
                aggregation_functions_(aggregation_functions),
                mat_stat_(mat_stat) {
                }

                virtual bool execute(); 

                virtual ~CPU_Groupby_Operator() {
                }
            private:
                std::list<std::string> grouping_columns_;
                std::list<ColumnAggregation> aggregation_functions_;
                MaterializationStatus mat_stat_;
            };

            class GPU_Groupby_Operator : public hype::queryprocessing::UnaryOperator<TablePtr, TablePtr> {
            public:
                typedef hype::queryprocessing::OperatorMapper_Helper_Template<TablePtr>::TypedOperatorPtr TypedOperatorPtr;

                GPU_Groupby_Operator(const hype::SchedulingDecision& sched_dec,
                        TypedOperatorPtr child,
                        const std::list<std::string>& grouping_columns,
                        const std::list<ColumnAggregation>& aggregation_functions,
                        MaterializationStatus mat_stat = MATERIALIZE) : UnaryOperator<TablePtr, TablePtr>(sched_dec, child),
                grouping_columns_(grouping_columns),
                aggregation_functions_(aggregation_functions),
                mat_stat_(mat_stat) {
                }

                virtual bool execute() {
                    return false;
                }

                virtual ~GPU_Groupby_Operator() {
                }
            private:
                std::list<std::string> grouping_columns_;
                std::list<ColumnAggregation> aggregation_functions_;
                MaterializationStatus mat_stat_;
            };

            Physical_Operator_Map_Ptr map_init_function_groupby_operator();
			TypedOperatorPtr create_CPU_Groupby_Operator(TypedLogicalNode& logical_node, const hype::SchedulingDecision&, TypedOperatorPtr left_child, TypedOperatorPtr right_child);
			TypedOperatorPtr create_GPU_Groupby_Operator(TypedLogicalNode& logical_node, const hype::SchedulingDecision&, TypedOperatorPtr left_child, TypedOperatorPtr right_child);

        }//end namespace physical_operator

        namespace logical_operator {

			class Logical_Groupby : public hype::queryprocessing::TypedNode_Impl<TablePtr,physical_operator::map_init_function_groupby_operator> //init_function_Groupby_operator>
            {
            public:

                Logical_Groupby(const std::list<std::string>& grouping_columns,
                        const std::list<ColumnAggregation>& aggregation_functions,
                        MaterializationStatus mat_stat = LOOKUP,
                        hype::DeviceConstraint dev_constr = CoGaDB::RuntimeConfiguration::instance().getGlobalDeviceConstraint())
                : TypedNode_Impl<TablePtr, physical_operator::map_init_function_groupby_operator>(false, dev_constr),
                grouping_columns_(grouping_columns),
                aggregation_functions_(aggregation_functions),
                mat_stat_(mat_stat) {
                }

                virtual unsigned int getOutputResultSize() const {
                    return 10;
                }

                virtual double getCalculatedSelectivity() const {
                    return 0.3;
                }

                virtual std::string getOperationName() const {
                    return "GROUPBY";
                }
                std::string toString(bool verbose) const{
                    std::string result="GROUPBY";
                    if(verbose){
                        result+=" (";
                        std::list<std::string>::const_iterator cit;
                        for(cit=grouping_columns_.begin();cit!=grouping_columns_.end();++cit){
                            result+=*cit;
                            if(cit!=--grouping_columns_.end())
                                result+=",";
                        }
                        result+=")";
                        result+=" USING (";
                        std::list<ColumnAggregation>::const_iterator agg_func_cit;
                        for(agg_func_cit=aggregation_functions_.begin();agg_func_cit!=aggregation_functions_.end();++agg_func_cit){
                            result+=CoGaDB::util::getName(agg_func_cit->second.first);
                            result+="(";
                            result+=agg_func_cit->first;
                            result+=")";
                        }
                        result+=")";
                    }
                    return result;

                }
                
                const std::list<std::string> getNamesOfReferencedColumns() const{
                    std::list<std::string> result(grouping_columns_.begin(),grouping_columns_.end());
                    std::list<ColumnAggregation>::const_iterator agg_func_cit;
                    for(agg_func_cit=aggregation_functions_.begin();agg_func_cit!=aggregation_functions_.end();++agg_func_cit){
                        result.push_back(agg_func_cit->first);
//                        std::cout << "Aggregated Column: " << agg_func_cit->first << std::endl;
                    }
                    return result;
                }
                
                const std::list<std::string>& getGroupingColumns() {
                    return grouping_columns_;
                }

                const std::list<ColumnAggregation>& getColumnAggregationFunctions() {
                    return aggregation_functions_;
                }

                const MaterializationStatus& getMaterializationStatus() const {
                    return mat_stat_;
                }
                
            const hype::Tuple getFeatureVector() const{
                hype::Tuple t;
                if (this->left_) { //if left child is valid (has to be by convention!), add input data size
                    //if we already know the correct input data size, because the child node was already executed
                    //during query chopping, we use the real cardinality, other wise we call the estimator
                    if(this->left_->getPhysicalOperator()){
                                                
                        //for the learning algorithms, it is helpful to 
                        //artificially adjust the points in multidimensional space
                        //so proper regression models can be build
                        //we use the logarithm function to destribute the points more equally
                        
                        double input_size = this->left_->getPhysicalOperator()->getResultSize();
//                        if(input_size>0){
//                            input_size = log(input_size)*10;
//                        }
                        t.push_back(input_size);
                        t.push_back(this->aggregation_functions_.size()*input_size);
                        //t.push_back(this->grouping_columns_.size()*10);

                    }else{
                        return this->Node::getFeatureVector();
                        //t.push_back(this->left_->getOutputResultSize());
                    }
                }else{
                    HYPE_FATAL_ERROR("Invalid Left Child!",std::cout);
                }
                
//                //size_t number_of_dimension_tids = this->left_->getOutputResultSize();
//                //TablePtr pk_table = getTablebyName(this->getPK_FK_Join_Predicate().join_pk_table_name);
//                TablePtr fk_table = getTablebyName(this->getPK_FK_Join_Predicate().join_fk_table_name);
//                size_t number_of_fact_table_tids = fk_table->getNumberofRows();       
//                
//                //t.push_back(double(number_of_dimension_tids));
//                t.push_back(double(number_of_fact_table_tids));
                
                return t;           
            }
                
            void produce_impl(CodeGeneratorPtr code_gen, QueryContextPtr context);

            void consume_impl(CodeGeneratorPtr code_gen, QueryContextPtr context); 
            
            private:
                std::list<std::string> grouping_columns_;
                std::list<ColumnAggregation> aggregation_functions_;
                MaterializationStatus mat_stat_;
            };

        }//end namespace logical_operator

    }//end namespace query_processing

}; //end namespace CogaDB
