#pragma once

#include <query_processing/definitions.hpp>

namespace CoGaDB {
    namespace query_processing {
        namespace physical_operator {

            class CPU_Projection_Operator : public hype::queryprocessing::UnaryOperator<TablePtr, TablePtr> {
            public:
                typedef hype::queryprocessing::OperatorMapper_Helper_Template<TablePtr>::TypedOperatorPtr TypedOperatorPtr;

                CPU_Projection_Operator(const hype::SchedulingDecision& sched_dec,
                        TypedOperatorPtr child,
                        const std::list<std::string>& columns_to_select,
                        MaterializationStatus mat_stat = MATERIALIZE) : UnaryOperator<TablePtr, TablePtr>(sched_dec, child),
                columns_to_select_(columns_to_select),
                mat_stat_(mat_stat) {
                }

                virtual bool execute() {
                    if (!quiet && verbose && debug) std::cout << "Execute Projection CPU" << std::endl;
                    //const TablePtr sort(TablePtr table, const std::string& column_name, SortOrder order=ASCENDING, MaterializationStatus mat_stat=MATERIALIZE, ComputeDevice comp_dev=CPU);
                    //this->result_=BaseTable::sort(this->getInputData(), column_name_,order_, mat_stat_,CPU);
                    this->result_ = BaseTable::projection(this->getInputData(), columns_to_select_, mat_stat_, CPU);
                    if (this->result_) {
                        setResultSize(((TablePtr) this->result_)->getNumberofRows());
                        return true;
                    } else
                        return false;
                }

                virtual ~CPU_Projection_Operator() {
                }
            private:
                std::list<std::string> columns_to_select_;
                MaterializationStatus mat_stat_;
            };

            class GPU_Projection_Operator : public hype::queryprocessing::UnaryOperator<TablePtr, TablePtr> {
            public:
                typedef hype::queryprocessing::OperatorMapper_Helper_Template<TablePtr>::TypedOperatorPtr TypedOperatorPtr;

                GPU_Projection_Operator(const hype::SchedulingDecision& sched_dec,
                        TypedOperatorPtr child,
                        const std::list<std::string>& columns_to_select,
                        MaterializationStatus mat_stat = MATERIALIZE) : UnaryOperator<TablePtr, TablePtr>(sched_dec, child),
                columns_to_select_(columns_to_select),
                mat_stat_(mat_stat) {
                }

                virtual bool execute() {
                    if (!quiet && verbose && debug) std::cout << "Execute Projection GPU" << std::endl;
                    //const TablePtr sort(TablePtr table, const std::string& column_name, SortOrder order=ASCENDING, MaterializationStatus mat_stat=MATERIALIZE, ComputeDevice comp_dev=CPU);
                    this->result_ = BaseTable::projection(this->getInputData(), columns_to_select_, mat_stat_, GPU);
                    if (this->result_) {
                        setResultSize(((TablePtr) this->result_)->getNumberofRows());
                        return true;
                    } else
                        return false;
                }

                virtual ~GPU_Projection_Operator() {
                }
            private:
                std::list<std::string> columns_to_select_;
                MaterializationStatus mat_stat_;
            };

            Physical_Operator_Map_Ptr map_init_function_projection_operator();
			TypedOperatorPtr create_CPU_Projection_Operator(TypedLogicalNode& logical_node, const hype::SchedulingDecision&, TypedOperatorPtr left_child, TypedOperatorPtr right_child);
			TypedOperatorPtr create_GPU_Projection_Operator(TypedLogicalNode& logical_node, const hype::SchedulingDecision&, TypedOperatorPtr left_child, TypedOperatorPtr right_child);

        }//end namespace physical_operator

        //extern Map_Init_Function init_function_Projection_operator;

        //Map_Init_Function init_function_Projection_operator=physical_operator::map_init_function_Projection_operator; //boost::bind();

        namespace logical_operator {

			class Logical_Projection : public hype::queryprocessing::TypedNode_Impl<TablePtr,physical_operator::map_init_function_projection_operator> //init_function_Projection_operator>
            {
            public:

                Logical_Projection(const std::list<std::string>& columns_to_select,
                        MaterializationStatus mat_stat = MATERIALIZE) : TypedNode_Impl<TablePtr, physical_operator::map_init_function_projection_operator>(),
                columns_to_select_(columns_to_select),
                mat_stat_(mat_stat) {
                }

                virtual unsigned int getOutputResultSize() const {
                    return 10;
                }

                virtual double getCalculatedSelectivity() const {
                    return 1;
                }

                virtual std::string getOperationName() const {
                    return "PROJECTION";
                }
                
                std::string toString(bool verbose) const{
                    std::string result="PROJECTION";
                    if(verbose){
                        result+=" (";
                        std::list<std::string>::const_iterator cit;
                        for(cit=columns_to_select_.begin();cit!=columns_to_select_.end();++cit){
                            result+=*cit;
                            if(cit!=--columns_to_select_.end())
                                result+=",";
                        }
                        result+=")";
                    }
                    return result;
                    
                }
                
//            const hype::Tuple getFeatureVector() const{
//                hype::Tuple t;
//                if (this->left_) { //if left child is valid (has to be by convention!), add input data size
//                    //if we already know the correct input data size, because the child node was already executed
//                    //during query chopping, we use the real cardinality, other wise we call the estimator
//                    if(this->left_->getPhysicalOperator()){
//                                                
//                        //for the learning algorithms, it is helpful to 
//                        //artificially adjust the points in multidimensional space
//                        //so proper regression models can be build
//                        //we use the logarithm function to destribute the points more equally
//                        
//                        double input_size = this->left_->getPhysicalOperator()->getResultSize();
//
//                        //the first feature is always the number of rows
//                        t.push_back(input_size);
//                        //for projections, the number of rows doesn't matter, so we artificially increase the 
//                        //number of columns to include in the projection
//                        //this operator hardly needs correct estimations, because 
//                        //it takes only a tiny fraction of the query time 
//                        t.push_back(this->columns_to_select_.size()*1000*1000*1000);
//                        //t.push_back(this->aggregation_functions_.size());
//                        //t.push_back(this->grouping_columns_.size()*10);
//
//                    }else{
//                        return this->Node::getFeatureVector();
//                        //t.push_back(this->left_->getOutputResultSize());
//                    }
//                }else{
//                    HYPE_FATAL_ERROR("Invalid Left Child!",std::cout);
//                }
//                
////                //size_t number_of_dimension_tids = this->left_->getOutputResultSize();
////                //TablePtr pk_table = getTablebyName(this->getPK_FK_Join_Predicate().join_pk_table_name);
////                TablePtr fk_table = getTablebyName(this->getPK_FK_Join_Predicate().join_fk_table_name);
////                size_t number_of_fact_table_tids = fk_table->getNumberofRows();       
////                
////                //t.push_back(double(number_of_dimension_tids));
////                t.push_back(double(number_of_fact_table_tids));
//                
//                return t;           
//            }
                
                const std::list<std::string>& getColumnList() {
                    return columns_to_select_;
                }

                const MaterializationStatus& getMaterializationStatus() const {
                    return mat_stat_;
                }
                
                const std::list<std::string> getNamesOfReferencedColumns() const{
                    return columns_to_select_;
                }
                
                void produce_impl(CodeGeneratorPtr code_gen, QueryContextPtr context);

                void consume_impl(CodeGeneratorPtr code_gen, QueryContextPtr context);  
                
            private:
                std::list<std::string> columns_to_select_;
                MaterializationStatus mat_stat_;
            };

        }//end namespace logical_operator

    }//end namespace query_processing

}; //end namespace CogaDB
