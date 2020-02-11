#pragma once

#include <query_processing/scan_operator.hpp>
#include <query_processing/rename_operator.hpp>
#include <query_processing/sort_operator.hpp>
#include <query_processing/selection_operator.hpp>
#include <query_processing/join_operator.hpp>
#include <query_processing/cross_join_operator.hpp>
#include <query_processing/projection_operator.hpp>
#include <query_processing/groupby_operator.hpp>
#include <query_processing/complex_selection_operator.hpp>
#include <query_processing/pk_fk_join_operator.hpp>
#include <query_processing/invisible_join_operator.hpp>
#include <query_processing/indexed_tuple_reconstruction_operator.hpp>
#include <query_processing/chain_join_operator.hpp>
#include <query_processing/fetch_join_operator.hpp>
#include <query_processing/generate_constant_column_operator.hpp>
#include <query_processing/udf_operator.hpp>
#include <query_processing/logical_query_plan.hpp>

#include <query_processing/column_computation_constant_operator.hpp>
#include <query_processing/column_computation_algebra_operator.hpp>

#include <query_processing/column_processing/cpu_columnscan_operator.hpp>
#include <query_processing/column_processing/cpu_algebra_operator.hpp>
#include <query_processing/column_processing/cpu_column_constant_filter_operator.hpp>
#include <query_processing/column_processing/positionlist_operator.hpp>
#include <query_processing/column_processing/column_comparator.hpp>
//newe column based operators
#include <query_processing/column_processing/bitmap_operator.hpp>
#include <query_processing/column_processing/column_convert_bitmap_to_positionlist.hpp>
#include <query_processing/column_processing/column_convert_bitmap_to_positionlist.hpp>
#include <query_processing/column_processing/column_convert_positionlist_to_bitmap.hpp>
#include <query_processing/column_processing/column_fetch_join_operator.hpp>
#include <query_processing/column_processing/column_bitmap_fetch_join_operator.hpp>
#include <query_processing/column_processing/column_bitmap_selection_operator.hpp>

#include <statistics/statistics_manager.hpp>
#include <parser/client.hpp>

//#define COGADB_EXECUTE_GPU_OPERATOR(name_of_gpu_operator) \
//                    StatisticsManager::instance().addToValue(NUMBER_OF_EXECUTED_GPU_OPERATORS,1);
//
//#define COGADB_ABORT_GPU_OPERATOR(name_of_gpu_operator) \
//                    COGADB_WARNING("GPU Operator for" << name_of_gpu_operator << "! Falling back to CPU operator...",""); \
//                    StatisticsManager::instance().addToValue(NUMBER_OF_ABORTED_GPU_OPERATORS,1);

//#include <query_processing/benchmark.hpp>

//#define INVISIBLE_JOIN_USE_POSITIONLIST_ONLY_PLANS

namespace CoGaDB
{
	namespace query_processing{
		
           CoGaDB::query_processing::PhysicalQueryPlanPtr optimize_and_execute(const std::string& query_name, LogicalQueryPlan& log_plan, ClientPtr client); 
            
           query_processing::column_processing::cpu::LogicalQueryPlanPtr createColumnPlanforDisjunction(TablePtr table, const Disjunction& disjunction, hype::DeviceConstraint dev_constr);
           
           const query_processing::column_processing::cpu::LogicalQueryPlanPtr createColumnBasedQueryPlan(TablePtr table, const KNF_Selection_Expression& knf_expr, hype::DeviceConstraint dev_constr = hype::DeviceConstraint());
           
           //PositionListPtr optimize_and_execute_column_based_plan(query_processing::column_processing::cpu::LogicalQueryPlanPtr);
 
           //const query_processing::PhysicalQueryPlanPtr createPhysicalQueryPlan(query_processing::LogicalQueryPlan&);
           
           //const query_processing::PhysicalQueryPlanPtr createPhysicalQueryPlan(query_processing::LogicalQueryPlanPtr);
           
           //const query_processing::column_processing::cpu::PhysicalQueryPlanPtr createPhysicalQueryPlan(query_processing::column_processing::cpu::LogicalQueryPlanPtr);
           
           TablePtr two_phase_physical_optimization_selection(TablePtr table, const KNF_Selection_Expression&, hype::DeviceConstraint dev_constr = hype::DeviceConstraint(), MaterializationStatus mat_stat=MATERIALIZE, ParallelizationMode comp_mode=SERIAL, std::ostream* out = &std::cout);
           
           
           
//           {
//
//                if(!table) return query_processing::column_processing::cpu::LogicalQueryPlanPtr();                 
//                if(knf_expr.disjunctions.empty()) return  query_processing::column_processing::cpu::LogicalQueryPlanPtr();
//            //stores the result for each disjunction
//            //std::vector<PositionListPtr> disjunctions_result_tid_lists(knf_expr.disjunctions.size());
//            typedef query_processing::column_processing::cpu::TypedLogicalNodePtr TypedLogicalNodePtr;
//            std::queue<TypedLogicalNodePtr> conjunction_queue; // current_tree_level;  
//            std::vector<std::queue<TypedLogicalNodePtr> > disjunction_queues(knf_expr.disjunctions.size());
//            for(unsigned int i=0;i<knf_expr.disjunctions.size();i++){
//                //stores the tid list for each predicate
//                //std::vector<PositionListPtr> predicate_result_tid_lists(knf_expr.disjunctions[i].size());
//                
//                
//                for(unsigned j=0;j<knf_expr.disjunctions[i].size();j++){
//                    if(knf_expr.disjunctions[i][j].getPredicateType()==ValueValuePredicate){
//                        std::cerr << "Currently mot Supported!" << std::endl;
//                        
//                        //ColumnPtr col = table->getColumnbyName(knf_expr.disjunctions[i][j].getColumn1Name());
//                        
//                        //boost::shared_ptr<logical_operator::Logical_Column_Scan> scan_col(new logical_operator::Logical_Column_Scan(table,knf_expr.disjunctions[i][j].getColumn1Name()));
//                        //boost::shared_ptr<logical_operator::Logical_Column_Constant_Filter>  filter_col(new logical_operator::Logical_Column_Constant_Filter(Predicate("",boost::any(20),ValueConstantPredicate,GREATER))); 
////                 
////                        if(!col){ 
////                                cout << "Error! in BaseTable::selection(): Could not find Column " << knf_expr.disjunctions[i][j].getColumn1Name() << " in Table " << table->getName() << endl; 
////                                cout << "In File " << __FILE__ << ":" << __LINE__ << endl;
////                                return TablePtr();
////                        }
////                        ColumnPtr col2= table->getColumnbyName(knf_expr.disjunctions[i][j].getColumn2Name());
////                        if(!col2){ 
////                                cout << "Error! in BaseTable::selection(): Could not find Column " << knf_expr.disjunctions[i][j].getColumn2Name() << " in Table " << table->getName() << endl; 
////                                cout << "In File " << __FILE__ << ":" << __LINE__ << endl;                               
////                                return TablePtr();
////                        }
////                        predicate_result_tid_lists[j]=col->selection(col2,knf_expr.disjunctions[i][j].getValueComparator());
//                    }else if(knf_expr.disjunctions[i][j].getPredicateType()==ValueConstantPredicate){
//                        boost::shared_ptr<logical_operator::Logical_Column_Scan> scan_col(new logical_operator::Logical_Column_Scan(table,knf_expr.disjunctions[i][j].getColumn1Name()));
//                        boost::shared_ptr<logical_operator::Logical_Column_Constant_Filter>  filter_col(new logical_operator::Logical_Column_Constant_Filter(knf_expr.disjunctions[i][j])); 
//                 
//                        filter_col->setLeft(scan_col);
//                        disjunction_queues[i].push(filter_col);
//                        
//                        
//                        //predicate_result_tid_lists[j]=col->selection(knf_expr.disjunctions[i][j].getConstant(),knf_expr.disjunctions[i][j].getValueComparator());
//                    }else{
//                        std::cerr << "FATAL ERROR! in BaseTable::selection(): Unknown Predicate Type!" << std::endl;
//                        std::cerr << "In File " << __FILE__ << ":" << __LINE__ << std::endl;
//                        exit(-1);
//                    }
//                }
//            }
//            //merge sorted tid lists (compute the disjunction)
//            //->build a subtree for each disjunction
//            for(unsigned int i=0;i<disjunction_queues[i].size();i++){
//                while(disjunction_queues[i].size()>1){
//                    TypedLogicalNodePtr node1 =disjunction_queues[i].front();
//                    disjunction_queues[i].pop();
//                    TypedLogicalNodePtr node2=disjunction_queues[i].front();
//                    disjunction_queues[i].pop();
//                    boost::shared_ptr<logical_operator::Logical_PositionList_Operator>  tid_union(new logical_operator::Logical_PositionList_Operator(POSITIONLIST_UNION));    
//                    tid_union->setLeft(node1);
//                    tid_union->setRight(node2);                
//                    disjunction_queues[i].push(tid_union);        
//                }
//                //subtree for disjunction completed, now it has to be combined wit hthe other disjunctions
//                conjunction_queue.push(disjunction_queues[i].front());
//            }
//            
//            //now combine all sub trees for the disjunctions by adding TID Intersion nodes
//            while(conjunction_queue.size()>1){
//                TypedLogicalNodePtr node1=conjunction_queue.front();
//                conjunction_queue.pop();
//                TypedLogicalNodePtr node2=conjunction_queue.front();
//                conjunction_queue.pop();
//                boost::shared_ptr<logical_operator::Logical_PositionList_Operator>  tid_intersection(new logical_operator::Logical_PositionList_Operator(POSITIONLIST_INTERSECTION));    
//                tid_intersection->setLeft(node1);
//                tid_intersection->setRight(node2);                
//                conjunction_queue.push(tid_intersection);        
//            }
//            
//            return query_processing::column_processing::cpu::LogicalQueryPlanPtr(new query_processing::column_processing::cpu::LogicalQueryPlan(conjunction_queue.front()));
//            }
		
	} //end namespace query_processing
}; //end namespace CogaDB
