
#include <query_processing/cross_join_operator.hpp>
#include <util/hardware_detector.hpp>

namespace CoGaDB {

    namespace query_processing {

        //Map_Init_Function init_function_Selection_operator=physical_operator::map_init_function_Selection_operator;

        namespace physical_operator {

            TypedOperatorPtr create_CPU_CrossJoin_Operator(TypedLogicalNode& logical_node, const hype::SchedulingDecision& sched_dec, TypedOperatorPtr left_child, TypedOperatorPtr right_child) {
                logical_operator::Logical_CrossJoin& log_join_ref = static_cast<logical_operator::Logical_CrossJoin&> (logical_node);
                if (!quiet && verbose && debug) std::cout << "create CPU_CrossJoin_Operator!" << std::endl;
                if (!left_child) {
                    std::cout << "Error! File: " << __FILE__ << " Line: " << __LINE__ << std::endl;
                    exit(-1);
                }



                assert(right_child != NULL); //binary operator
                return TypedOperatorPtr(new CPU_CrossJoin_Operator(sched_dec,
                        left_child,
                        right_child,
                        log_join_ref.getMaterializationStatus()));
            }


            Physical_Operator_Map_Ptr map_init_function_crossjoin_operator() {
                //std::cout << sd.getNameofChoosenAlgorithm() << std::endl;
                Physical_Operator_Map map;
                if (!quiet) std::cout << "calling map init function for JOIN operator!" << std::endl;
                //hype::Scheduler::instance().addAlgorithm("JOIN","CPU_NestedLoopJoin_Algorithm",hype::CPU,"Least Squares 2D","Periodic Recomputation");
                //stemod::Scheduler::instance().addAlgorithm("JOIN","CPU_SortMergeJoin_Algorithm",stemod::CPU,"Least Squares 2D","Periodic Recomputation");
                //hype::Scheduler::instance().addAlgorithm("JOIN","CPU_HashJoin_Algorithm",hype::CPU,"Least Squares 2D","Periodic Recomputation");

#ifdef COGADB_USE_KNN_REGRESSION_LEARNER
                hype::AlgorithmSpecification join_alg_spec_cpu_nlj("CPU_CrossJoin_Algorithm",
                        "CROSS_JOIN",
                        hype::KNN_Regression,
                        hype::Periodic);
#else
                hype::AlgorithmSpecification join_alg_spec_cpu_nlj("CPU_CrossJoin_Algorithm",
                        "CROSS_JOIN",
                        hype::Multilinear_Fitting_2D,
                        hype::Periodic);
#endif

                //addAlgorithmSpecificationToHardware();

                const DeviceSpecifications& dev_specs = HardwareDetector::instance().getDeviceSpecifications();

                for (unsigned int i = 0; i < dev_specs.size(); ++i) {
                    if (dev_specs[i].getDeviceType() == hype::CPU) {
                        hype::Scheduler::instance().addAlgorithm(join_alg_spec_cpu_nlj,dev_specs[i]);
                    } else if (dev_specs[i].getDeviceType() == hype::GPU) {
                        //hype::Scheduler::instance().addAlgorithm(group_by_alg_spec_gpu,dev_specs[i]);
                    }
                }



                //stemod::Scheduler::instance().addAlgorithm("SELECTION","GPU_Selection_Algorithm",stemod::GPU,"Least Squares 1D","Periodic Recomputation");
                map["CPU_CrossJoin_Algorithm"]=create_CPU_CrossJoin_Operator;

                //map["GPU_Selection_Algorithm"]=create_GPU_Selection_Operator;
                return Physical_Operator_Map_Ptr(new Physical_Operator_Map(map));
            }

        }//end namespace physical_operator

    }//end namespace query_processing

}; //end namespace CogaDB
