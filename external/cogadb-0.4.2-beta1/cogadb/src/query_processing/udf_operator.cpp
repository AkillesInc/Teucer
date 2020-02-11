
#include <sstream>
#include <query_processing/udf_operator.hpp>
#include <util/hardware_detector.hpp>
#include <util/iostream.hpp>

namespace CoGaDB {
    namespace query_processing {
        namespace physical_operator {

            bool UDF_Operator::execute() {
                hype::ProcessingDeviceID id=sched_dec_.getDeviceSpecification().getProcessingDeviceID();
                ProcessorSpecification proc_spec(id);
                this->result_ = BaseTable::user_defined_function(this->child_->getResult(), function_name_, function_parameters_, proc_spec);
                if (this->result_) {
                    this->result_size_ = this->result_->getNumberofRows();
                    return true;
                } else {
                    return false;
                }
            }

            TypedOperatorPtr create_udf_operator(TypedLogicalNode& logical_node, const hype::SchedulingDecision& sched_dec, TypedOperatorPtr left_child, TypedOperatorPtr right_child) {
                logical_operator::Logical_UDF& log_sort_ref = static_cast<logical_operator::Logical_UDF&> (logical_node);
                if (!left_child) {
                    COGADB_FATAL_ERROR("UDF operator requires that left child is present!", "");
                }
                assert(right_child == NULL); //unary operator
                return TypedOperatorPtr(new UDF_Operator(sched_dec,
                        left_child,
                        log_sort_ref.getFunctionName(),
                        log_sort_ref.getFunctionParameters()));
            }

            Physical_Operator_Map_Ptr map_init_function_udf_operator() {
                Physical_Operator_Map map;

#ifdef COGADB_USE_KNN_REGRESSION_LEARNER
                hype::AlgorithmSpecification udf_alg_spec_cpu("CPU_UDF_Algorithm",
                        "UDF",
                        hype::KNN_Regression,
                        hype::Periodic);

#else

                hype::AlgorithmSpecification sort_alg_spec_cpu("CPU_UDF_Algorithm",
                        "UDF",
                        hype::Multilinear_Fitting_2D,
                        hype::Periodic);

#endif

                const DeviceSpecifications& dev_specs = HardwareDetector::instance().getDeviceSpecifications();

                for (unsigned int i = 0; i < dev_specs.size(); ++i) {
                    if (dev_specs[i].getDeviceType() == hype::CPU) {
                        hype::Scheduler::instance().addAlgorithm(udf_alg_spec_cpu, dev_specs[i]);
                    } else if (dev_specs[i].getDeviceType() == hype::GPU) {
#ifdef ENABLE_GPU_ACCELERATION

#endif
                    }
                }

                map["CPU_UDF_Algorithm"] = create_udf_operator;
                return Physical_Operator_Map_Ptr(new Physical_Operator_Map(map));
            }
        }//end namespace physical_operator

        namespace logical_operator {

            Logical_UDF::Logical_UDF(const std::string& _function_name,
                    const std::vector<boost::any>& _function_parameters,
                    hype::DeviceConstraint dev_constr)
            : TypedNode_Impl<TablePtr, physical_operator::map_init_function_udf_operator>(false, dev_constr),
            function_name(_function_name),
            function_parameters(_function_parameters) {
            }

            unsigned int Logical_UDF::getOutputResultSize() const {
                return this->left_->getOutputResultSize();
            }

            double Logical_UDF::getCalculatedSelectivity() const {
                return 1;
            }

            std::string Logical_UDF::getOperationName() const {
                return "UDF";
            }

            std::string Logical_UDF::toString(bool verbose) const {
                std::string result = "UDF";
                if (verbose) {
                    std::stringstream ss;
                    ss << " (";
                    ss << function_name;
                    if (!function_parameters.empty()) {
                        ss << ", ";
                        for (size_t i = 0; i < function_parameters.size(); ++i) {
                            ss << function_parameters[i];
                            if ((i + 1) < function_parameters.size())
                                ss << ", ";
                        }
                    }
                    ss << ")";
                    result += ss.str();
                }
                return result;
            }

            const std::string& Logical_UDF::getFunctionName() {
                return function_name;
            }

            const std::vector<boost::any>& Logical_UDF::getFunctionParameters() {
                return function_parameters;
            }

        }//end namespace logical_operator
    }//end namespace query_processing
}; //end namespace CoGaDB

