
#include <query_processing/sort_operator.hpp>
#include <util/hardware_detector.hpp>
#include <query_compilation/code_generator.hpp>
#include <query_compilation/query_context.hpp>
#include <query_compilation/predicate_expression.hpp>

namespace CoGaDB {

    namespace query_processing {

        namespace physical_operator {

            TypedOperatorPtr create_CPU_SORT_Operator(TypedLogicalNode& logical_node, const hype::SchedulingDecision& sched_dec, TypedOperatorPtr left_child, TypedOperatorPtr right_child) {
                logical_operator::Logical_Sort& log_sort_ref = static_cast<logical_operator::Logical_Sort&> (logical_node);
                //std::cout << "create CPU_SORT_Operator!" << std::endl;
                if (!left_child) {
                    std::cout << "Error! File: " << __FILE__ << " Line: " << __LINE__ << std::endl;
                    exit(-1);
                }
                assert(right_child == NULL); //unary operator
                return TypedOperatorPtr(new CPU_Sort_Operator(sched_dec,
                        left_child,
                        //log_sort_ref.getColumnNames(),
                        //log_sort_ref.getSortOrder(),
                        log_sort_ref.getSortAttributes(),
                        log_sort_ref.getMaterializationStatus()));
            }

            TypedOperatorPtr create_GPU_SORT_Operator(TypedLogicalNode& logical_node, const hype::SchedulingDecision& sched_dec, TypedOperatorPtr left_child, TypedOperatorPtr right_child) {
                logical_operator::Logical_Sort& log_sort_ref = static_cast<logical_operator::Logical_Sort&> (logical_node);
                //std::cout << "create GPU_SORT_Operator!" << std::endl;
                if (!left_child) {
                    std::cout << "Error! File: " << __FILE__ << " Line: " << __LINE__ << std::endl;
                    exit(-1);
                }
                assert(right_child == NULL); //unary operator
                return TypedOperatorPtr(new GPU_Sort_Operator(sched_dec,
                        left_child,
                        //log_sort_ref.getColumnNames(),
                        //log_sort_ref.getSortOrder(),
                        log_sort_ref.getSortAttributes(),
                        log_sort_ref.getMaterializationStatus()));
            }

            Physical_Operator_Map_Ptr map_init_function_sort_operator() {
                Physical_Operator_Map map;
                if (!quiet) std::cout << "calling map init function for Sort Operator!" << std::endl;

#ifdef COGADB_USE_KNN_REGRESSION_LEARNER
                hype::AlgorithmSpecification sort_alg_spec_cpu("CPU_Sort_Algorithm",
                        "SORT",
                        hype::KNN_Regression,
                        hype::Periodic);

                hype::AlgorithmSpecification sort_alg_spec_gpu("GPU_Sort_Algorithm",
                        "SORT",
                        hype::KNN_Regression,
                        hype::Periodic);
#else
                
                hype::AlgorithmSpecification sort_alg_spec_cpu("CPU_Sort_Algorithm",
                        "SORT",
                        hype::Multilinear_Fitting_2D,
                        hype::Periodic);

                hype::AlgorithmSpecification sort_alg_spec_gpu("GPU_Sort_Algorithm",
                        "SORT",
                        hype::Multilinear_Fitting_2D, //Least_Squares_1D,
                        hype::Periodic);
#endif

                const DeviceSpecifications& dev_specs = HardwareDetector::instance().getDeviceSpecifications();

                for (unsigned int i = 0; i < dev_specs.size(); ++i) {
                    if (dev_specs[i].getDeviceType() == hype::CPU) {
                        hype::Scheduler::instance().addAlgorithm(sort_alg_spec_cpu, dev_specs[i]);
                    } else if (dev_specs[i].getDeviceType() == hype::GPU) {
#ifdef ENABLE_GPU_ACCELERATION
//                        hype::Scheduler::instance().addAlgorithm(sort_alg_spec_gpu, dev_specs[i]);
#endif
                    }
                }


                map["CPU_Sort_Algorithm"] = create_CPU_SORT_Operator;
                map["GPU_Sort_Algorithm"] = create_GPU_SORT_Operator;
                return Physical_Operator_Map_Ptr(new Physical_Operator_Map(map));
            }

        }//end namespace physical_operator
        
        
        namespace logical_operator {

                Logical_Sort::Logical_Sort(const SortAttributeList& sort_attributes,
                        MaterializationStatus mat_stat,
                        hype::DeviceConstraint dev_constr)
                : TypedNode_Impl<TablePtr, physical_operator::map_init_function_sort_operator>(false, dev_constr),
                sort_attributes_(sort_attributes),
                mat_stat_(mat_stat) {
                }

                unsigned int Logical_Sort::getOutputResultSize() const {
                    return this->left_->getOutputResultSize();
                }

                double Logical_Sort::getCalculatedSelectivity() const {
                    return 1;
                }

                std::string Logical_Sort::getOperationName() const {
                    return "SORT";
                }
                
                std::string Logical_Sort::toString(bool verbose) const{
                    std::string result="SORT";
                    if(verbose){
                        result+=" BY (";

                        std::string asc("ASCENDING");
                        std::string desc("DESCENDING");
                        
                        SortAttributeList::const_iterator cit;
                        for(cit=sort_attributes_.begin();
                                cit!=sort_attributes_.end(); ++cit){
                            
                            result+=cit->first;
                            result+=" ORDER ";
                            
                            if(cit->second == ASCENDING)
                                result += asc;
                            else 
                                result += desc;
                            
                            if(cit!=--sort_attributes_.end())
                                result+=",";
                        }
                        result+=")";
                    }
                    return result;

                }
                
                const std::list<SortAttribute>& Logical_Sort::getSortAttributes() {
                    return sort_attributes_;
                }

                MaterializationStatus Logical_Sort::getMaterializationStatus() {
                    return mat_stat_;
                }
                                
                
            void Logical_Sort::produce_impl(CodeGeneratorPtr code_gen, QueryContextPtr context){
                /* The sort operator is a pipeline breaker. Thus, we 
                 * generate a new code generator and a new query context */
                ProjectionParam param;
                CodeGeneratorPtr code_gen_build_phase = createCodeGenerator(CPP_CODE_GENERATOR, 
                        param);
                QueryContextPtr context_build_phase = createQueryContext(NORMAL_PIPELINE);
                /* add the attributes accessed by this operator to the list in 
                 * the query context */
                std::list<SortAttribute>::const_iterator cit;
                for(cit=this->sort_attributes_.begin();cit!=this->sort_attributes_.end();++cit){
                    context->addAccessedColumn(cit->first);
                }
                
                /* pass down all referenced columns up to now to new query context */
                std::vector<std::string> accessed_columns = context->getAccessedColumns();
                for(size_t i=0;i<accessed_columns.size();++i){
                    context_build_phase->addAccessedColumn(accessed_columns[i]);
                }
                /* pass down projection list to sub pipeline */
                std::vector<std::string> projected_columns = context->getProjectionList();
                for(size_t i=0;i<projected_columns.size();++i){
                    context_build_phase->addColumnToProjectionList(projected_columns[i]);
                }
                for(cit=this->sort_attributes_.begin();cit!=this->sort_attributes_.end();++cit){
                    context_build_phase->addColumnToProjectionList(cit->first);
                }
                /* create the build pipeline */
                left_->produce(code_gen_build_phase, context_build_phase);
                TablePtr result;
//                if(!code_gen_build_phase->isEmpty()){
                    PipelinePtr build_pipeline = code_gen_build_phase->compile();
                    if(!build_pipeline){
                        COGADB_FATAL_ERROR("Compiling code for pipeline running "
                                << "before sort failed!","");
                    }
                    build_pipeline->execute();
                    result = build_pipeline->getResult();
                if(!result){
                    COGADB_FATAL_ERROR("Execution of compiled code for pipeline "
                            << "running before sort failed!!","");
                }
                context->updateStatistics(build_pipeline);
                context->updateStatistics(context_build_phase);
                Timestamp begin = getTimestamp();
                result = BaseTable::sort(result, sort_attributes_, mat_stat_, CPU);
                Timestamp end = getTimestamp();
                if(!result){
                    COGADB_FATAL_ERROR("Failed to sort table!","");
                }
                double sort_execution_time = double(end-begin)/(1000*1000*1000);
                context->addExecutionTime(sort_execution_time);
                /* we later require the attribute references to the computed result,
                 so we store it in the query context */
                std::vector<ColumnProperties> col_props = result->getPropertiesOfColumns();
                projected_columns.clear();
                for(size_t i=0;i<col_props.size();++i){
                        AttributeReferencePtr attr = createInputAttribute(
                                result,
                                col_props[i].name);
                        context->addReferencedAttributeFromOtherPipeline(attr);
                        code_gen->addToScannedAttributes(*attr);
                        projected_columns.push_back(col_props[i].name);
                }
                /* add attribute references of result table to this pipelines
                 * output schema, if we find them in the list of projected columns */
                for(size_t i=0;i<projected_columns.size();++i){
                    if(result->hasColumn(projected_columns[i])){
                        AttributeReferencePtr attr = createInputAttribute(
                                result,
                                projected_columns[i]);                    
                        code_gen->addAttributeProjection(*attr);
                    }
                }
                code_gen->createForLoop(result,1);
            }
            
            void Logical_Sort::consume_impl(CodeGeneratorPtr code_gen, QueryContextPtr context){

                /* this is a pipeline breaker, so we must not call consume of the parent operator! */
                /* as we currently do not generate code for sort operations, we 
                 * do not do anything in this function*/
                
//                std::vector<AttributeReferencePtr> sort_attributes;
//                SortSpecification sort_spec;
//                std::list<SortAttribute>::const_iterator cit;
//                for(cit=this->sort_attributes_.begin();cit!=this->sort_attributes_.end();++cit){
//                    AttributeReferencePtr sort_attr 
//                            = code_gen->getScannedAttributeByName(cit->first);
//                    assert(sort_attr!=NULL);
//                    if(!sort_attr){
//                        COGADB_FATAL_ERROR("Could not retrieve Attribute Reference " 
//                                << toString(sort_attr) << " sort operator!","");
//                    }
//                    sort_spec.push_back(SortStep(sort_attr), cit->second);
////                    sort_attributes.push_back(sort_attr);
//                }
//
////                    AttributeReferencePtr left_join_attr 
////                            = context->getAccessedColumns(this->join_column1_name_);
//
//                    if(!code_gen->consumeBuildHashTable(*left_join_attr)){
//                        COGADB_FATAL_ERROR("Failed to generate build pipeline for join!","");
//                    }
// 
            }
                
            const hype::Tuple Logical_Sort::getFeatureVector() const{
                
                hype::Tuple t;
                if (this->left_) { //if left child is valid (has to be by convention!), add input data size
                    //if we already know the correct input data size, because the child node was already executed
                    //during query chopping, we use the real cardinality, other wise we call the estimator
                    if(this->left_->getPhysicalOperator()){
                        t.push_back(this->left_->getPhysicalOperator()->getResultSize()); // ->result_size_;
                    }else{
                        t.push_back(this->left_->getOutputResultSize());
                    }
                    t.push_back(sort_attributes_.size());
                }
                return t;
            }

        }//end namespace logical_operator

    }//end namespace query_processing

}; //end namespace CogaDB
