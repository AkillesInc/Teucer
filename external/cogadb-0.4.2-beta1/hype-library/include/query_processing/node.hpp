
#pragma once

#include <map>
#include <ostream>
#include <core/scheduling_decision.hpp>

#include <boost/shared_ptr.hpp>
#include <core/specification.hpp>
#include <query_processing/typed_operator.hpp>
#include <query_optimization/qep.hpp>
#include <util/get_name.hpp>

#define STRICT_DATA_PLACEMENT_DRIVEN_OPTIMIZATION true
#define HYPE_PULL_BASED_QUERY_CHOPPING
                     
#ifdef HYPE_ENABLE_INTERACTION_WITH_COGADB
namespace CoGaDB{
    class CodeGenerator;
    typedef boost::shared_ptr<CodeGenerator> CodeGeneratorPtr;
    class QueryContext;
    typedef boost::shared_ptr<QueryContext> QueryContextPtr;
}; //end namespace CoGaDB
#endif

namespace hype {
    namespace queryprocessing {

        //forward declaration
        class Node;
        typedef boost::shared_ptr<Node> NodePtr;
        //forward declaration
        template <typename Type>
        class TypedNode;

        template <typename Type>
        class PhysicalQueryPlan;

        //make Operator Mapper a singleton!
        //when OperatorMapper is instanciated, add second tempel argument where user has to specify a function that returns the Physical_Operator_Map;

        //OperatorMapper<TablePtr,initFunction> mapper;
        //boost::function<std::map<std::string,boost::function<boost::shared_ptr<TypedOperator<Type> > (const stemod::SchedulingDecision&)> > function> ()>
        //each node has to get one

        template <typename Type>
        struct OperatorMapper_Helper_Template {
            typedef Type type;
            typedef TypedNode<Type> TypedLogicalNode;
            typedef boost::shared_ptr<TypedLogicalNode> TypedNodePtr;
            //typedef TypedOperator<Type> TypedOperator;
            typedef boost::shared_ptr<TypedOperator<Type> > TypedOperatorPtr;
            typedef boost::function < TypedOperatorPtr(TypedLogicalNode&, const hype::SchedulingDecision&, TypedOperatorPtr, TypedOperatorPtr) > Create_Typed_Operator_Function;
            typedef std::map<std::string, Create_Typed_Operator_Function> Physical_Operator_Map;
            typedef boost::shared_ptr<Physical_Operator_Map> Physical_Operator_Map_Ptr;
            //typedef boost::function<Physical_Operator_Map_Ptr ()> Map_Init_Function;
            typedef Physical_Operator_Map_Ptr(Map_Init_Function)();
            typedef boost::shared_ptr<PhysicalQueryPlan<Type> > PhysicalQueryPlanPtr;
        };

        template <typename Type, typename OperatorMapper_Helper_Template<Type>::Map_Init_Function& function>
        class OperatorMapper {
        public:
            //typedef boost::shared_ptr<boost::shared_ptr<TypedOperator<Type> > > (*Create_Typed_Operator_Function)(const stemod::SchedulingDecision& sched_dec);
            //typedef boost::function<boost::shared_ptr<TypedOperator<Type> > (const stemod::SchedulingDecision&)> Create_Typed_Operator_Function_t;
            //typedef std::map<std::string,Create_Typed_Operator_Function_t> Physical_Operator_Map;

            typedef typename OperatorMapper_Helper_Template<Type>::Create_Typed_Operator_Function Create_Typed_Operator_Function;
            typedef typename OperatorMapper_Helper_Template<Type>::Physical_Operator_Map Physical_Operator_Map;
            typedef typename OperatorMapper_Helper_Template<Type>::Physical_Operator_Map_Ptr Physical_Operator_Map_Ptr;
            typedef typename OperatorMapper_Helper_Template<Type>::Map_Init_Function Map_Init_Function;
            //typedef typename OperatorMapper_Helper_Template<Type>::TypedOperator TypedOperator;
            typedef typename OperatorMapper_Helper_Template<Type>::TypedOperatorPtr TypedOperatorPtr;
            typedef typename OperatorMapper_Helper_Template<Type>::TypedLogicalNode TypedLogicalNode;
            typedef typename OperatorMapper_Helper_Template<Type>::TypedNodePtr TypedNodePtr;


            static const Physical_Operator_Map_Ptr static_algorithm_name_to_physical_operator_map_ptr; //=function();

            OperatorMapper() {
            } //: algorithm_name_to_physical_operator_map_(){}

            TypedOperatorPtr getPhysicalOperator(TypedLogicalNode& logical_node, const hype::Tuple& features_of_input_dataset, TypedOperatorPtr left_child, TypedOperatorPtr right_child, DeviceTypeConstraint dev_constr) const {
                const std::string& operation_name = logical_node.getOperationName();

                OperatorSpecification op_spec(operation_name,
                        features_of_input_dataset,
                        //parameters are the same, because in the query processing engine, we model copy operations explicitely, so the copy cost have to be zero
                        hype::PD_Memory_0, //input data is in CPU RAM
                        hype::PD_Memory_0); //output data has to be stored in CPU RAM

                //DeviceConstraint dev_constr;

                hype::SchedulingDecision sched_dec = hype::Scheduler::instance().getOptimalAlgorithm(op_spec, dev_constr);
                //find operation name in map
                typename Physical_Operator_Map::iterator it = static_algorithm_name_to_physical_operator_map_ptr->find(sched_dec.getNameofChoosenAlgorithm());
                if (it == static_algorithm_name_to_physical_operator_map_ptr->end()) {
                    std::cout << "[HyPE library] FATAL Error! " << typeid(OperatorMapper<Type, function>).name() << ": Missing entry in PhysicalOperatorMap for Algorithm '"
                            << sched_dec.getNameofChoosenAlgorithm() << "'" << std::endl;
                    exit(-1);
                }
                TypedOperatorPtr physical_operator;
                //call create function
                if (it->second) {
                    physical_operator = it->second(logical_node, sched_dec, left_child, right_child);
                } else {
                    std::cout << "[HyPE library] FATAL Error! Invalid Function Pointer in OperationMapper::getPhysicalOperator()" << std::endl;
                    exit(-1);
                }
                //return physical operator
                return physical_operator;
            }
            
            TypedOperatorPtr createPhysicalOperator(TypedLogicalNode& logical_node, const hype::SchedulingDecision& sched_dec, TypedOperatorPtr left_child, TypedOperatorPtr right_child) const {
                const std::string& operation_name = logical_node.getOperationName();

                //find operation name in map
                typename Physical_Operator_Map::iterator it = static_algorithm_name_to_physical_operator_map_ptr->find(sched_dec.getNameofChoosenAlgorithm());
                if (it == static_algorithm_name_to_physical_operator_map_ptr->end()) {
                    std::cout << "[HyPE library] FATAL Error! " << typeid (OperatorMapper<Type, function>).name() << ": Missing entry in PhysicalOperatorMap for Algorithm '"
                            << sched_dec.getNameofChoosenAlgorithm() << "'" << std::endl;
                    exit(-1);
                }
                TypedOperatorPtr physical_operator;
                //call create function
                if (it->second) {
                    physical_operator = it->second(logical_node, sched_dec, left_child, right_child);
                } else {
                    std::cout << "[HyPE library] FATAL Error! Invalid Function Pointer in OperationMapper::getPhysicalOperator()" << std::endl;
                    exit(-1);
                }
                //return physical operator
                return physical_operator;
            }
            
            //insert map definition here
            //Physical_Operator_Map algorithm_name_to_physical_operator_map_;
        };

        template <typename Type, typename OperatorMapper_Helper_Template<Type>::Map_Init_Function& function>
        const typename OperatorMapper<Type, function>::Physical_Operator_Map_Ptr OperatorMapper<Type, function>::static_algorithm_name_to_physical_operator_map_ptr = function();


        bool scheduleAndExecute(NodePtr logical_operator, std::ostream* out);
        //for logical plan, create derived class, which gets as template argument the Types of the corresponding physical Operators

        class Node {
        public:
//            typedef boost::shared_ptr<Node> NodePtr;

            Node(DeviceConstraint dev_constr = DeviceConstraint())
             : parent_(), left_(), right_(), level_(0), dev_constr_(dev_constr), 
             mutex_(), condition_variable_(), left_child_ready_(false), 
             right_child_ready_(false), is_ready_(false), physical_operator_(), out(&std::cout) {
 
            }

            /*
            Node(NodePtr parent) : parent_(parent), left_(), right_(), level_(0){

            }

            Node(NodePtr parent, NodePtr left, NodePtr right) : parent_(parent), left_(left), right_(right), level_(0){

            }*/

            virtual ~Node() {
                //physical_operator_.reset();
                //set pointer to NULL
                physical_operator_=OperatorPtr();
            }

            bool isRoot() const {
                if (parent_.get() == NULL) return true;
                return false;
            }

            bool isLeaf() const {
                if (left_.get() == NULL && right_.get() == NULL) return true;
                return false;
            }

            const NodePtr getLeft() const {
                return left_;
            }

            const NodePtr getRight() const {
                return right_;
            }

            const NodePtr getParent() const{
                return parent_;
            }
            
            unsigned int getLevel() {
                return level_;
            }
            
            void setLevel(unsigned int level) {
                level_ = level;
            }

            virtual hype::query_optimization::QEP_Node* toQEP_Node() = 0;
            
            virtual unsigned int getOutputResultSize() const = 0;

            virtual double getSelectivity() const = 0;
            
            //virtual const Tuple getFeatureVector() const = 0;

            virtual std::string getOperationName() const = 0;
            
#ifdef HYPE_ENABLE_INTERACTION_WITH_COGADB
            void produce(CoGaDB::CodeGeneratorPtr code_gen, 
                CoGaDB::QueryContextPtr context);            
            void consume(CoGaDB::CodeGeneratorPtr code_gen, 
                CoGaDB::QueryContextPtr context); 
//                        virtual void produce_impl(CoGaDB::CodeGeneratorPtr code_gen, 
//                CoGaDB::QueryContextPtr context);            
//            virtual void consume_impl(CoGaDB::CodeGeneratorPtr code_gen, 
//                CoGaDB::QueryContextPtr context); 
#endif            
            
            
            virtual const std::list<std::string> getNamesOfReferencedColumns() const{
                return std::list<std::string>();
            }
            
            //this function is needed to determine, whether an operator uses
            //wo phase physical optimization feature, where one operator may
            //generate a query plan itself (e.g., for invisible join and complex 
            //selection)
            //in case query chopping is enable, the system can run into a deadlock, because
            //if the operator generating and executing the subplan runs on the same
            //device as one operator in the subplan, a deadlock occures because the 
            //generator operator waits for the processing operator, whereas the processing 
            //operator waits to be scheduled, but is blocked by the generator operator 
            //(operators are executed serially on one processing device)
            virtual bool generatesSubQueryPlan() const{
                return false;
            }

            virtual std::string toString(bool verbose=false) const{
                if(verbose){
                        return this->getOperationName();
                        //return this->getOperationName()+std::string("\t")+util::getName(this->dev_constr_);                
                }else{
                        return this->getOperationName();               
                }
            }
            
            //const std::string& getOperationName() const;

            void setLeft(NodePtr left) {
                left_ = left;
                //left->setParent(left_);
            }

            void setRight(NodePtr right) {
                right_ = right;
                //right->setParent(right_);
            }

            void setParent(NodePtr parent) {
                parent_ = parent;
            }

            const DeviceConstraint& getDeviceConstraint() const {
                return this->dev_constr_;
            }

            void setDeviceConstraint(const DeviceConstraint& dev_constr){
                this->dev_constr_=dev_constr;
            }
            
            virtual OperatorPtr getPhysicalOperator() = 0;
            virtual OperatorPtr getPhysicalOperator(const SchedulingDecision& sched_dec) = 0;
            
            
            void waitForChildren(){
                {                    
                    boost::unique_lock<boost::mutex> lock(this->mutex_);
                    while(!this->left_child_ready_ || !this->right_child_ready_)
                    {
                        this->condition_variable_.wait(lock);
                    }
                    
//                    boost::unique_lock<boost::mutex> lock(this->mutex_);
//                    if(this->right_){
//                        //binary operator
//                        assert(this->left_!=NULL);
//                        while(!this->left_->is_ready_ || !this->right_->is_ready_)
//                        {
//                            this->condition_variable_.wait(lock);
//                        }
//                    }else{
//                        //unary operator
//                        while(!this->left_->is_ready_)
//                        {
//                            this->condition_variable_.wait(lock);
//                        }
//                    }
                    
                }
            }
            
            void waitForSelf(){
                {                    
                    boost::unique_lock<boost::mutex> lock(this->mutex_);
                    while(!this->is_ready_)
                    {
                        this->condition_variable_.wait(lock);
                    }
                }
            }
            
//            bool scheduleAndExecute(){
//                        Tuple t = this->getFeatureVector();
//
//                        OperatorSpecification op_spec(this->getOperationName(),
//                                t,
//                                //parameters are the same, because in the query processing engine, we model copy operations explicitely, so the copy cost have to be zero
//                                hype::PD_Memory_0, //input data is in CPU RAM
//                                hype::PD_Memory_0); //output data has to be stored in CPU RAM
//
//                        SchedulingDecision sched_dec = hype::Scheduler::instance().getOptimalAlgorithm(op_spec, this->getDeviceConstraint());
//                        OperatorPtr op = this->getPhysicalOperator(sched_dec);
//                        op->setLogicalOperator(this);
//                        op->setOutputStream(*out);
//                        return op->operator ()();
//            }
            
            static bool hasChildNodeAborted(NodePtr logical_operator){
                assert(logical_operator!=NULL);
                bool left_aborted=false;
                bool right_aborted=false;
                if(logical_operator->getLeft() 
                && logical_operator->getLeft()->getPhysicalOperator()){
                    left_aborted = logical_operator->getLeft()->getPhysicalOperator()->hasAborted();
                    left_aborted|=hasChildNodeAborted(logical_operator->getLeft());
                }
                if(logical_operator->getRight() 
                && logical_operator->getRight()->getPhysicalOperator()){
                    right_aborted = logical_operator->getRight()->getPhysicalOperator()->hasAborted();
                    right_aborted|=hasChildNodeAborted(logical_operator->getRight());
                }
                if(left_aborted || right_aborted){
                    return true;
                }else{
                    return false;
                } 
            }
            
            static void chopOffAndExecute(NodePtr logical_operator, std::ostream* out){
                if(!logical_operator) return;
                assert(out!=NULL);
                logical_operator->setOutputStream(*out);
                if(logical_operator->isLeaf()){
                    if(hype::core::Runtime_Configuration::instance().isPullBasedQueryChoppingEnabled()){
                        //queue operator in global operator stream
                        core::Scheduler::instance().addIntoGlobalOperatorStream(logical_operator);     
                    }else{
                        scheduleAndExecute(logical_operator,out);
                    }
                    return;
                    //logical_operator->waitForSelf();
                    
                    
                }else{

#ifdef HYPE_ENABLE_PARALLEL_QUERY_PLAN_EVALUATION
                    boost::thread_group threads;
                    if(logical_operator->right_){
                        //chopOffAndExecute(logical_operator->right_);
                        threads.add_thread(new boost::thread(boost::bind(&Node::chopOffAndExecute, logical_operator->right_, out)));
                    }else{
                        logical_operator->right_child_ready_=true;
                    }
                    if(logical_operator->left_){
                        //chopOffAndExecute(logical_operator->left_);
                        threads.add_thread(new boost::thread(boost::bind(&Node::chopOffAndExecute, logical_operator->left_, out)));
                    }else{
                        logical_operator->left_child_ready_=true;
                    }
                    threads.join_all();
#else
                    if(logical_operator->right_){
                        Node::chopOffAndExecute(logical_operator->right_, out);
                    }else{
                        logical_operator->right_child_ready_=true;
                    }
                    if(logical_operator->left_){
                        Node::chopOffAndExecute(logical_operator->left_, out);
                    }else{
                        logical_operator->left_child_ready_=true;
                    }
#endif
                                    
                    logical_operator->waitForChildren();
                    
                    //operators generating query plans are executed immediately 
                    //(otherwise, the system may run into a deadlock, see also 
                    //the comments in method generatesSubQueryPlan()), all other 
                    //operators are executed by the execution engine
                    //therefore, the plan generator should avoid doing data processing and
                    //instead put the computational intensive tasks in the generated plans,
                    //which are then executed by the execution engine
                    if(logical_operator->generatesSubQueryPlan()){
                          scheduleAndExecute(logical_operator,out);
//                        Tuple t = logical_operator->getFeatureVector();
//
//                        OperatorSpecification op_spec(logical_operator->getOperationName(),
//                                t,
//                                //parameters are the same, because in the query processing engine, we model copy operations explicitely, so the copy cost have to be zero
//                                hype::PD_Memory_0, //input data is in CPU RAM
//                                hype::PD_Memory_0); //output data has to be stored in CPU RAM
//
//                        SchedulingDecision sched_dec = hype::Scheduler::instance().getOptimalAlgorithm(op_spec, logical_operator->getDeviceConstraint());
//                        OperatorPtr op = logical_operator->getPhysicalOperator(sched_dec);
//                        op->setLogicalOperator(logical_operator);
//                        op->setOutputStream(*out);
//                        op->operator ()();
                    }else{
                        
                        if(core::Runtime_Configuration::instance().getDataPlacementDrivenOptimization()){
                            //consider current data placement in query otpimization
                            //if we find that a fetch joins join indexes is cached on the GPU, we set a GPU_ONLY cosntraint
                            //otherwise, we set a CPU_ONLY constrained
                            //note that this requires to have a data placement routine that regularily puts the most recently used
                            //access structures on the GPU!
                            if(logical_operator->isInputDataCachedInGPU() && logical_operator->dev_constr_!=hype::CPU_ONLY){
                                if(STRICT_DATA_PLACEMENT_DRIVEN_OPTIMIZATION
                                || logical_operator->getOperationName()=="COLUMN_FETCH_JOIN" || logical_operator->getOperationName()=="FETCH_JOIN"){
                                    //std::cout << "Annote COLUMN_FETCH_JOIN as GPU_ONLY!" << std::endl;
                                    logical_operator->dev_constr_=hype::GPU_ONLY;
                                }
                                //logical_operator->dev_constr_=hype::GPU_ONLY;
                            }else if(!logical_operator->isInputDataCachedInGPU() && (STRICT_DATA_PLACEMENT_DRIVEN_OPTIMIZATION
                                || logical_operator->getOperationName()=="COLUMN_FETCH_JOIN" || logical_operator->getOperationName()=="FETCH_JOIN")){
                                logical_operator->dev_constr_=hype::CPU_ONLY;
                            }
                        }
#define ENABLE_CHAINING_FOR_QUERY_CHOPPING                     
#ifdef ENABLE_CHAINING_FOR_QUERY_CHOPPING
                        //if any operator in the subtree aborted, then we 
                        //enforce a CPU only execution
                        if(hasChildNodeAborted(logical_operator)){
                            logical_operator->setDeviceConstraint(hype::CPU_ONLY);
                        }
                        //chain operators on same processor to avoid copy operations
                        if(logical_operator->getLeft() && logical_operator->getRight()){
                            //binary operators
                            if(logical_operator->getLeft()->getPhysicalOperator()
                            && logical_operator->getRight()->getPhysicalOperator()){
                               ProcessingDeviceType left_pt = logical_operator->getLeft()->getPhysicalOperator()->getSchedulingDecision().getDeviceSpecification().getDeviceType();
                               ProcessingDeviceType right_pt = logical_operator->getRight()->getPhysicalOperator()->getSchedulingDecision().getDeviceSpecification().getDeviceType();
                               
                               DeviceTypeConstraint left_dev_constr = util::getDeviceConstraintForProcessingDeviceType(left_pt);
                               DeviceTypeConstraint right_dev_constr = util::getDeviceConstraintForProcessingDeviceType(right_pt);
                               
                               //we omit chain breakers in our placement heuristic, 
                               //and handle them differently than normal operators
                               if(!util::isChainBreaker(logical_operator->getOperationName())){
                                   //if at least one of the childs is a 
                                   //chain breaker, we let HyPE's operator 
                                   //placement decide on which processor the 
                                   //chain is continued
                                   if(util::isChainBreaker(logical_operator->getLeft()->getOperationName())
                                   || util::isChainBreaker(logical_operator->getRight()->getOperationName())
                                   //it is commonly aceptable to change the 
                                   //processor for a binary operator when at 
                                   //least one child nodes is a selection
                                   || logical_operator->getLeft()->getOperationName()=="COMPLEX_SELECTION"
                                   || logical_operator->getRight()->getOperationName()=="COMPLEX_SELECTION"){
                                       //logical_operator->setDeviceConstraint(hype::ANY_DEVICE);
                                   }else if(left_pt==right_pt && logical_operator->dev_constr_==ANY_DEVICE){
                                       //continue chain (but continue chain on GPU only in case no operator aborted!)
                                       if((left_pt==hype::GPU
                                         && !logical_operator->getLeft()->getPhysicalOperator()->hasAborted() 
                                         && !logical_operator->getRight()->getPhysicalOperator()->hasAborted() 
                                          ) )
                                       {
                                           logical_operator->setDeviceConstraint(hype::GPU_ONLY); //left_dev_constr);
                                           //std::cout << "[QC]: Operator aborted, will not continue chain!" << std::endl; 
                                       }
                                       if(left_pt==hype::CPU 
                                       || logical_operator->getLeft()->getPhysicalOperator()->hasAborted()
                                       || logical_operator->getRight()->getPhysicalOperator()->hasAborted()){
                                           logical_operator->setDeviceConstraint(hype::CPU_ONLY);
                                       }
                                       
                                       
                                   }
//                                   }else if(left_dev_constr==right_dev_constr && logical_operator->dev_constr_==ANY_DEVICE){
//                                       //continue chain (but continue chain on GPU only in case no operator aborted!)
//                                       if(! (left_dev_constr==hype::GPU_ONLY 
//                                       && ( logical_operator->getLeft()->getPhysicalOperator()->hasAborted() 
//                                         || logical_operator->getRight()->getPhysicalOperator()->hasAborted() 
//                                          ) ) )
//                                       {
//                                           logical_operator->setDeviceConstraint(left_dev_constr);
//                                           std::cout << "[QC]: Operator aborted, will not continue chain!" << std::endl; 
//                                       }
//                                   }          
                               }
                            }
                        }else{
                            //unary operators
                            if(logical_operator->getLeft()->getPhysicalOperator()){
                               ProcessingDeviceType left_pt = logical_operator->getLeft()->getPhysicalOperator()->getSchedulingDecision().getDeviceSpecification().getDeviceType();
                               
                               DeviceTypeConstraint left_dev_constr = util::getDeviceConstraintForProcessingDeviceType(left_pt);                  
                               //we omit chain breakers in our placement heuristic, 
                               //and handle them differently than normal operators
                               if(!util::isChainBreaker(logical_operator->getOperationName()) && logical_operator->dev_constr_==ANY_DEVICE){
                                   //if the child is a "chain breaker" (e.g., a 
                                   //management operator pinned to the CPU such 
                                   //as Scans or rename operations),
                                   //we let HyPE decide where to begin the chain, 
                                   //either on a CPU or a co-processor
                                   if(util::isChainBreaker(logical_operator->getLeft()->getOperationName())){
                                       //logical_operator->setDeviceConstraint(hype::ANY_DEVICE);
                                   }else{
                                       //else, continue the chain
//                                       logical_operator->setDeviceConstraint(left_dev_constr);
				       //else, continue chain (but continue chain on GPU only in case no operator aborted!)

                                       //continue chain (but continue chain on GPU only in case no operator aborted!)
                                       if((left_pt==hype::GPU
                                         && !logical_operator->getLeft()->getPhysicalOperator()->hasAborted()
                                          ) )
                                       {
                                           logical_operator->setDeviceConstraint(hype::GPU_ONLY); //left_dev_constr);
                                           //std::cout << "[QC]: Operator aborted, will not continue chain!" << std::endl; 
                                       }
                                       if(left_pt==hype::CPU || logical_operator->getLeft()->getPhysicalOperator()->hasAborted()){
                                           logical_operator->setDeviceConstraint(hype::CPU_ONLY);
                                       }
                                       
                                   }  
                                       
//                                       if(! (left_dev_constr==hype::GPU_ONLY 
//                                       && logical_operator->getLeft()->getPhysicalOperator()->hasAborted()  ) ){
//                                           logical_operator->setDeviceConstraint(left_dev_constr);
//                                           std::cout << "[QC]: Operator aborted, will not continue chain!" << std::endl; 
//                                       }
                                       
                                       
                                       
                                   }
                               }
                            }
                        
#endif
                      
                        if(hype::core::Runtime_Configuration::instance().isPullBasedQueryChoppingEnabled()){
                            //queue operator in global operator stream
                            core::Scheduler::instance().addIntoGlobalOperatorStream(logical_operator);     
                        }else{
                            scheduleAndExecute(logical_operator,out);
                        }
                    }
                }
                
            }
            
            void notify(){
                {
                    boost::lock_guard<boost::mutex> lock(mutex_);
                    this->is_ready_=true;
                }
                condition_variable_.notify_one(); 
            }

            
            
            void notify(NodePtr logical_operator){
                {
                    boost::lock_guard<boost::mutex> lock(mutex_);
                    if(logical_operator==left_){
                        left_child_ready_=true;
                    }else if(logical_operator==right_){
                        right_child_ready_=true;
                    }else{
                        HYPE_FATAL_ERROR("Broken child/Parent relationship: Notification of a node that is not the parent of the notifying node!",std::cout);
                    }
                }
                condition_variable_.notify_one();
            }
            
            virtual const Tuple getFeatureVector() const{
                
                hype::Tuple t;
                if (this->left_) { //if left child is valid (has to be by convention!), add input data size
                    //if we already know the correct input data size, because the child node was already executed
                    //during query chopping, we use the real cardinality, other wise we call the estimator
                    if(this->left_->physical_operator_){
                        t.push_back(this->left_->physical_operator_->getResultSize()); // ->result_size_;
                    }else{
                        t.push_back(this->left_->getOutputResultSize());
                    }
                    if (this->right_) { //if right child is valid (not null), add input data size for it as well
                        if(this->right_->physical_operator_){
                            t.push_back(this->right_->physical_operator_->getResultSize()); // ->result_size_;
                        }else{
                            t.push_back(this->right_->getOutputResultSize());
                        }
                    }
                }else{
                    HYPE_FATAL_ERROR("Invalid Left Child!",std::cout);
                }
                if (useSelectivityEstimation())
                    t.push_back(this->getSelectivity()); //add selectivity of this operation, when use_selectivity_estimation_ is true
#ifdef HYPE_INCLUDE_INPUT_DATA_LOCALITY_IN_FEATURE_VECTOR 
                //dirty workaround! We should make isInputDataCachedInGPU() a const member function!
                t.push_back(const_cast<Node*>(this)->isInputDataCachedInGPU());
#endif
                return t;
            }
            
            virtual bool useSelectivityEstimation() const = 0;
            
            virtual bool isInputDataCachedInGPU(){
                //bool ret=true;
                if(!left_ && !right_) return false; 
                if(left_){
                    if(left_->physical_operator_ && (left_->physical_operator_->getSchedulingDecision().getDeviceSpecification().getDeviceType()!=hype::GPU || left_->physical_operator_->hasAborted())){
                        return false;
                    }else if(!right_ && left_->physical_operator_ && left_->physical_operator_->getSchedulingDecision().getDeviceSpecification().getDeviceType()==hype::GPU && !left_->physical_operator_->hasAborted()){
                        return true;
                    }
                    if(!right_){
                        if(left_->getDeviceConstraint().getDeviceTypeConstraint()==hype::GPU_ONLY){
                            return true;
                        }
                    }
                }
                if(right_){
                    if(right_->physical_operator_ && (right_->physical_operator_->getSchedulingDecision().getDeviceSpecification().getDeviceType()!=hype::GPU || right_->physical_operator_->hasAborted())){
                        return false;
                    }else if(right_->physical_operator_ && right_->physical_operator_->getSchedulingDecision().getDeviceSpecification().getDeviceType()==hype::GPU && !right_->physical_operator_->hasAborted()){
                        if(left_ && left_->physical_operator_){
                            if(left_->physical_operator_->getSchedulingDecision().getDeviceSpecification().getDeviceType()==hype::GPU && !left_->physical_operator_->hasAborted()){
                                return true;
                            }else{
                                return false;
                            }
                        }else{
                            return true;
                        }
                    }
                    if(left_->getDeviceConstraint().getDeviceTypeConstraint()==hype::GPU_ONLY 
                    && right_->getDeviceConstraint().getDeviceTypeConstraint()==hype::GPU_ONLY){
                        return true;
                    }
                }

                return false;
            }
            
            void setOutputStream(std::ostream& output_stream){
                this->out=&output_stream;
            }
            std::ostream& getOutputStream(){
                return *out;
            }
         
        private:
#ifdef HYPE_ENABLE_INTERACTION_WITH_COGADB
            virtual void produce_impl(CoGaDB::CodeGeneratorPtr code_gen, 
                CoGaDB::QueryContextPtr context){
                HYPE_FATAL_ERROR("Called unimplemented produce function in "
                        "operator '" << this->getOperationName() << "'",std::cerr);
            }          
            virtual void consume_impl(CoGaDB::CodeGeneratorPtr code_gen, 
                CoGaDB::QueryContextPtr context){
                HYPE_FATAL_ERROR("Called unimplemented consume function in "
                        "operator '" << this->getOperationName() << "'",std::cerr);                
            }
#endif              
            
        protected:
            NodePtr parent_;
            NodePtr left_;
            NodePtr right_;
            unsigned int level_;
            DeviceConstraint dev_constr_;
            
            boost::mutex mutex_;
            boost::condition_variable condition_variable_;
            volatile bool left_child_ready_;
            volatile bool right_child_ready_;     
            volatile bool is_ready_;
            OperatorPtr physical_operator_;
            std::ostream* out;

            //std::string operation_name_;
        };
        
        
        inline void Node::produce(CoGaDB::CodeGeneratorPtr code_gen, 
                CoGaDB::QueryContextPtr context){
            produce_impl(code_gen, context);
        }    
        inline void Node::consume(CoGaDB::CodeGeneratorPtr code_gen, 
                CoGaDB::QueryContextPtr context){
            consume_impl(code_gen, context);
        }         
        
//        std::string Node::toString(bool) const{
//            return this->getOperationName();
//        }

//        typedef Node::NodePtr NodePtr;

        inline bool scheduleAndExecute(NodePtr logical_operator, std::ostream* out){
                    Tuple t = logical_operator->getFeatureVector();

                    OperatorSpecification op_spec(logical_operator->getOperationName(),
                            t,
                            //parameters are the same, because in the query processing engine, we model copy operations explicitely, so the copy cost have to be zero
                            hype::PD_Memory_0, //input data is in CPU RAM
                            hype::PD_Memory_0); //output data has to be stored in CPU RAM

                    SchedulingDecision sched_dec = hype::Scheduler::instance().getOptimalAlgorithm(op_spec, logical_operator->getDeviceConstraint());
                    OperatorPtr op = logical_operator->getPhysicalOperator(sched_dec);
                    op->setLogicalOperator(logical_operator);
                    op->setOutputStream(*out);
                    return op->operator ()();
        }
        
        
        /*
        Automatic Processing Device Selector
        AProDeS

        Automatic Processing Device Selector for Coprocessing
        AProDeSCo*/

        template <typename Type>
        class TypedNode : public Node {
        public:
            typedef Type NodeElementType;
            typedef typename OperatorMapper_Helper_Template<Type>::Physical_Operator_Map Physical_Operator_Map;
            typedef typename OperatorMapper_Helper_Template<Type>::Physical_Operator_Map_Ptr Physical_Operator_Map_Ptr;
            typedef typename OperatorMapper_Helper_Template<Type>::TypedOperatorPtr TypedOperatorPtr;


            
            
            TypedNode(DeviceConstraint dev_constr) : Node(dev_constr) {

            }

            virtual hype::query_optimization::QEP_Node* toQEP_Node() = 0;
            virtual TypedOperatorPtr getOptimalOperator(TypedOperatorPtr left_child = NULL, TypedOperatorPtr right_child = NULL, DeviceTypeConstraint dev_constr = ANY_DEVICE) = 0;
            virtual TypedOperatorPtr createPhysicalOperator(TypedOperatorPtr left_child, TypedOperatorPtr right_child, const hype::SchedulingDecision& sched_dec) = 0;
            virtual Physical_Operator_Map_Ptr getPhysical_Operator_Map() = 0;

            virtual bool useSelectivityEstimation() const = 0;
            
            //virtual const Tuple getFeatureVector() const = 0;

            
            virtual ~TypedNode() {
            }

        };

        template <typename Type, typename OperatorMapper_Helper_Template<Type>::Map_Init_Function& function>
        class TypedNode_Impl : public TypedNode<Type> {
        public:

            typedef typename TypedNode<Type>::Physical_Operator_Map Physical_Operator_Map;
            typedef typename TypedNode<Type>::Physical_Operator_Map_Ptr Physical_Operator_Map_Ptr;
            typedef typename TypedNode<Type>::TypedOperatorPtr TypedOperatorPtr;
            //typedef OperatorMapper<Type,function> OperatorMapper;

            TypedNode_Impl(bool use_selectivity_estimation = false, DeviceConstraint dev_constr = DeviceConstraint()) : TypedNode<Type>(dev_constr), operator_mapper_(), use_selectivity_estimation_(use_selectivity_estimation) {
                customSelectivity = -1;
            }

            virtual ~TypedNode_Impl() {
            }

            virtual hype::query_optimization::QEP_Node* toQEP_Node(){
                hype::Tuple t = this->getFeatureVector();
//                if (this->left_) { //if left child is valid (has to be by convention!), add input data size
//                    t.push_back(this->left_->getOutputResultSize());
//                    if (this->right_) { //if right child is valid (not null), add input data size for it as well
//                        t.push_back(this->right_->getOutputResultSize());
//                    }
//                }
//                if (use_selectivity_estimation_)
//                    t.push_back(this->getSelectivity()); //add selectivity of this operation, when use_selectivity_estimation_ is true

                OperatorSpecification op_spec(this->getOperationName(),
                        t,
                        //parameters are the same, because in the query processing engine, we model copy oeprations explicitely, so the copy cost have to be zero
                        hype::PD_Memory_0, //input data is in CPU RAM
                        hype::PD_Memory_0); //output data has to be stored in CPU RAM
                
                
                hype::query_optimization::QEP_Node* node = new hype::query_optimization::QEP_Node(op_spec, this->dev_constr_,this->isInputDataCachedInGPU());
                return node; //hype::query_optimization::QEPPtr(new hype::query_optimization::QEP(node));
            }
            
            virtual bool useSelectivityEstimation() const{
                return this->use_selectivity_estimation_;
            }
            
//            virtual const Tuple getFeatureVector() const{
//                hype::Tuple t;
//                if (this->left_) { //if left child is valid (has to be by convention!), add input data size
//                    //if we already know the correct input data size, because the child node was already executed
//                    //during query chopping, we use the real cardinality, other wise we call the estimator
//                    if(this->left_->physical_operator_){
//                        t.push_back(this->left_->physical_operator_->getResultSize()); // ->result_size_;
//                    }else{
//                        t.push_back(this->left_->getOutputResultSize());
//                    }
//                    if (this->right_) { //if right child is valid (not null), add input data size for it as well
//                        t.push_back(this->right_->getOutputResultSize());
//                    }
//                }
//                if (use_selectivity_estimation_)
//                    t.push_back(this->getSelectivity()); //add selectivity of this operation, when use_selectivity_estimation_ is true
//                return t;
//            }
            
            
            virtual TypedOperatorPtr getOptimalOperator(TypedOperatorPtr left_child, TypedOperatorPtr right_child, DeviceTypeConstraint dev_constr) {
                hype::Tuple t = this->getFeatureVector();
//                if (this->left_) { //if left child is valid (has to be by convention!), add input data size
//                    t.push_back(this->left_->getOutputResultSize());
//                    if (this->right_) { //if right child is valid (not null), add input data size for it as well
//                        t.push_back(this->right_->getOutputResultSize());
//                    }
//                }
//                if (use_selectivity_estimation_)
//                    t.push_back(this->getSelectivity()); //add selectivity of this operation, when use_selectivity_estimation_ is true

                return operator_mapper_.getPhysicalOperator(*this, t, left_child, right_child, dev_constr); //this->getOperationName(), t, left_child, right_child);
            }

            //this method allows an optimizer to create a physical operator based on its final physical plan
            virtual TypedOperatorPtr createPhysicalOperator(TypedOperatorPtr left_child, TypedOperatorPtr right_child, const hype::SchedulingDecision& sched_dec) {
                return operator_mapper_.createPhysicalOperator(*this, sched_dec, left_child, right_child);
            }
            
            virtual Physical_Operator_Map_Ptr getPhysical_Operator_Map() {
                return OperatorMapper<Type, function>::static_algorithm_name_to_physical_operator_map_ptr;
            }
          
            double getSelectivity() const {
                if (customSelectivity != -1) {
                    return customSelectivity;
                } else {
                    return getCalculatedSelectivity();
                }
            }

            void setSelectivity(double selectivity) {
                customSelectivity = selectivity;
            }
            //boost::shared_ptr<> toPhysicalNode();
      
            virtual OperatorPtr getPhysicalOperator(){
                return this->physical_operator_; 
            }     
            
            virtual OperatorPtr getPhysicalOperator(const SchedulingDecision& sched_dec){
                if(this->physical_operator_){
                    return this->physical_operator_;
                }else{
                    OperatorPtr left;
                    OperatorPtr right;
                    
                    if(this->left_) left = this->left_->getPhysicalOperator();
                    if(this->right_) right = this->right_->getPhysicalOperator(); 
                    
                    TypedOperatorPtr typed_left_child=boost::dynamic_pointer_cast<TypedOperator<Type> >(left);
                    TypedOperatorPtr typed_right_child=boost::dynamic_pointer_cast<TypedOperator<Type> >(right);
                    TypedOperatorPtr result = this->createPhysicalOperator(typed_left_child, typed_right_child, sched_dec);
                    this->physical_operator_=result;
                    return result;
                }
            }
            
        protected:
            OperatorMapper<Type, function> operator_mapper_;
            bool use_selectivity_estimation_;
            double customSelectivity;

            virtual double getCalculatedSelectivity() const {
                return 0.1;
            }
        };

    }; //end namespace queryprocessing
}; //end namespace hype
