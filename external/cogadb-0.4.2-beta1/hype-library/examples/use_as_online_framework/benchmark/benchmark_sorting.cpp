

#include "definitions.hpp"

#include <algorithm>
#include <cstdlib>
#include <iostream>
#include <fstream>
#include <cassert>
#include <vector>

#include <stdint.h>

#include <tbb/parallel_sort.h>
#include <tbb/task_scheduler_init.h>

#include <boost/chrono.hpp>

#include <sys/mman.h>

#include <hype.hpp>

#include <config/configuration.hpp>
#include <query_processing/operator.hpp>
#include <query_processing/processing_device.hpp>

//using namespace std;
using namespace boost::chrono;


	enum SchedulingConfiguration{CPU_ONLY,GPU_ONLY,HYBRID};


        uint64_t getTimestamp()
        {
		high_resolution_clock::time_point tp = high_resolution_clock::now();
		nanoseconds dur = tp.time_since_epoch();

		return (uint64_t)dur.count();
        }

struct Random_Number_Generator{

	Random_Number_Generator(unsigned int max_value_size) : max_value_size_(max_value_size){}

	ElementType operator() (){
		return (ElementType) rand()%max_value_size_;
	}
	private:
		unsigned int max_value_size_;
};

enum Architecture{Architecture_32Bit,Architecture_64Bit};

Architecture getArchitecture(){
#ifdef __LP64__
  //64-bit Intel or PPC
  //#warning "Compiling for 64 Bit"
  return Architecture_64Bit;
#else
  //32-bit Intel, PPC or ARM
  //#warning "Compiling for 32 Bit"
  return Architecture_32Bit;
#endif
}


void CPU_Sort(VecPtr dataset){
	assert(dataset!=NULL);
	Vec data_copy(dataset->begin(),dataset->end());
	std::sort(data_copy.begin(),data_copy.end());
}

void CPU_Sort_Parallel(VecPtr dataset){
	assert(dataset!=NULL);
	//tbb::task_scheduler_init init(4);
	Vec data_copy(dataset->begin(),dataset->end());
	tbb::parallel_sort(data_copy.begin(),data_copy.end());
}

void GPU_Sort(VecPtr dataset);

//void GPU_Sort(VecPtr dataset){
//	assert(dataset!=NULL);
//	// transfer data to the device
//  thrust::device_vector<ElementType> d_vec (dataset->begin(),dataset->end());

//  // sort data on the device (846M keys per second on GeForce GTX 480)
//  thrust::sort(d_vec.begin(), d_vec.end());

//  // transfer data back to host
//  thrust::copy(d_vec.begin(), d_vec.end(), dataset->begin());
//}

	class CPU_Serial_Sort_Operator : public hype::queryprocessing::Operator{
		public:
		CPU_Serial_Sort_Operator(const hype::SchedulingDecision& sd, VecPtr input_data) : Operator(sd), input_data_(input_data){

		}

		virtual bool execute(){
			CPU_Sort(input_data_);
			return true;
		}

		VecPtr input_data_;		
	};

	class CPU_Parallel_Sort_Operator : public hype::queryprocessing::Operator{
		public:
		CPU_Parallel_Sort_Operator(const hype::SchedulingDecision& sd, VecPtr input_data) : Operator(sd), input_data_(input_data){

		}

		virtual bool execute(){
			CPU_Sort_Parallel(input_data_);
			return true;
		}

		VecPtr input_data_;		
	};

	class GPU_Sort_Operator : public hype::queryprocessing::Operator{
		public:
		GPU_Sort_Operator(const hype::SchedulingDecision& sd, VecPtr input_data) : Operator(sd), input_data_(input_data){

		}

		virtual bool execute(){
			//std::cout << "[GPU_Sort_Operator] Exec GPU Sort" << std::endl;
			GPU_Sort(input_data_);
			return true;
		}

		VecPtr input_data_;		
	};


VecPtr generate_dataset(unsigned int size_in_number_of_elements){
	VecPtr data(new Vec());
	for(unsigned int i=0;i<size_in_number_of_elements;i++){
		ElementType e = (ElementType) rand();
		data->push_back(e);
	}
	assert(data!=NULL);
	//std::cout << "created new data set: " << data.get() << " of size: " << data->size() << std::endl;
	return data;
}

vector<VecPtr> generate_random_datasets(unsigned int max_size_in_number_of_elements, unsigned int number_of_datasets){
	vector<VecPtr> datasets;
		for(unsigned int i=0;i<number_of_datasets;i++){
			VecPtr vec_ptr = generate_dataset((unsigned int) (rand()%max_size_in_number_of_elements) );
			assert(vec_ptr!=NULL);
			datasets.push_back(vec_ptr);
		}
	return datasets;
}

int main(int argc, char* argv[]){

	//we don't want the OS to swap out our data to disc that's why we lock it
	mlockall(MCL_CURRENT|MCL_FUTURE);

//	tbb::task_scheduler_init init(8);
	

	//tbb::task_scheduler_init (2);
//	tbb::task_scheduler_init init(1);
//	


	cout << "TBB use " << tbb::task_scheduler_init::default_num_threads() << " number of threads as a default" << endl;
	unsigned int MAX_DATASET_SIZE_IN_MB=10; //MB  //(10*1000*1000)/sizeof(int); //1000000;
	unsigned int NUMBER_OF_DATASETS=10; //3; //100;
	unsigned int NUMBER_OF_SORT_OPERATIONS_IN_WORKLOAD=100; //3; //1000;
	unsigned int RANDOM_SEED=0;
	//unsigned int READY_QUEUE_LENGTH=100;

	SchedulingConfiguration sched_config=HYBRID; //CPU_ONLY,GPU_ONLY,HYBRID
	//SchedulingConfiguration sched_config=GPU_ONLY;
	//SchedulingConfiguration sched_config=CPU_ONLY;

	std::string stemod_optimization_criterion="Response Time";
	std::string stemod_statistical_method="Least Squares 1D";
	std::string stemod_recomputation_heuristic="Periodic Recomputation";

// Declare the supported options.
boost::program_options::options_description desc("Allowed options");
desc.add_options()
    ("help", "produce help message")
    ("number_of_datasets", boost::program_options::value<unsigned int>(), "set the number of data sets for workload")
    ("number_of_operations", boost::program_options::value<unsigned int>(), "set the number of operations in workload")
    ("max_dataset_size_in_MB", boost::program_options::value<unsigned int>(), "set the maximal dataset size in MB")
	 //("ready_queue_length", boost::program_options::value<unsigned int>(), "set the queue length of operators that may be concurrently scheduled (clients are blocked on a processing device)")
    ("scheduling_method", boost::program_options::value<std::string>(), "set the decision model (CPU_ONLY, GPU_ONLY, HYBRID)")
    ("random_seed", boost::program_options::value<unsigned int>(), "seed to use before for generating datasets and operation workload")
    ("optimization_criterion", boost::program_options::value<std::string>(), "set the decision models optimization_criterion for all algorithms")
    ("statistical_method", boost::program_options::value<std::string>(), "set the decision models statistical_method for all algorithms")
    ("recomputation_heuristic", boost::program_options::value<std::string>(), "set the decision models recomputation_heuristic for all algorithms")
;

boost::program_options::variables_map vm;
boost::program_options::store(boost::program_options::parse_command_line(argc, argv, desc), vm);
boost::program_options::notify(vm);    

if (vm.count("help")) {
    cout << desc << "\n";
    return 1;
}

if (vm.count("number_of_datasets")) {
    cout << "Number of Datasets: " 
 << vm["number_of_datasets"].as<unsigned int>() << "\n";
	NUMBER_OF_DATASETS=vm["number_of_datasets"].as<unsigned int>();
} else {
    cout << "number_of_datasets was not specified, using default value...\n";
}

if (vm.count("number_of_operations")) {
    cout << "Number of Operations: " 
 << vm["number_of_operations"].as<unsigned int>() << "\n";
	NUMBER_OF_SORT_OPERATIONS_IN_WORKLOAD=vm["number_of_operations"].as<unsigned int>();
} else {
    cout << "number_of_operations was not specified, using default value...\n";
}

if (vm.count("max_dataset_size_in_MB")) {
    cout << "max_dataset_size_in_MB: " 
 << vm["max_dataset_size_in_MB"].as<unsigned int>() << "MB \n";
	MAX_DATASET_SIZE_IN_MB=vm["max_dataset_size_in_MB"].as<unsigned int>(); //*1024*1024)/sizeof(int); //convert value in MB to equivalent number of integer elements
} else {
    cout << "max_dataset_size_in_MB was not specified, using default value...\n";
}

if (vm.count("random_seed")) {
    cout << "Random Seed: " 
 << vm["random_seed"].as<unsigned int>() << "\n";
	RANDOM_SEED=vm["random_seed"].as<unsigned int>();
} else {
    cout << "random_seed was not specified, using default value...\n";
}


if (vm.count("scheduling_method")) {
    cout << "scheduling_method: " 
 << vm["scheduling_method"].as<std::string>() << "\n";
	std::string scheduling_method=vm["scheduling_method"].as<std::string>();
	if(scheduling_method=="CPU_ONLY"){
		sched_config=CPU_ONLY;
	}else if(scheduling_method=="GPU_ONLY"){
		sched_config=GPU_ONLY;
	}else if(scheduling_method=="HYBRID"){
		sched_config=HYBRID;
	}
	
} else {
    cout << "scheduling_method was not specified, using default value...\n";
}

if (vm.count("optimization_criterion")) {
    cout << "optimization_criterion: " 
 << vm["optimization_criterion"].as<std::string>() << "\n";
	stemod_optimization_criterion=vm["optimization_criterion"].as<std::string>();

	if(sched_config!=HYBRID){
		cout << "Specification of STEMOD Parameter needs hybrid scheduling (scheduling_method=HYBRID)" << endl;
		return -1;
	}

} else {
    cout << "optimization_criterion was not specified, using default value...\n";
}

if (vm.count("statistical_method")) {
    cout << "statistical_method: " 
 << vm["statistical_method"].as<std::string>() << "\n";
	stemod_statistical_method=vm["statistical_method"].as<std::string>();
	if(sched_config!=HYBRID){
		cout << "Specification of STEMOD Parameter needs hybrid scheduling (scheduling_method=HYBRID)" << endl;
		return -1;
	}

} else {
    cout << "statistical_method was not specified, using default value...\n";
}

if (vm.count("recomputation_heuristic")) {
    cout << "recomputation_heuristic: " 
 << vm["recomputation_heuristic"].as<std::string>() << "\n";
	stemod_recomputation_heuristic=vm["recomputation_heuristic"].as<std::string>();
	if(sched_config!=HYBRID){
		cout << "Specification of STEMOD Parameter needs hybrid scheduling (scheduling_method=HYBRID)" << endl;
		return -1;
	}

} else {
    cout << "recomputation_heuristic was not specified, using default value...\n";
}

/*
if (vm.count("ready_queue_length")) {
    cout << "Ready Queue Length: " 
 << vm["ready_queue_length"].as<std::string>() << "\n";
	READY_QUEUE_LENGTH=vm["ready_queue_length"].as<unsigned int>();
	if(sched_config!=HYBRID){
		cout << "Specification of STEMOD Parameter needs hybrid scheduling (scheduling_method=HYBRID)" << endl;
		return -1;
	}

} else {
    cout << "ready_queue_length was not specified, using default value...\n";
}*/





//"if (vm.count(\"$VAR\")) {
//    cout << \"$VAR: \" 
// << vm[\"$VAR\"].as<std::string>() << \"\n\";
//	std::string s=vm[\"$VAR\"].as<std::string>();

//	
//} else {
//    cout << \"$VAR was not specified, using default value...\n\";
//}"




	srand(RANDOM_SEED);

	cout << "Generating Data sets..." << endl;
	cout << "Estimated RAM usage: " << MAX_DATASET_SIZE_IN_MB*NUMBER_OF_DATASETS << "MB" << endl;
	if(MAX_DATASET_SIZE_IN_MB*NUMBER_OF_DATASETS>1024*3.7 && getArchitecture()==Architecture_32Bit){
		cout << "Memory for Datasets to generate exceeds 32 bit adress space! (" << MAX_DATASET_SIZE_IN_MB*NUMBER_OF_DATASETS << "MB)" << endl;
		return -1; 
	}
	//generate_random_datasets expects data size in number of integer elements, while MAX_DATASET_SIZE_IN_MB specifies data size in Mega Bytes
	vector<VecPtr> datasets=generate_random_datasets( (MAX_DATASET_SIZE_IN_MB*1024*1024)/sizeof(int), NUMBER_OF_DATASETS);
	vector<unsigned int> query_indeces(NUMBER_OF_SORT_OPERATIONS_IN_WORKLOAD);

	std::generate(query_indeces.begin(), query_indeces.end(), Random_Number_Generator(NUMBER_OF_DATASETS));

	//std::copy(query_indeces.begin(), query_indeces.end(), std::ostream_iterator<unsigned int>(std::cout, "\n"));

	//setup STEMOD
	//stemod::Scheduler::instance().addAlgorithm("SORT","CPU_Algorithm_serial","Least Squares 1D","Periodic Recomputation");
	hype::Scheduler::instance().addAlgorithm("SORT","CPU_Algorithm_parallel", hype::CPU, "Least Squares 1D", "Periodic Recomputation");
	hype::Scheduler::instance().addAlgorithm("SORT","GPU_Algorithm", hype::GPU, "Least Squares 1D", "Periodic Recomputation");

	cout << "Setting Optimization Criterion '" << stemod_optimization_criterion << "'...";
	if(!hype::Scheduler::instance().setOptimizationCriterion("SORT",stemod_optimization_criterion)){ 
		std::cout << "Error: Could not set '" << stemod_optimization_criterion << "' as Optimization Criterion!" << std::endl; 	return -1;}
	else cout << "Success..." << endl;
	//if(!scheduler.setOptimizationCriterion("MERGE","Throughput")) std::cout << "Error" << std::endl;

	if(!hype::Scheduler::instance().setStatisticalMethod("CPU_Algorithm_parallel",stemod_statistical_method)){ 
		std::cout << "Error" << std::endl; return -1;
	} else cout << "Success..." << endl;
	if(!hype::Scheduler::instance().setStatisticalMethod("GPU_Algorithm",stemod_statistical_method)){ 
		std::cout << "Error" << std::endl; return -1;
	} else cout << "Success..." << endl;

	if(!hype::Scheduler::instance().setRecomputationHeuristic("CPU_Algorithm_parallel",stemod_recomputation_heuristic)){ 
		std::cout << "Error" << std::endl; return -1;
	}	else cout << "Success..." << endl;
	if(!hype::Scheduler::instance().setRecomputationHeuristic("GPU_Algorithm",stemod_recomputation_heuristic)){ 
		std::cout << "Error" << std::endl; return -1;
	} else cout << "Success..." << endl;


	hype::queryprocessing::ProcessingDevice& cpu = hype::queryprocessing::getProcessingDevice(hype::CPU);
	hype::queryprocessing::ProcessingDevice& gpu = hype::queryprocessing::getProcessingDevice(hype::GPU);
		
	cpu.start();
	gpu.start();

	//boost::this_thread::sleep( boost::posix_time::seconds(30) );

	cout << "Starting Benchmark..." << endl;

	uint64_t begin_benchmark_timestamp = getTimestamp();
	uint64_t end_training_timestamp=0;

	for(unsigned int i=0;i<NUMBER_OF_SORT_OPERATIONS_IN_WORKLOAD;i++){
		unsigned int index = query_indeces[i];
		VecPtr dataset = datasets[index];
	
		assert(NUMBER_OF_SORT_OPERATIONS_IN_WORKLOAD==query_indeces.size());
		assert(index<NUMBER_OF_DATASETS); //NUMBER_OF_SORT_OPERATIONS_IN_WORKLOAD);	
	
		hype::Tuple t;
		t.push_back(dataset->size());
		//stemod::SchedulingDecision sched_dec_local("",stemod::core::EstimatedTime(0),t);
	

		//cout << "RUN: " << i << "/" << NUMBER_OF_SORT_OPERATIONS_IN_WORKLOAD << endl;
		
		if(sched_config==HYBRID){ //CPU_ONLY,GPU_ONLY,HYBRID)
			//cout << "scheduling operator " << i << endl;
			const unsigned int number_of_training_operations = (hype::core::Runtime_Configuration::instance().getTrainingLength()*2)+1; //*number of algortihms per operation (2)
			if(number_of_training_operations==i){
				if(!hype::core::quiet)
					cout << "waiting for training to complete" << endl;
					//wait until training operations finished
					while(!cpu.isIdle() || !gpu.isIdle()){
						boost::this_thread::sleep(boost::posix_time::microseconds(20));
					}	
					end_training_timestamp = getTimestamp();
					//cout << "stat: cpu " << !cpu.isIdle() << " gpu " << !gpu.isIdle() << endl; 
				if(!hype::core::quiet)
					cout << "training completed! Time: " << end_training_timestamp-begin_benchmark_timestamp << "ns (" 
						  << double(end_training_timestamp-begin_benchmark_timestamp)/(1000*1000*1000) <<"s)" << endl;
			} 
		
			hype::SchedulingDecision sched_dec = hype::Scheduler::instance().getOptimalAlgorithmName("SORT",t);
			
			//cout << "Estimated Time: " << sched_dec.getEstimatedExecutionTimeforAlgorithm().getTimeinNanoseconds() << endl;
		//cout << "Decision: " << sched_dec.getNameofChoosenAlgorithm() << endl;

//		if(sched_dec.getNameofChoosenAlgorithm()=="CPU_Algorithm_serial"){
//			stemod::AlgorithmMeasurement alg_measure(sched_dec);
//			CPU_Sort(dataset);
//			alg_measure.afterAlgorithmExecution(); 
//		}else if(sched_dec.getNameofChoosenAlgorithm()=="CPU_Algorithm_parallel"){
//			stemod::AlgorithmMeasurement alg_measure(sched_dec);
//			CPU_Sort_Parallel(dataset);
//			alg_measure.afterAlgorithmExecution(); 
//		}else if(sched_dec.getNameofChoosenAlgorithm()=="GPU_Algorithm"){
//			stemod::AlgorithmMeasurement alg_measure(sched_dec);
//			GPU_Sort(dataset);
//			alg_measure.afterAlgorithmExecution(); 
//		}

		if(sched_dec.getNameofChoosenAlgorithm()=="CPU_Algorithm_serial"){
			cpu.addOperator( boost::shared_ptr<CPU_Serial_Sort_Operator>( new CPU_Serial_Sort_Operator(sched_dec, dataset) ) );
//			stemod::AlgorithmMeasurement alg_measure(sched_dec);
//			CPU_Sort(dataset);
//			alg_measure.afterAlgorithmExecution(); 
		}else if(sched_dec.getNameofChoosenAlgorithm()=="CPU_Algorithm_parallel"){
			cpu.addOperator( boost::shared_ptr<CPU_Parallel_Sort_Operator>( new CPU_Parallel_Sort_Operator(sched_dec, dataset) ) );
//			stemod::AlgorithmMeasurement alg_measure(sched_dec);
//			CPU_Sort_Parallel(dataset);
//			alg_measure.afterAlgorithmExecution(); 
		}else if(sched_dec.getNameofChoosenAlgorithm()=="GPU_Algorithm"){
			gpu.addOperator( boost::shared_ptr<GPU_Sort_Operator>( new GPU_Sort_Operator(sched_dec, dataset) ) );
//			stemod::AlgorithmMeasurement alg_measure(sched_dec);
//			GPU_Sort(dataset);
//			alg_measure.afterAlgorithmExecution(); 
		}

		}else if(sched_config==CPU_ONLY){
			CPU_Sort_Parallel(dataset);
			//std::cout << "Assigning Operator to CPU... " << std::endl;
			//cpu.addOperator( boost::shared_ptr<CPU_Parallel_Sort_Operator>( new CPU_Parallel_Sort_Operator(sched_dec_local, dataset) ) );
		}else if(sched_config==GPU_ONLY){
			GPU_Sort(dataset);
			//std::cout << "Assigning Operator to GPU... " << std::endl;
			//gpu.addOperator( boost::shared_ptr<GPU_Sort_Operator>( new GPU_Sort_Operator(sched_dec_local, dataset) ) );
		}

	}

//	boost::this_thread::sleep( boost::posix_time::seconds(3) );

//	cpu.stop();
//	gpu.stop();

	while(!cpu.isIdle() || !gpu.isIdle()){

	}
	uint64_t end_benchmark_timestamp = getTimestamp();
	cout << "stat: cpu " << !cpu.isIdle() << " gpu " << !gpu.isIdle() << endl; 
	cout << "[Main Thread] Processing Devices finished..." << endl;

	cpu.stop();
	gpu.stop();


	//if one of the following assertiosn are not fulfilled, then abort, because results are rubbish
	assert(end_benchmark_timestamp>=begin_benchmark_timestamp);
	double time_for_training_phase=0;
	double relative_error_cpu_parallel_algorithm = 0;
	double relative_error_gpu_algorithm	= 0;

	if(sched_config==HYBRID){ //a training phase only exists when the decision model is used
		assert(end_training_timestamp>=begin_benchmark_timestamp);
		assert(end_benchmark_timestamp>=end_training_timestamp);
		time_for_training_phase=end_training_timestamp-begin_benchmark_timestamp;
		relative_error_cpu_parallel_algorithm = hype::Report::instance().getRelativeEstimationError("CPU_Algorithm_parallel_0");
		relative_error_gpu_algorithm = hype::Report::instance().getRelativeEstimationError("GPU_Algorithm_1");
	}

	cout << "Time for Training: " << time_for_training_phase << "ns (" 
						  << double(time_for_training_phase)/(1000*1000*1000) <<"s)" << endl;
	
	cout << "Time for Workload: " <<  end_benchmark_timestamp-begin_benchmark_timestamp << "ns (" 
		  << double(end_benchmark_timestamp-begin_benchmark_timestamp)/(1000*1000*1000) << "s)" << endl;



	double total_time_cpu=cpu.getTotalProcessingTime();
	double total_time_gpu=gpu.getTotalProcessingTime();
	double total_processing_time_forall_devices=total_time_cpu + total_time_gpu;

	unsigned int total_dataset_size_in_bytes = 0;
	
	for(unsigned int i=0;i<datasets.size();i++){
		total_dataset_size_in_bytes += datasets[i]->size()*sizeof(ElementType);
	}
	
	double percentaged_execution_time_on_cpu=0;
	double percentaged_execution_time_on_gpu=0;

	if(total_processing_time_forall_devices>0){
		percentaged_execution_time_on_cpu = total_time_cpu/total_processing_time_forall_devices;
		percentaged_execution_time_on_gpu = total_time_gpu/total_processing_time_forall_devices;
	}



	cout << "Time for CPU: " <<  total_time_cpu  << "ns \tTime for GPU: " << total_time_gpu << "ns" << endl 
		  << "CPU Utilization: " <<  percentaged_execution_time_on_cpu << endl
		  << "GPU Utilization: " <<  percentaged_execution_time_on_gpu << endl;

	cout << "Relative Error CPU_Algorithm_parallel: " << relative_error_cpu_parallel_algorithm << endl;
	cout << "Relative Error GPU_Algorithm: " << relative_error_gpu_algorithm	 << endl;
	
	cout << "Total Size of Datasets: " << total_dataset_size_in_bytes << " Byte (" << total_dataset_size_in_bytes/(1024*1024) << "MB)" << endl;



 cout << MAX_DATASET_SIZE_IN_MB << "\t"
		<< NUMBER_OF_DATASETS << "\t"
		<< NUMBER_OF_SORT_OPERATIONS_IN_WORKLOAD << "\t"
		<< sched_config << "\t"
		<< total_dataset_size_in_bytes << "\t"
		<< RANDOM_SEED << "\t"
		<< stemod_optimization_criterion << "\t"
		<< stemod_statistical_method << "\t"
		<< stemod_recomputation_heuristic << "\t"
		<< hype::core::Runtime_Configuration::instance().getMaximalReadyQueueLength() << "\t"
		<< hype::core::Runtime_Configuration::instance().getHistoryLength() << "\t"
		<< hype::core::Runtime_Configuration::instance().getRecomputationPeriod() << "\t"
		<< hype::core::Runtime_Configuration::instance().getTrainingLength() << "\t"
		<< hype::core::Runtime_Configuration::instance().getOutlinerThreshold()<< "\t"
		<< hype::core::Runtime_Configuration::instance().getMaximalSlowdownOfNonOptimalAlgorithm() << "\t"		
		<< end_benchmark_timestamp-begin_benchmark_timestamp << "\t"
		<< time_for_training_phase << "\t"
		<< total_time_cpu << "\t"
		<< total_time_gpu << "\t"
		<< percentaged_execution_time_on_cpu << "\t"
		<< percentaged_execution_time_on_gpu << "\t"
		<< relative_error_cpu_parallel_algorithm << "\t"
		<< relative_error_gpu_algorithm		
		<< endl;

 std::fstream file("benchmark_results.log",std::ios_base::out | std::ios_base::app);

	file.seekg(0, ios::end); // put the "cursor" at the end of the file
	unsigned int file_length = file.tellg(); // find the position of the cursor

 if(file_length==0){ //if file empty, write header
 file << "MAX_DATASET_SIZE_IN_MB" << "\t"
		<< "NUMBER_OF_DATASETS" << "\t"
		<< "NUMBER_OF_SORT_OPERATIONS_IN_WORKLOAD" << "\t"
		<< "sched_config" << "\t"
		<< "total_size_of_datasets_in_bytes" << "\t"		
		<< "RANDOM_SEED" << "\t"
		<< "stemod_optimization_criterion" << "\t"
		<< "stemod_statistical_method" << "\t"
		<< "stemod_recomputation_heuristic" << "\t"
		<< "stemod_maximal_ready_queue_length" << "\t"
		<< "stemod_history_length" << "\t"
		<< "stemod_recomputation_period" << "\t"
		<< "stemod_length_of_training_phase" << "\t"
		<< "stemod_outliner_threshold_in_percent" << "\t"		
		<< "stemod_maximal_slowdown_of_non_optimal_algorithm" << "\t"	
		<< "workload_execution_time_in_ns" << "\t"
		<< "execution_time_training_only_in_ns" << "\t"		
		<< "total_time_cpu"  << "\t"
		<< "total_time_gpu"  << "\t"
		<< "spent_time_on_cpu_in_percent"  << "\t"
		<< "spent_time_on_gpu_in_percent"  << "\t"
		<< "average_estimation_error_CPU_Algorithm_parallel" << "\t"
		<< "average_estimation_error_GPU_Algorithm" 		
		<< endl;
	}

 file << MAX_DATASET_SIZE_IN_MB << "\t"
		<< NUMBER_OF_DATASETS << "\t"
		<< NUMBER_OF_SORT_OPERATIONS_IN_WORKLOAD << "\t"
		<< sched_config << "\t"
		<< total_dataset_size_in_bytes << "\t"		
		<< RANDOM_SEED << "\t"
		<< stemod_optimization_criterion << "\t"
		<< stemod_statistical_method << "\t"
		<< stemod_recomputation_heuristic << "\t"
		<< hype::core::Runtime_Configuration::instance().getMaximalReadyQueueLength() << "\t"
		<< hype::core::Runtime_Configuration::instance().getHistoryLength() << "\t"
		<< hype::core::Runtime_Configuration::instance().getRecomputationPeriod() << "\t"
		<< hype::core::Runtime_Configuration::instance().getTrainingLength() << "\t"
		<< hype::core::Runtime_Configuration::instance().getOutlinerThreshold()<< "\t"
		<< hype::core::Runtime_Configuration::instance().getMaximalSlowdownOfNonOptimalAlgorithm() << "\t"	
		<< end_benchmark_timestamp-begin_benchmark_timestamp << "\t"
		<< time_for_training_phase << "\t"		
		<< total_time_cpu << "\t"
		<< total_time_gpu << "\t"
		<< percentaged_execution_time_on_cpu << "\t"
		<< percentaged_execution_time_on_gpu  << "\t"
		<< relative_error_cpu_parallel_algorithm << "\t"
		<< relative_error_gpu_algorithm 			
		<< endl;

 file.close();

 return 0;
}

