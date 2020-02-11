
/** \example online_learning_manual_time_measurement.cpp
 * This is an example of how to use the framework. This example tests the learning procedure. Two simple algorithms wait different times depending on their input data sizes. Note that one algorthm (CPU) is faster for little data sets and one is faster for big data sets (GPU). Note that the time measurement can be done manually. In this case, the user is responsible to pass the measured execution time back to HyPE using \ref hype::Scheduler::addObservation.
 */


#include <iostream>
#include <hype.hpp>

#include <boost/thread.hpp>

using namespace hype;
using namespace std;



void CPU_algorithm(size_t data_size){
	boost::this_thread::sleep(boost::posix_time::milliseconds(data_size ));

}

void GPU_algorithm(size_t data_size){
	boost::this_thread::sleep(boost::posix_time::milliseconds((data_size/2)+50 ));

}

int main(){
	
   Scheduler& scheduler=Scheduler::instance();


	AlgorithmSpecification cpu_alg("CPU_Algorithm",
											 "SORT",
											 hype::Least_Squares_1D,
											 hype::Periodic,
											 hype::ResponseTime);

	DeviceSpecification cpu_dev_spec(hype::PD0, //by convention, the first CPU has Device ID: PD0  (any system has at least one)
											   hype::CPU, //a CPU is from type CPU
											   hype::PD_Memory_0); //by convention, the host main memory has ID PD_Memory_0

	AlgorithmSpecification gpu_alg("GPU_Algorithm",
											 "SORT",
											 hype::Least_Squares_1D,
											 hype::Periodic,
											 hype::ResponseTime);

	DeviceSpecification gpu_dev_spec(hype::PD1, //different porcessing device (naturally)
											   hype::GPU, //Device Type
											   hype::PD_Memory_1); //seperate device memory

	scheduler.addAlgorithm(cpu_alg, cpu_dev_spec);
	scheduler.addAlgorithm(gpu_alg, gpu_dev_spec);


//	if(!scheduler.setOptimizationCriterion("SORT","Simple Round Robin")) 
//		std::cout << "Error: Could not set Optimization Criterion!" << std::endl;	else cout << "Success..." << endl;

	if(!scheduler.setOptimizationCriterion("SORT","Response Time")) 
		std::cout << "Error: Could not set Optimization Criterion!" << std::endl;	else cout << "Success..." << endl;

//	if(!scheduler.setOptimizationCriterion("SORT","WaitingTimeAwareResponseTime")) 
//		std::cout << "Error: Could not set Optimization Criterion!" << std::endl;	else cout << "Success..." << endl;

//	if(!scheduler.setOptimizationCriterion("SORT","Throughput")) 
//		std::cout << "Error: Could not set Optimization Criterion!" << std::endl;	else cout << "Success..." << endl;

//	if(!scheduler.setOptimizationCriterion("SORT","ProbabilityBasedOutsourcing")) 
//		std::cout << "Error: Could not set Optimization Criterion!" << std::endl;	else cout << "Success..." << endl;


	for(int i=0;i<500;i++){
	core::Tuple t;
	t.push_back(rand()%300);
	uint64_t global_begin = core::getTimestamp();

	OperatorSpecification op_spec("SORT", 
											t,
											hype::PD_Memory_0, //input data is in CPU RAM
											hype::PD_Memory_0); //output data has to be stored in CPU RAM

	DeviceConstraint dev_constr;


	SchedulingDecision sched_dec = scheduler.getOptimalAlgorithm(op_spec, dev_constr);
	cout << "Line: " << i << " ";
	if(sched_dec.getNameofChoosenAlgorithm()=="CPU_Algorithm"){

		uint64_t begin=hype::core::getTimestamp();
		CPU_algorithm(t[0]);
		uint64_t end=hype::core::getTimestamp();
		assert(end>begin);
		//scheduling decision and measured execution time in nanoseconds!!!
		scheduler.addObservation(sched_dec,end-begin);
		

		//alg_measure.afterAlgorithmExecution(); 
	}else if(sched_dec.getNameofChoosenAlgorithm()=="GPU_Algorithm"){

		uint64_t begin=hype::core::getTimestamp();
		GPU_algorithm(t[0]);
		uint64_t end=hype::core::getTimestamp();
		assert(end>begin);
		//scheduling decision and measured execution time in nanoseconds!!!
		scheduler.addObservation(sched_dec,end-begin);

	}
	
	uint64_t global_end = core::getTimestamp();

	uint64_t result = global_end-global_begin;
	if(global_begin>global_end) cout << "Fatal Error" << endl; 
	cout << result << endl;
	}

	
	//string algorithm_name = sched_dec.getNameofChoosenAlgorithm();

 return 0;
};

