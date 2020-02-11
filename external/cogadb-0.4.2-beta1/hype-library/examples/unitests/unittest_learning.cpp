

#include <iostream>
#include <stdlib.h>
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

	scheduler.addAlgorithm("SORT","CPU_Algorithm",CPU,"Least Squares 1D","Periodic Recomputation");
	scheduler.addAlgorithm("SORT","GPU_Algorithm",GPU,"Least Squares 1D","Periodic Recomputation");


	for(int i=0;i<100;i++){
	Tuple t;
	t.push_back(rand()%300);
	uint64_t begin = core::getTimestamp();
	SchedulingDecision sched_dec = scheduler.getOptimalAlgorithmName("SORT",t);
	cout << "Line: " << i << " ";
	if(sched_dec.getNameofChoosenAlgorithm()=="CPU_Algorithm"){
		AlgorithmMeasurement alg_measure(sched_dec);
		CPU_algorithm(t[0]);
		alg_measure.afterAlgorithmExecution(); 
	}else if(sched_dec.getNameofChoosenAlgorithm()=="GPU_Algorithm"){
		AlgorithmMeasurement alg_measure(sched_dec);
		GPU_algorithm(t[0]);
		alg_measure.afterAlgorithmExecution(); 
	}
	
	uint64_t end = core::getTimestamp();

	uint64_t result = end-begin;
	if(begin>end) {
		cout << "Fatal Error" << endl;
		abort();
	}
	cout << result << endl;
	}

	
	//string algorithm_name = sched_dec.getNameofChoosenAlgorithm();

 return 0;
};

