


#include <stdlib.h>

#include <hype.hpp>

using namespace hype;
using namespace std;

int main(){

   Scheduler& scheduler=Scheduler::instance();

//	scheduler.addAlgorithm("SORT","BubbleSort","LEAST_SQUARES","PERIODIC");
//	scheduler.addAlgorithm("SORT","MergeSort","LEAST_SQUARES","EVENTBASED");
//	scheduler.addAlgorithm("SORT","QuickSort","SPLINES","HYBRID");

	scheduler.addAlgorithm("SORT","Bubblesort",CPU,"Least Squares 1D","Periodic Recomputation");
	scheduler.addAlgorithm("SORT","Mergesort",CPU,"Least Squares 1D","Periodic Recomputation");
	scheduler.addAlgorithm("SORT","Quicksort",CPU,"Least Squares 1D","Periodic Recomputation");

	if(!scheduler.setOptimizationCriterion("SORT","Response Time")) std::cout << "Error: Could not set Response Time as Optimization Criterion!" << std::endl; 			else cout << "Success..." << endl;
	//if(!scheduler.setOptimizationCriterion("MERGE","Throughput")) std::cout << "Error" << std::endl;

	if(!scheduler.setStatisticalMethod("Quicksort","Least Squares 1D")) std::cout << "Error" << std::endl; else cout << "Success..." << endl;
	if(!scheduler.setStatisticalMethod("Bubblesort","Least Squares 1D")) std::cout << "Error" << std::endl; else cout << "Success..." << endl;

	if(!scheduler.setRecomputationHeuristic("Quicksort","Periodic Recomputation")) std::cout << "Error" << std::endl;	else cout << "Success..." << endl;
	if(!scheduler.setRecomputationHeuristic("Bubblesort","Periodic Recomputation")) std::cout << "Error" << std::endl; else cout << "Success..." << endl;

	Tuple t;
	t.push_back(2.5);

	//string algorithm_name = scheduler.getOptimalAlgorithmName("SORT",t);



	vector<double> feature_values_of_input_data;
	feature_values_of_input_data.push_back(5.5);
	feature_values_of_input_data.push_back(2.3);

	for(int i=0;i<1000;i++){
	uint64_t begin = core::getTimestamp();
	SchedulingDecision sched_dec = scheduler.getOptimalAlgorithmName("SORT",t);
	cout << "Line: " << i << " ";
	if(sched_dec.getNameofChoosenAlgorithm()=="Bubblesort"){
		//sleep(1);
		AlgorithmMeasurement alg_measure(sched_dec);
		cout << "Hallo" << endl; 
		alg_measure.afterAlgorithmExecution(); 
	}else if(sched_dec.getNameofChoosenAlgorithm()=="Quicksort"){
		AlgorithmMeasurement alg_measure(sched_dec);
		cout << "Hallo Hallöchen lieber Leser" << endl; 
		alg_measure.afterAlgorithmExecution(); 
	}else if(sched_dec.getNameofChoosenAlgorithm()=="Mergesort"){
		AlgorithmMeasurement alg_measure(sched_dec);
		cout << "Hi" << endl; 
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

