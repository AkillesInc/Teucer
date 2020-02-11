

#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>

#include <hype.h>


int main(){
    /*#############################################################################*/
    /********************* Create Hardware Specification ***************************/
    /*#############################################################################*/
 C_AlgorithmSpecification* alg_spec_merge_sort = hype_createAlgorithmSpecification("MergeSort","SORT",Least_Squares_1D,Periodic,ResponseTime);
 C_AlgorithmSpecification* alg_spec_quick_sort = hype_createAlgorithmSpecification("QuickSort","SORT",Least_Squares_1D,Periodic,ResponseTime);

 C_DeviceSpecification* cpu_device = hype_createDeviceSpecification(PD0,CPU,PD_Memory_0);
 C_DeviceSpecification* gpu_device = hype_createDeviceSpecification(PD1,GPU,PD_Memory_1);

 if(!hype_addAlgorithm(alg_spec_merge_sort, cpu_device)) printf("Failed to add Algorithm Configuration");
 if(!hype_addAlgorithm(alg_spec_quick_sort, gpu_device)) printf("Failed to add Algorithm Configuration");

 hype_printStatus();
    /*#############################################################################*/
    /********************* Main Loop ***************************/
    /*#############################################################################*/
	unsigned int i;
	for(i=0;i<100;i++){
		printf("Iteration: %i\n",i);
	 double buffer[1];
	 buffer[0]=2.5;

	/*Build Query for HyPE*/
    C_OperatorSpecification* op_spec = hype_create_OperatorSpecification("SORT",buffer,1,PD_Memory_0,PD_Memory_0);
    C_DeviceConstraint* dev_constr = hype_createDeviceConstraint(ANY_DEVICE,PD_Memory_0);
	/*Query HyPE, on which processing device to use*/
    C_SchedulingDecision* sched_dec = hype_getOptimalAlgorithm(op_spec, dev_constr);
	 
		//query scheduling decision
		uint64_t begin = hype_getTimestamp();
		//execute Algorithm
		uint64_t end = hype_getTimestamp();

	  if(!hype_addObservation(sched_dec, (double)end-begin)) printf("Failed to add Observation");
	  if(!hype_deleteSchedulingDecision(sched_dec)) printf("Failed to delete SchedulingDecision");
	  if(!hype_deleteOperatorSpecification(op_spec)) printf("Failed to delete OperatorSpecification");
	  if(!hype_deleteDeviceConstraint(dev_constr)) printf("Failed to delete device constraint");
	}


    /*#############################################################################*/
    /***************************** Free Ressources *********************************/
    /*#############################################################################*/
 if(!hype_deleteDeviceSpecification(cpu_device)) printf("Failed to free DeviceSpecification");
 if(!hype_deleteDeviceSpecification(gpu_device)) printf("Failed to free DeviceSpecification");

 if(!hype_deleteAlgorithmSpecification(alg_spec_merge_sort)) printf("Failed to free AlgortihmSpecification");
 if(!hype_deleteAlgorithmSpecification(alg_spec_quick_sort)) printf("Failed to free AlgortihmSpecification");

	return 0;
}

