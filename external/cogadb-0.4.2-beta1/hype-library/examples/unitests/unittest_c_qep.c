

#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>

#include <hype.h>
#include <query_optimization/qep.h>

void recursivePrint_(C_QEP_Node* root, int level){
    int i=0;
//    printf("My Plan:\n");
    //root
    char* node_string = hype_QEPNodeToString(root);
    char indent[level+1];
    //memset(indent,0,level+1);
    for(i=0;i<level+1;++i){
        indent[i]='\t';
    }
    indent[level]='\0';
    
    printf("%s%s\n",indent,node_string);
    free(node_string);
    
    size_t num_children=0;
    C_QEP_Node** children = hype_getChildren(root, &num_children);
    
    for(i=0;i<num_children;++i){
//        node_string = hype_QEPNodeToString(children[i]);
//        printf("\t%s\n",node_string);
//        free(node_string);
        recursivePrint_(children[i],level+1);
    }
    
    for(i=0;i<num_children;++i){
        hype_freeQEPNode(children[i]);
    }
    free(children);
    
}

int indended_print_QEP_Node(C_QEP_Node* root, int level){
    int i=0;
    char* node_string = hype_QEPNodeToString(root);
    char indent[level+1];
    //memset(indent,0,level+1);
    for(i=0;i<level+1;++i){
        indent[i]='\t';
    }
    indent[level]='\0';
    
    printf("%s%s\n",indent,node_string);
    free(node_string);
    return 0;
}
    
void recursivePrint(C_QEP_Node* root){
    printf("My Plan:\n");
    //recursivePrint_(root, 0);
    if(hype_traverseQEP(root, indended_print_QEP_Node)){
        printf("Error printing Plan!");
    }
}


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
	//for(i=0;i<100;i++)
        {
	 //printf("Iteration: %i\n",i);
	 double buffer[1];
	 buffer[0]=2.5;

	/*Build Query for HyPE*/
    C_OperatorSpecification* op_spec = hype_create_OperatorSpecification("SORT",buffer,1,PD_Memory_0,PD_Memory_0);
    C_DeviceConstraint* dev_constr = hype_createDeviceConstraint(ANY_DEVICE,PD_Memory_0);
    
    C_QEP_Node* root = hype_createQEPNode(op_spec, dev_constr);

    if(!hype_deleteOperatorSpecification(op_spec)) printf("Failed to delete OperatorSpecification");
    if(!hype_deleteDeviceConstraint(dev_constr)) printf("Failed to delete device constraint");
    
    C_QEP_Node* childs[3];
    for(i=0;i<3;++i){
        op_spec = hype_create_OperatorSpecification("SORT",buffer,1,PD_Memory_0,PD_Memory_0);
        dev_constr = hype_createDeviceConstraint(ANY_DEVICE,PD_Memory_0);
        
        childs[i] = hype_createQEPNode(op_spec, dev_constr);
        if(!hype_deleteOperatorSpecification(op_spec)) printf("Failed to delete OperatorSpecification");
        if(!hype_deleteDeviceConstraint(dev_constr)) printf("Failed to delete device constraint");
        
    }
    
    hype_setChildren(root,childs,3);
    hype_printQEPNode(root);
    
    for(i=0;i<3;++i){
        free(childs[i]);
    }
    
    
    
    printf("My Plan:\n");
    //root
    char* node_string = hype_QEPNodeToString(root);
    printf("%s\n",node_string);
    free(node_string);
    
    size_t num_children=0;
    C_QEP_Node** children = hype_getChildren(root, &num_children);
    
    for(i=0;i<num_children;++i){
        node_string = hype_QEPNodeToString(children[i]);
        printf("\t%s\n",node_string);
        free(node_string);
    }
    
    for(i=0;i<num_children;++i){
        hype_freeQEPNode(children[i]);
    }
    free(children);
    
    
    recursivePrint(root);
    
    hype_recursivefreeQEPNode(root);
    
    
    
    
//	/*Query HyPE, on which processing device to use*/
//    C_SchedulingDecision* sched_dec = hype_getOptimalAlgorithm(op_spec, dev_constr);
//	 
//		//query scheduling decision
//		uint64_t begin = hype_getTimestamp();
//		//execute Algorithm
//		uint64_t end = hype_getTimestamp();
//
//	  if(!hype_addObservation(sched_dec, (double)end-begin)) printf("Failed to add Observation");
//	  if(!hype_deleteSchedulingDecision(sched_dec)) printf("Failed to delete SchedulingDecision");

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

