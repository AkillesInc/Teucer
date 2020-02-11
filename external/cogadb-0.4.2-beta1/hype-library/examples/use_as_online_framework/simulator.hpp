/* 
 * File:   simulator.hpp
 * Author: sebastian
 *
 * Created on 6. Oktober 2013, 23:40
 */

#ifndef SIMULATOR_HPP
#define	SIMULATOR_HPP

const unsigned int BYTES_PER_ROW=100; //4;

#include <query_processing/generic_operator_benchmark.hpp>
#include <query_processing/logical_query_plan.hpp>

namespace hype
{
	namespace simulator{

        struct Artificial_Dataset{
            Artificial_Dataset() : size_in_number_of_bytes_(0){}
            Artificial_Dataset(unsigned long long size_in_number_of_bytes) : size_in_number_of_bytes_(size_in_number_of_bytes){}
            unsigned int getSizeinBytes() const{ return size_in_number_of_bytes_;}
            unsigned int getNumberofRows() const{ return size_in_number_of_bytes_/BYTES_PER_ROW;} //assume 32 Bit integer elements}
        private:
            unsigned long long size_in_number_of_bytes_;
        };   
        //typedefinitions to map simulator data types to HyPE
        typedef hype::queryprocessing::OperatorMapper_Helper_Template<Artificial_Dataset>::Map_Init_Function Map_Init_Function;
        typedef hype::queryprocessing::OperatorMapper_Helper_Template<Artificial_Dataset>::Physical_Operator_Map Physical_Operator_Map;
        typedef hype::queryprocessing::OperatorMapper_Helper_Template<Artificial_Dataset>::Physical_Operator_Map_Ptr Physical_Operator_Map_Ptr;
        typedef hype::queryprocessing::OperatorMapper_Helper_Template<Artificial_Dataset>::TypedOperatorPtr TypedOperatorPtr;
        typedef hype::queryprocessing::OperatorMapper_Helper_Template<Artificial_Dataset>::TypedLogicalNode TypedLogicalNode;
        typedef hype::queryprocessing::OperatorMapper_Helper_Template<Artificial_Dataset>::TypedNodePtr TypedNodePtr;
        typedef hype::queryprocessing::NodePtr NodePtr;                
        typedef hype::queryprocessing::OperatorMapper_Helper_Template<Artificial_Dataset>::PhysicalQueryPlanPtr PhysicalQueryPlanPtr;
        typedef hype::queryprocessing::LogicalQueryPlan<Artificial_Dataset> LogicalQueryPlan;
        typedef boost::shared_ptr<LogicalQueryPlan> LogicalQueryPlanPtr;
        //typedefinitions for simulator 
        typedef queryprocessing::Generic_Operation_Benchmark<Artificial_Dataset>::DeviceSpecifications DeviceSpecifications;
        typedef queryprocessing::Generic_Operation_Benchmark<Artificial_Dataset>::AlgorithmSpecifications AlgorithmSpecifications;

class Simulator : public queryprocessing::Generic_Operation_Benchmark<Artificial_Dataset>{
public:
                Simulator(const std::string& operation_name, 
                          const AlgorithmSpecifications& alg_specs_, 
                          const DeviceSpecifications& dev_specs_, 
                          const std::vector<double>& relative_processing_device_speeds) 
                        : queryprocessing::Generic_Operation_Benchmark<Artificial_Dataset>(operation_name,alg_specs_,dev_specs_), 
                          relative_processing_device_speeds_(relative_processing_device_speeds)
                {
                            assert(relative_processing_device_speeds_.size()==dev_specs_.size());
                            assert(!relative_processing_device_speeds_.empty());
                            //first processign device is always 1, and the other speed values are 
                            //the speed relative to the first processing device
                            assert(relative_processing_device_speeds_.front()==1);
                }
    
    		virtual TypedNodePtr generate_logical_operator(Artificial_Dataset dataset);

		virtual Artificial_Dataset generate_dataset(unsigned int size_in_number_of_bytes);
private:
    std::vector<double> relative_processing_device_speeds_;
};
    
            
        namespace physical_operator{

            class Simulation_Operator : public hype::queryprocessing::UnaryOperator<Artificial_Dataset, Artificial_Dataset> {
				public:
					typedef hype::queryprocessing::OperatorMapper_Helper_Template<Artificial_Dataset>::TypedOperatorPtr TypedOperatorPtr;
					Simulation_Operator(const hype::SchedulingDecision& sched_dec, Artificial_Dataset data);
					virtual bool execute();
					virtual ~Simulation_Operator();
				private:
					Artificial_Dataset data_;
			};

			Physical_Operator_Map_Ptr map_init_function_Simulation_Operator();
			TypedOperatorPtr create_Simulation_Operator(TypedLogicalNode& logical_node, const hype::SchedulingDecision&, TypedOperatorPtr left_child, TypedOperatorPtr right_child);
			
		}//end namespace physical_operator


        namespace logical_operator {

			class Logical_Simulation_Operator : public hype::queryprocessing::TypedNode_Impl<Artificial_Dataset, physical_operator::map_init_function_Simulation_Operator> //init_function_Simulation_Operator> //init_function_Simulation_Operator>
			{
				public:
					Logical_Simulation_Operator(Artificial_Dataset data);
					
					virtual unsigned int getOutputResultSize() const;
					
                                        virtual double getCalculatedSelectivity() const;
					
					virtual std::string getOperationName() const;
					
                                        virtual std::string toString(bool verbose) const;
                                        
					//const std::string& getTableName();
                                        
                                        const Artificial_Dataset getArtificial_Dataset();
					
					virtual TypedOperatorPtr getOptimalOperator(TypedOperatorPtr left_child, TypedOperatorPtr right_child, hype::DeviceTypeConstraint dev_constr);
					
				private:						
					Artificial_Dataset data_;
			};

		}//end namespace logical_operator

	}; //end namespace simulator
}; //end namespace hype

#endif	/* SIMULATOR_HPP */

