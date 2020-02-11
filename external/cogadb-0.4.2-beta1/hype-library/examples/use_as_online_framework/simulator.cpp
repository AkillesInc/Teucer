/* 
 * File:   simulator.cpp
 * Author: sebastian
 *
 * Created on 6. Oktober 2013, 21:56
 */

#include <cstdlib>
#include <boost/lexical_cast.hpp>
#include <boost/version.hpp>
//#include <boost/random/mersenne_twister.hpp> 
#include <boost/random/inversive_congruential.hpp>
#include <boost/program_options/options_description.hpp>
#include "simulator.hpp"

using namespace std;
using namespace hype;
using namespace queryprocessing;
using namespace simulator;

namespace hype
{
    namespace simulator
    {
        
        const double MICROSECONDS_PER_ROW=0.1;
        double DEFAULT_DATA_CACHE_HITRATE=0.5; //0.9; //1.0;
        
         std::vector<double> relative_processing_device_speeds; //speedup w.r.t. processing device zero
         std::vector<double> average_data_cache_hitrate; //probability that input data has not to be copied 
                                                         //from CPU to (Co-)Processor i (in Case of NUMA, result 
                                                         //might be transfered to different memory bank)
         std::vector<double> waiting_time_on_data_transfer;  //total time processing device could not transfer data because bus was busy
         double average_operator_selectivity_value=1.0; //steers the result sizes, which have to be 
                                                              //transferred back from Co-Processor to CPU
         double relative_bus_speed=1.0; //average speed w.r.t. processing device zero (CPU), steers the 
                                              //overhead of the data transfers
         
         
         //mutexes for simulating data transfers, one lock for each direction CPU->CP and CP->CPU
         boost::mutex cpu_to_cp_transfer_mutex;
         boost::mutex cp_to_cpu_transfer_mutex;
         
//         boost::mt19937 rng_tmp;
//         //this reference will later point to the random number generator from 
//         //the generic operator benchmark, which is steered by the CLI seed value
//         boost::mt19937& rng(rng_tmp);
         
         boost::hellekalek1995 rng_tmp;
         boost::hellekalek1995& rng(rng_tmp);
         
//         double normal_distribution_box_mueller_transform(){
//         
//             //Generate two uniform (0, 1) random samples u and v using any random generator you trust. Then let r = sqrt( -2 log(u) ) and return x = r sin(2 pi v). (This is called the Box-Mueller method.)
//             int u=rng();
//             int v=rng();
//             //If you need normal samples samples with mean mu and standard deviation sigma, return sigma*x + mu instead of just x.
//         
//         }

         
		namespace physical_operator
		{
			TypedOperatorPtr create_Simulation_Operator(TypedLogicalNode& logical_node, const hype::SchedulingDecision& sched_dec, TypedOperatorPtr, TypedOperatorPtr)
			{
				logical_operator::Logical_Simulation_Operator& log_sort_ref = static_cast<logical_operator::Logical_Simulation_Operator&>(logical_node);
				//std::cout << "create SCAN Operator!" << std::endl;
				/*
				if(!left_child) {
					std::cout << "Error!" << std::endl;
					exit(-1);
				}
				assert(right_child==NULL); //unary operator
				*/
				return TypedOperatorPtr( new Simulation_Operator(sched_dec, log_sort_ref.getArtificial_Dataset()) );
			}



			Physical_Operator_Map_Ptr map_init_function_Simulation_Operator()
			{
				//std::cout << sd.getNameofChoosenAlgorithm() << std::endl;
				Physical_Operator_Map map;
				if(!hype::core::quiet) std::cout << "calling map init function! (SCAN OPERATION)" << std::endl;
				//hype::Scheduler::instance().addAlgorithm("SCAN","TABLE_SCAN",hype::CPU,"Least Squares 1D","Periodic Recomputation");
//				hype::AlgorithmSpecification selection_alg_spec_cpu("TABLE_SCAN",
//																	"SCAN",
//																	hype::Least_Squares_1D,
//																	hype::Periodic);


		//addAlgorithmSpecificationToHardware();

//		const DeviceSpecifications& dev_specs = HardwareDetector::instance().getDeviceSpecifications();
//
//		for(unsigned int i=0;i<dev_specs.size();++i){
//			if(dev_specs[i].getDeviceType()==hype::CPU){
//				hype::Scheduler::instance().addAlgorithm(selection_alg_spec_cpu,dev_specs[i]);
//			}
//		}

				map["Simulated_Algorithm"]=create_Simulation_Operator;
                                
				//map["GPU_Algorithm"]=create_GPU_SORT_Operator;
				return Physical_Operator_Map_Ptr(new Physical_Operator_Map(map));
			}

			Simulation_Operator::Simulation_Operator(const hype::SchedulingDecision& sched_dec, Artificial_Dataset data)
                        : UnaryOperator<Artificial_Dataset,Artificial_Dataset>(sched_dec, TypedOperatorPtr()),data_(data)
			{
					//this->result_=data_;
			}
			bool Simulation_Operator::execute()
			{	if(!hype::core::quiet && hype::core::verbose && hype::core::debug) std::cout << "Execute Simulation Operator"  << std::endl;
				//const Artificial_Dataset sort(Artificial_Dataset table, const std::string& column_name, SortOrder order=ASCENDING, MaterializationStatus mat_stat=MATERIALIZE, ComputeDevice comp_dev=CPU);
				
                                //cout << "Process Artificial Data set of Size: " << data_.getSizeinBytes() << " Bytes (" << data_.getNumberofRows() << " rows) with relative speed: " << relative_processing_device_speeds[sched_dec_.getDeviceSpecification().getProcessingDeviceID()] << endl;
                                //this->result_=data_;
                                
                                //data transfer from CPU to Co-Processor
                                {       
                                        assert(average_data_cache_hitrate[sched_dec_.getDeviceSpecification().getProcessingDeviceID()]>=0.0);
                                        assert(average_data_cache_hitrate[sched_dec_.getDeviceSpecification().getProcessingDeviceID()]<=1.0);
                                        std::vector<double> probabilities(2);
                                        //probability that data has not to be transfered from CPU to Co-Processor because of cache hit
                                        probabilities[0]=average_data_cache_hitrate[sched_dec_.getDeviceSpecification().getProcessingDeviceID()];
                                        //probability that data has to be transfered from CPU to Co-Processor because of cache miss
                                        probabilities[1]=1-average_data_cache_hitrate[sched_dec_.getDeviceSpecification().getProcessingDeviceID()];
                                        
                                        boost::random::discrete_distribution<> dist(probabilities.begin(),probabilities.end());

                                        int cache_miss = dist(rng);
                                        
                                        if(cache_miss==1){
                                            //std::cout << "Cache Miss! Device: " << (int) sched_dec_.getDeviceSpecification().getProcessingDeviceID() << std::endl;
                                            //block in case bus is busy
                                            uint64_t wait_for_bus_begin=hype::core::getTimestamp();
                                            boost::mutex::scoped_lock lock(cpu_to_cp_transfer_mutex);
                                            uint64_t wait_for_bus_end=hype::core::getTimestamp();
                                            boost::this_thread::sleep(boost::posix_time::microseconds( (data_.getNumberofRows()*MICROSECONDS_PER_ROW)/relative_bus_speed ));
                                            assert(wait_for_bus_end>=wait_for_bus_begin);
                                            waiting_time_on_data_transfer[sched_dec_.getDeviceSpecification().getProcessingDeviceID()]+=double(wait_for_bus_end-wait_for_bus_begin);//((data_.getNumberofRows()*MICROSECONDS_PER_ROW)*relative_bus_speed)*1000; //add time in nanoseconds to statistics
                                        }else{
                                            //std::cout << "Cache Hit! Device: " << (int) sched_dec_.getDeviceSpecification().getProcessingDeviceID() << std::endl;
                                        }
                                }
                                
                                //simulate algorithm execution time
                                boost::normal_distribution<> norm_dist(0,100); //jitter as for execution time of roughly 1000 rows
				double jitter_value =  std::abs(norm_dist(rng)); //rand();
                                //std::cout << "Base time: " << data_.getNumberofRows()/relative_processing_device_speeds[sched_dec_.getDeviceSpecification().getProcessingDeviceID()]  << "micro secs\tJitter: " << jitter_value << "micro secs" << std::endl;
                                //boost::this_thread::sleep(boost::posix_time::microseconds(data_.getNumberofRows()));
                                assert(0<relative_processing_device_speeds[sched_dec_.getDeviceSpecification().getProcessingDeviceID()]);
                                boost::this_thread::sleep(boost::posix_time::microseconds( ( (data_.getNumberofRows()*MICROSECONDS_PER_ROW)/relative_processing_device_speeds[sched_dec_.getDeviceSpecification().getProcessingDeviceID()])+jitter_value));
                                
                                //data transfer from Co-Processor to CPU (result has always to be copied back in case it is not the CPU!)
                                {       if(sched_dec_.getDeviceSpecification().getProcessingDeviceID()!=PD0){
                                            //block in case bus is busy
                                            uint64_t wait_for_bus_begin=hype::core::getTimestamp();
                                            boost::mutex::scoped_lock lock(cp_to_cpu_transfer_mutex);
                                            uint64_t wait_for_bus_end=hype::core::getTimestamp();
                                            boost::this_thread::sleep(boost::posix_time::microseconds( ((data_.getNumberofRows()*MICROSECONDS_PER_ROW)*average_operator_selectivity_value)/relative_bus_speed ));
                                            assert(wait_for_bus_end>=wait_for_bus_begin);
                                            waiting_time_on_data_transfer[sched_dec_.getDeviceSpecification().getProcessingDeviceID()]+=double(wait_for_bus_end-wait_for_bus_begin);
                                        }
                                }
                                
                                return true;
			}

			Simulation_Operator::~Simulation_Operator() {}

		}//end namespace physical_operator


		namespace logical_operator
		{
//			Logical_Simulation_Operator::Logical_Simulation_Operator() : TypedNode_Impl<Artificial_Dataset,physical_operator::map_init_function_Simulation_Operator>(), data_()
//			{
//
//			}
                        Logical_Simulation_Operator::Logical_Simulation_Operator(Artificial_Dataset table) : TypedNode_Impl<Artificial_Dataset,physical_operator::map_init_function_Simulation_Operator>(), data_(table)
			{

			}
			unsigned int Logical_Simulation_Operator::getOutputResultSize() const
			{
				//Artificial_Dataset data_ptr = getTablebyName(data_name_);
				//assert(data_ptr != NULL);

				return data_.getNumberofRows();
			}

            double Logical_Simulation_Operator::getCalculatedSelectivity() const {
				return 1.0;
			}
			std::string Logical_Simulation_Operator::getOperationName() const
			{
				return "SIMULATED_OPERATION";
			}
                        
                        std::string Logical_Simulation_Operator::toString(bool verbose) const{
				return std::string("SIMULATED_OPERATION "); //+data_->getName();
			}
                        
//			const std::string& Logical_Simulation_Operator::getTableName()
//			{
//				return data_->getName();
//			}
                        
                        const Artificial_Dataset Logical_Simulation_Operator::getArtificial_Dataset()
			{
				return data_;
			}

	//redefining virtual function -> workaround so that scan operator can be processed uniformely with other operators
	TypedOperatorPtr Logical_Simulation_Operator::getOptimalOperator(TypedOperatorPtr left_child, TypedOperatorPtr right_child, hype::DeviceTypeConstraint dev_constr){
		hype::Tuple t;

		t.push_back(getOutputResultSize());

		return this->operator_mapper_.getPhysicalOperator(*this, t, left_child, right_child, dev_constr); //this->getOperationName(), t, left_child, right_child);
	}

        }//end namespace logical_operator
                
        TypedNodePtr Simulator::generate_logical_operator(Artificial_Dataset dataset){

                //cout << "Create Sort Operation for Table " << dataset->getName() << endl;

			//boost::shared_ptr<logical_operator::Logical_Scan>  scan(new logical_operator::Logical_Scan(dataset->getName()));
//                        std::list<std::string> sorting_columns;
//                        sorting_columns.push_back("Sort_Keys");         
//			boost::shared_ptr<logical_operator::Logical_Sort>  sort(new logical_operator::Logical_Sort(sorting_columns));	
//
//			sort->setLeft(scan);


                return TypedNodePtr(new logical_operator::Logical_Simulation_Operator(dataset));
        }
        
        Artificial_Dataset Simulator::generate_dataset(unsigned int size_in_number_of_bytes){
            static unsigned int dataset_counter=0;

            std::cout << "Create Dataset of Size " <<  size_in_number_of_bytes << " Byte" << std::endl;

            return Artificial_Dataset(size_in_number_of_bytes);
//			unsigned int size_in_number_of_elements = size_in_number_of_bytes/sizeof(int);
//
//			boost::mt19937& rng = this->getRandomNumberGenerator();
//			boost::uniform_int<> six(0,1000*1000*1000);
//
//			for(unsigned int i=0;i<size_in_number_of_elements;++i){
//				int e =  six(rng); //rand();
//			}
	}
                
    }; //end namespace simulator
}; //end namespace hype

std::vector<std::string> &split(const std::string &s, char delim, std::vector<std::string> &elems) {
    std::stringstream ss(s);
    std::string item;
    while (std::getline(ss, item, delim)) {
        elems.push_back(item);
    }
    return elems;
}


std::vector<std::string> split(const std::string &s, char delim) {
    std::vector<std::string> elems;
    split(s, delim, elems);
    return elems;
}


/*
 * 
 */
int main(int argc, char** argv) {
    if(BOOST_VERSION==104800){
        cout << "Warning: Detected Boost Random Library that has a bug in its normal distribution implementation: " << BOOST_LIB_VERSION << " (BOOST_VERSION: " << BOOST_VERSION << ")" << endl;
        cout << "For more details, see: https://bugs.launchpad.net/hugin/+bug/1048799" << endl;
    }
    //assert(BOOST_VERSION!=104800);
    
    // Declare the supported options.
    //boost::program_options::options_description desc; //("Allowed options");
    boost::program_options::options_description& desc = getGlobalBenchmarkOptionDescription();
    desc.add_options()
        //("help", "produce help message")
        ("speed_vector", boost::program_options::value<std::string>(), 
         "specifies the relative speed values of n processing devices," 
         "device zero always has speed 1, where the others have values "
         "specifying the speed w.r.t. processing deivce zero"
         "example: \"1 0.5 2\" defines 3 processing devices, "
         "where processing device 1 is twice as slow as processing device 0"
         "and processing device 2 is twice as fast as processing device 0")
        ("cache_hitrate",boost::program_options::value<double>(),"probability that input data is cached in a co-processor")
        ("average_operator_selectivity",boost::program_options::value<double>(),
         "average selectivity of the simulated operator has a high impact on the"
         "data transfer overhead of copying results from a co-processor back to the main meory")
         ("relative_bus_speed",boost::program_options::value<double>(),
          "speed of bus relative to processing device 0 (e.g., if PD0 needs "
          "X time units to process a data set of Y bytes, then the bus needs" 
          "relative_bus_speed*X time units to transfer Y bytes ober the bus)")
         ("comment",boost::program_options::value<std::string>(),"pass a user comment that will appear in the simulators logfile")
    ;
//    //backup the new program options (have to be propagated to benchmark, so it 
//    //does not throw an error when parsing simulator options)
//    boost::program_options::options_description desc_new;
//    desc_new.add(desc);
//    //add default benchmark command line options
//    desc.add(getGlobalBenchmarkOptionDescription());
    
    string simulated_operator_name="SIMULATED_OPERATION";
    
    AlgorithmSpecifications alg_specs;
    DeviceSpecifications dev_specs;

    alg_specs.push_back(AlgorithmSpecification("Simulated_Algorithm",
                                                simulated_operator_name,
                                                hype::Least_Squares_1D,
                                                hype::Periodic,
                                                hype::ResponseTime)
                       ); 
    
    boost::program_options::variables_map vm;
    //check whether first argument is --speed_vector
    boost::program_options::store(boost::program_options::parse_command_line(argc, argv, desc), vm);
    boost::program_options::notify(vm);    
    vector<string> tokens;
    std::string user_comment;
    std::string speed_vector_string;
    //cout << desc << endl; 
    if (vm.count("speed_vector")) {
        std::cout << "speed_vector: " 
        << vm["speed_vector"].as<string>() << "\n";
       speed_vector_string=vm["speed_vector"].as<std::string>();
       //speed_vector_string.
       tokens = split(speed_vector_string,',');
    
   } else {
        std::cout << "speed_vector was not specified, using default processing device configuration...\n";
        speed_vector_string="1,0.5,1.2";
        tokens = split("1,0.5,1.2",',');
    }
    if(vm.count("cache_hitrate")) { 
        DEFAULT_DATA_CACHE_HITRATE=vm["cache_hitrate"].as<double>();
        assert(DEFAULT_DATA_CACHE_HITRATE>=0 && DEFAULT_DATA_CACHE_HITRATE<=1);
    }
    std::cout << "Average Cache Hitrate of Co-processors: " << DEFAULT_DATA_CACHE_HITRATE*100 << "%" << std::endl;
    if(vm.count("average_operator_selectivity")) { 
        average_operator_selectivity_value=vm["average_operator_selectivity"].as<double>();
        assert(average_operator_selectivity_value>=0 && average_operator_selectivity_value<=1);
    }
    std::cout << "Average Operator Selectivity: " << average_operator_selectivity_value << std::endl;
    if(vm.count("relative_bus_speed")) { 
        relative_bus_speed=vm["relative_bus_speed"].as<double>();
        assert(relative_bus_speed>0);
    }
    std::cout << "Bus speed relative to processing Device PD0: " << relative_bus_speed << std::endl;

    if(vm.count("comment")) { 
        user_comment=vm["comment"].as<std::string>();
    }
    std::cout << "User Comment: " << user_comment << std::endl;
    
    
    //assure we don't get larger speed vectors than supported proccesing devices
    assert(tokens.size()<=PD20);  
    //create processing devicves
    for(unsigned int i=0;i<tokens.size();++i){
        //cout << "Processing Token " << i << ": " << tokens[i] << endl;
        relative_processing_device_speeds.push_back(boost::lexical_cast<double>(tokens[i]));
        if(i==0){
            dev_specs.push_back(DeviceSpecification((ProcessingDeviceID)(i),
                                                    hype::CPU, 
                                                    hype::PD_Memory_0)  
                               ); 
            //CPU never needs data transfers, because the data is already in main memory 
            average_data_cache_hitrate.push_back(1.0);
        }else{
            dev_specs.push_back(DeviceSpecification((ProcessingDeviceID)(i),
                                    hype::GPU, 
                                    (hype::ProcessingDeviceMemoryID) i)  
                               ); 
            //assign Co-Processor default Cache Hitrate
            average_data_cache_hitrate.push_back(DEFAULT_DATA_CACHE_HITRATE);
        }
        std::cout << "added processing device " << i << " with relative speed " << relative_processing_device_speeds[i] << std::endl;  
    }
    //init waiting times for bus for each processor (0)
    waiting_time_on_data_transfer.resize(tokens.size());
    
    
    Simulator sim(simulated_operator_name,alg_specs,dev_specs,relative_processing_device_speeds);
    //add new simulator options to benchmark options
//    boost::program_options::options_description& desc_benchmark = sim.getOptionsDescription();
//    desc_benchmark.add(desc_new);

    sim.setup(argc, argv);
    sim.run();

    cout << "Write Log File for Experiments" << endl;
    std::fstream file("simulator.log",std::ios_base::out | std::ios_base::app);
    file.seekg(0, std::ios::end); // put the "cursor" at the end of the file
    unsigned int file_length = file.tellg(); // find the position of the cursor

    if(file_length==0){ //if file empty, write header
        file << "#header of simulator logfile" << endl;
    }
    
    
    file << "speed_vector=" <<  speed_vector_string << "\t"
         << "cache_hitrate=" << DEFAULT_DATA_CACHE_HITRATE << "\t"
         << "average_operator_selectivity=" << average_operator_selectivity_value << "\t"
         << "relative_bus_speed=" << relative_bus_speed << "\t"
         //<< "waiting_time_through_waits_for_bus=" << 
         << "comment=" << user_comment
         << endl;   
            
            
    for(unsigned int i=0;i<tokens.size();++i){
        cout << "Waiting Time due to waits for the bus for processing device " << i <<": " << waiting_time_on_data_transfer[i] << "ns" << endl;
    }
    
    return 0;
}

