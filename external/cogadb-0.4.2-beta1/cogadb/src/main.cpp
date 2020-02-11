



#include <fstream>

#include <unittests/unittests.hpp>
#include <util/tpch_benchmark.hpp>
#include <util/star_schema_benchmark.hpp>
#include <core/runtime_configuration.hpp>
#include <util/reduce_by_keys.hpp>

#include <core/gpu_column_cache.hpp>

#include <query_processing/query_processor.hpp>
#include <parser/commandline_interpreter.hpp>

#include <boost/lexical_cast.hpp>
#include <boost/random.hpp>
#include <boost/generator_iterator.hpp>

#include <utility>
#include <parser/generated/Parser.h>

#include <util/hardware_detector.hpp>
#include <core/processor_data_cache.hpp>

using namespace std;
using namespace CoGaDB;
using namespace query_processing;




int main(int argc, char* argv[]){

        cout << "CoGaDB version " << COGADB_VERSION << endl;

        #ifdef ENABLE_SIMD_ACCELERATION
                cout << "SIMD ACCELERATION ENABLED" << endl;
        #else
                cout << "SIMD ACCELERATION DISABLED" << endl;        
        #endif

        #ifdef FORCE_USE_UNSTABLE_SORT_FOR_BENCHMARK
         cout << "Warning: Compiled with FORCE_USE_UNSTABLE_SORT_FOR_BENCHMARK defined, order by for multiple columns will not work!" << endl;        
        #endif       

#ifdef ENABLE_GPU_ACCELERATION
        if(HardwareDetector::instance().getNumberOfGPUs()==0){
            RuntimeConfiguration::instance().setGlobalDeviceConstraint(hype::CPU_ONLY);
            std::cout << "Found no CUDA Capable Devices! Switching to CPU_ONLY mode..." << std::endl;        
        }
#else
        std::cout << "Compiled without GPU acceleration! Switching to CPU_ONLY mode..." << std::endl;        
#endif

        cout << "Enter 'help' for instructions" << endl;
	std::string prompt("CoGaDB>");
	std::string input;
        CommandLineInterpreter cmd;
        //read commands from startup.coga
        {
            ifstream in_stream;
            in_stream.open("startup.coga");
            if(in_stream.good()){
                while(!in_stream.eof()){
                    std::getline(in_stream, input);
                    if(input=="quit") 
                        CoGaDB::exit(EXIT_SUCCESS);
                    if(!cmd.execute(input,cmd.getClient())){ 
                        cout << "Error! Command '" << input << "' failed!" << endl;
                       CoGaDB::exit(EXIT_FAILURE);
                    }
                }
            }
        }
        //if script file was specified, execute script and exit
        if(argc>1){
            ifstream in_stream;
            cout << "Executing coga script file: " << argv[1] << endl;
            in_stream.open(argv[1]);
            ofstream log_file_for_timings;
            //open log file, delete content and start over with empty file
            log_file_for_timings.open("cogascript_timings.log", ios_base::out | ios_base::trunc);
            
            while(!in_stream.eof()){
                std::getline(in_stream, input);
                //cout << "Execute Command '" << input << "'" << endl;
                if (input.substr(0, 12) == "parallelExec"){ // || input.substr(0, 18) == "parallel_execution") {
                    cmd.parallelExecution(input, &in_stream, &log_file_for_timings);
                    CoGaDB::exit(EXIT_SUCCESS);
                }
                if(input=="quit"){
                    CoGaDB::exit(EXIT_SUCCESS);
                }
                CoGaDB::Timestamp begin = CoGaDB::getTimestamp();
                bool ret = cmd.execute(input,cmd.getClient());
                CoGaDB::Timestamp end = CoGaDB::getTimestamp();
                if(!ret){ 
                    cout << "Error! Command '" << input << "' failed!" << endl;
                    CoGaDB::exit(EXIT_FAILURE);
                }else{
                    cout << input << "\t(" << double(end-begin)/(1000*1000) << "ms)" << endl;
                }
            }
            in_stream.close();
            CoGaDB::exit(EXIT_SUCCESS);
        }    
        //show interactive shell
	while(true){
		if (cmd.getline(prompt, input)) {
                    cout << endl;
                    CoGaDB::exit(EXIT_SUCCESS);
		}
		if(input=="quit"){
                    CoGaDB::exit(EXIT_SUCCESS);
                }
		if(!cmd.execute(input, cmd.getClient())) cout << "Error! Command '" << input << "' failed!" << endl;
	}
	return 0;
}
