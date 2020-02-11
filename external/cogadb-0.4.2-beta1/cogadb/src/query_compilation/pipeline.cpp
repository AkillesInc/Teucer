
#include <query_compilation/pipeline.hpp>
#include <boost/filesystem.hpp>
#include <dlfcn.h>

#include <util/time_measurement.hpp>

#include <boost/uuid/uuid.hpp>            // uuid class
#include <boost/uuid/uuid_generators.hpp> // generators
#include <boost/uuid/uuid_io.hpp>         // streaming operators etc.

#include <sstream>

namespace CoGaDB {
    
    Pipeline::Pipeline(CompiledQueryPtr query, const ScanParam& scan_param, double compile_time_in_sec)
    : query_(query), scan_param_(scan_param), compile_time_in_sec_(compile_time_in_sec),
    begin_(0), end_(0)
    {

    }

    bool Pipeline::execute() {
        if (query_) {
            begin_ = getTimestamp();
            result_ = (*query_)(scan_param_);
            end_ = getTimestamp();
            if (result_) {
                return true;
            } else {
                return false;
            }
        } else {
            return false;
        }
    }

    TablePtr Pipeline::getResult() const {
        return result_;
    }

    double Pipeline::getCompileTimeSec() const {
        return compile_time_in_sec_;
    }

    double Pipeline::getExecutionTimeSec() const{
        if(!result_) return double(0);
        assert(end_>=begin_);
        double exec_time = double(end_-begin_)/(1000*1000*1000);
        return exec_time;
    }
    
    Pipeline::~Pipeline() {

    }
    
    SharedLibPipeline::SharedLibPipeline(CompiledQueryPtr query,
            const ScanParam& scan_param,
            double compile_time_in_sec,
            void* shared_lib,
            const std::string& compiled_query_base_file_name)
    : Pipeline(query, scan_param, compile_time_in_sec),
    shared_lib_(shared_lib), compiled_query_base_file_name_(compiled_query_base_file_name) {
        assert(shared_lib_ != NULL);
    }

    SharedLibPipeline::~SharedLibPipeline() {
        dlclose(shared_lib_);
        if(boost::filesystem::exists(compiled_query_base_file_name_+".cpp")){
            boost::filesystem::remove(compiled_query_base_file_name_+".cpp");            
        }
        if(boost::filesystem::exists(compiled_query_base_file_name_+".o")){
            boost::filesystem::remove(compiled_query_base_file_name_+".o");            
        }            
        if(boost::filesystem::exists(compiled_query_base_file_name_+".so")){
            boost::filesystem::remove(compiled_query_base_file_name_+".so");            
        }
        if(boost::filesystem::exists(compiled_query_base_file_name_+".cpp.orig")){
            boost::filesystem::remove(compiled_query_base_file_name_+".cpp.orig");            
        }            
    }    

        const TablePtr do_nothing(const ScanParam&){
            return TablePtr();
        }

        DummyPipeline::DummyPipeline(TablePtr result, const ScanParam& scan_param)
        : Pipeline(&do_nothing, scan_param, 0)
        {
            this->result_=result;
        }

        bool DummyPipeline::execute(){
            return true;
        }    
    
    const PipelinePtr compileQueryFile(const std::string& path, const ScanParam& param){
        
        char* error = NULL;
        int ret=0;

        if(!boost::filesystem::exists(path)){
            std::cerr << "Could not find file: '" << path << "'" << std::endl;
            return PipelinePtr();
        }
        
        ScanParam scan_param;
        scan_param.insert(scan_param.begin(),param.begin(),param.end());
        
        boost::uuids::uuid uuid = boost::uuids::random_generator()();
        std::stringstream ss;
        ss << "gen_query_" << uuid;

        std::string filename = ss.str() + ".cpp";

        std::string copy_query_file_command = std::string("cp '") + path + std::string("' ") + filename;
        ret = system(copy_query_file_command.c_str());
        
        std::string format_command = std::string("astyle ") + filename;
        ret = system(format_command.c_str());

        std::string copy_last_query_command = std::string("cp '") + filename + std::string("' last_generated_query.cpp");
        ret = system(copy_last_query_command.c_str());

        Timestamp begin_compile = getTimestamp();

        std::stringstream compile_command;
        compile_command << "clang -g -O3 -I ../cogadb/include/ -I ../hype-library/include/ -c -fpic ";
        compile_command << filename << " -o " << ss.str() << ".o";
        ret = system(compile_command.str().c_str());
        if (ret != 0) {
            std::cout << "Compilation Failed!" << std::endl;
            return PipelinePtr();
        } else {
            std::cout << "Compilation Successful!" << std::endl;
        }
        std::stringstream linking_command;
        linking_command << "g++ -shared " << ss.str() << ".o -o " << ss.str() << ".so" << std::endl;
        ret = system(linking_command.str().c_str());
        
        Timestamp end_compile = getTimestamp();
        
        std::stringstream shared_lib;
        shared_lib << "./" << ss.str() << ".so";
        std::cout << "Loading shared library '" << shared_lib.str() << "'" << std::endl;
        void *myso = dlopen(shared_lib.str().c_str(), RTLD_NOW);
        assert(myso != NULL);
        /* get pointer to function "const TablePtr compiled_query(const ScanParam& param)" */        
        CompiledQueryPtr query = (CompiledQueryPtr) dlsym(myso, 
                "_Z14compiled_queryRKSt6vectorIN6CoGaDB18AttributeReferenceESaIS1_EE");
        error = dlerror();
        if (error) {
            std::cerr << error << std::endl;
            return PipelinePtr();
        }
        /* in case no scan parameter was passed, fetch a pointer to a generator 
         * function that returns the scan parameter for this query */
        if(scan_param.empty()){
            /* get pointer to function "const ScanParam getScanParam()" */
            ScanParamGeneratorPtr scan_param_generator = (ScanParamGeneratorPtr) dlsym(myso, 
                    "_Z12getScanParamv");
            error = dlerror();
            if (error) {
                std::cerr << error << std::endl;
                return PipelinePtr();
            }  
            assert(scan_param_generator  != NULL);            
            scan_param =  (*scan_param_generator)(); 
        }

        double compile_time_in_sec = double(end_compile - begin_compile) / (1000 * 1000 * 1000);
        return PipelinePtr(new SharedLibPipeline(query, scan_param, compile_time_in_sec, myso, ss.str()));
    }
    
    const TablePtr compileAndExecuteQueryFile(const std::string& path_to_query_file){
        PipelinePtr pipe = compileQueryFile("test_query_hand_coded.cpp");
        if(!pipe){
            return TablePtr();
        }
        if(!pipe->execute()){
            return TablePtr();
        }
        return pipe->getResult();
    }    
    
}; //end namespace CoGaDB


