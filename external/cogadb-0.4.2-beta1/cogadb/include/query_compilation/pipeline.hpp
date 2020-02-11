/* 
 * File:   pipeline.hpp
 * Author: sebastian
 *
 * Created on 21. August 2015, 14:56
 */

#ifndef PIPELINE_HPP
#define	PIPELINE_HPP

#include <core/attribute_reference.hpp>
#include <util/time_measurement.hpp>

namespace CoGaDB {
    /* Enum that steers how the result is written*/
    enum PipelineEndType{
        MATERIALIZE_FROM_ARRAY_TO_ARRAY,
        MATERIALIZE_FROM_ARRAY_TO_JOIN_HASH_TABLE_AND_ARRAY,
        MATERIALIZE_FROM_AGGREGATION_HASH_TABLE_TO_ARRAY
    };
    
    typedef const TablePtr(*CompiledQueryPtr)(const ScanParam&);
    typedef const ScanParam(*ScanParamGeneratorPtr)();

    class Pipeline {
    public:
        Pipeline(CompiledQueryPtr, const ScanParam& scan_param, double compile_time_in_sec);
        virtual bool execute();

        TablePtr getResult() const;
        double getCompileTimeSec() const;
        double getExecutionTimeSec() const;
        virtual ~Pipeline();
    private:
        CompiledQueryPtr query_;
    protected:
        TablePtr result_;
    private:
        ScanParam scan_param_;
        double compile_time_in_sec_;
        Timestamp begin_;
        Timestamp end_;
    };

    typedef boost::shared_ptr<Pipeline> PipelinePtr;
    
    class SharedLibPipeline : public Pipeline {
    public:

        SharedLibPipeline(CompiledQueryPtr query,
                const ScanParam& scan_param,
                double compile_time_in_sec,
                void* shared_lib,
                const std::string& compiled_query_base_file_name);

        ~SharedLibPipeline();
    private:
        void* shared_lib_;
        std::string compiled_query_base_file_name_;

    };    
    
    /* \brief the DummyPipeline is used to omit compilation steps for empty pipelines and 
     * pass the output of the prior pipeline to the next pipeline or the query processor
     * in case there are no more pipelines. */
    class DummyPipeline : public Pipeline {
    public:
        DummyPipeline(TablePtr result, const ScanParam& scan_param);
        virtual bool execute();
    };   

    const PipelinePtr compileQueryFile(const std::string& path_to_query_file, const ScanParam& param = ScanParam());
    
    const TablePtr compileAndExecuteQueryFile(const std::string& path_to_query_file);
    
}; //end namespace CoGaDB

#endif	/* PIPELINE_HPP */

