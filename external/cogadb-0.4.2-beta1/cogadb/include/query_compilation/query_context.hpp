/* 
 * File:   query_context.hpp
 * Author: sebastian
 *
 * Created on 1. September 2015, 15:01
 */

#ifndef QUERY_CONTEXT_HPP
#define	QUERY_CONTEXT_HPP

#include <ios>
#include <string>
#include <vector>
#include <map>
#include <set>
#include <boost/shared_ptr.hpp>

namespace CoGaDB {
    
    class QueryContext;
    typedef boost::shared_ptr<QueryContext> QueryContextPtr;
    
    class AttributeReference;
    typedef boost::shared_ptr<AttributeReference> AttributeReferencePtr;
    
    class Pipeline;
    typedef boost::shared_ptr<Pipeline> PipelinePtr;
    
    enum PipelineType{
        NORMAL_PIPELINE,
        BUILD_HASH_TABLE_PIPELINE,
        AGGREGATE_PIPELINE
    };    
    
    const QueryContextPtr createQueryContext(PipelineType pipe = NORMAL_PIPELINE);

    class QueryContext{
    public:
        void addAccessedColumn(const std::string& column_name);
        void addColumnToProjectionList(const std::string& column_name);
        void addReferencedAttributeFromOtherPipeline(const AttributeReferencePtr attr);
        void addComputedAttribute(const std::string& column_name, const AttributeReferencePtr attr);
        void addUnresolvedProjectionAttribute(const std::string& column_name);
        
        std::set<std::string> getUnresolvedProjectionAttributes();
        
        const AttributeReferencePtr getComputedAttribute(const std::string& column_name);
        
        const AttributeReferencePtr getAttributeFromOtherPipelineByName(const std::string& name);
        
        PipelineType getPipelineType() const;
     
        std::vector<std::string> getProjectionList() const;
        std::vector<std::string> getAccessedColumns() const;
        std::vector<AttributeReferencePtr> getReferencedAttributeFromOtherPipelines() const;
        
        void updateStatistics(PipelinePtr);
        void updateStatistics(QueryContextPtr);
        void addExecutionTime(double time_in_seconds);
        double getCompileTimeSec() const;
        double getExecutionTimeSec() const;
    private:
        friend const QueryContextPtr createQueryContext(PipelineType);
        QueryContext(PipelineType);
        std::vector<std::string> projection_list;
        std::vector<std::string> accessed_columns;
        std::vector<AttributeReferencePtr> referenced_attributes_from_other_pipelines;
        typedef std::map<std::string,AttributeReferencePtr> ComputedAttributes;
        ComputedAttributes computed_attributes;
        std::set<std::string> unresolved_projection_columns;        
        PipelineType pipe_type;
        double compilation_time_in_s;
        double execution_time_in_s;
    };
    
}; //end namespace CoGaDB

#endif	/* QUERY_CONTEXT_HPP */

