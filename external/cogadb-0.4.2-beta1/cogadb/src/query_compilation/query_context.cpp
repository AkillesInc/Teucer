
#include <query_compilation/query_context.hpp>
#include <query_compilation/pipeline.hpp>
#include <core/attribute_reference.hpp>
#include <set>

namespace CoGaDB {
    
    const QueryContextPtr createQueryContext(PipelineType pipe_type){
        return QueryContextPtr(new QueryContext(pipe_type));
    }
    
    QueryContext::QueryContext(PipelineType _pipe_type)
    : accessed_columns(), projection_list(), 
    computed_attributes(), pipe_type(_pipe_type), 
    unresolved_projection_columns(), compilation_time_in_s(0),
    execution_time_in_s(0)
    {
        
    }
    
    void QueryContext::addAccessedColumn(const std::string& column_name){
        accessed_columns.push_back(column_name);
    }
    
    void QueryContext::addColumnToProjectionList(const std::string& column_name){
        projection_list.push_back(column_name);
    }

    void QueryContext::addReferencedAttributeFromOtherPipeline(const AttributeReferencePtr attr){
        referenced_attributes_from_other_pipelines.push_back(attr);
    }
    
    void QueryContext::addComputedAttribute(const std::string& column_name, 
            const AttributeReferencePtr attr){
        assert(attr!=NULL);
        if(computed_attributes.find(column_name)==computed_attributes.end()){
            computed_attributes.insert(std::make_pair(column_name, attr));
        }else{
            COGADB_FATAL_ERROR("Computed attribute exists already!","");
        }
    }
    
    void QueryContext::addUnresolvedProjectionAttribute(const std::string& column_name){
        unresolved_projection_columns.insert(column_name);
    }

    std::set<std::string> QueryContext::getUnresolvedProjectionAttributes(){
        return unresolved_projection_columns;
    }
    
    const AttributeReferencePtr QueryContext::getComputedAttribute(const std::string& column_name){
        if(computed_attributes.find(column_name)!=computed_attributes.end()){
            return computed_attributes[column_name];
        }
        return AttributeReferencePtr();
    }
    
    
    const AttributeReferencePtr QueryContext::getAttributeFromOtherPipelineByName(const std::string& name){
        for(size_t i=0;i<referenced_attributes_from_other_pipelines.size();++i){
            if(referenced_attributes_from_other_pipelines[i]->getUnversionedAttributeName()==name){
                return referenced_attributes_from_other_pipelines[i];
            }
        }
        return AttributeReferencePtr();
    } 
    
    PipelineType QueryContext::getPipelineType() const{
        return pipe_type;
    }    
    
    std::vector<std::string> QueryContext::getProjectionList() const{
        return projection_list;
    }
    
    std::vector<std::string> QueryContext::getAccessedColumns() const{
        return accessed_columns;
    }
    
    std::vector<AttributeReferencePtr> QueryContext::getReferencedAttributeFromOtherPipelines() const{
        return this->referenced_attributes_from_other_pipelines;
    }
    
    void QueryContext::updateStatistics(PipelinePtr pipeline){
        if(!pipeline)
            return;
//        static int counter=0;
//        counter++;
//        std::cout << "Update Stats: " << counter << std::endl;
//        std::cout << "Current Compile Time: " << this->compilation_time_in_s << std::endl;
//        std::cout << "Current Execution Time: " << this->execution_time_in_s << std::endl;
//        
//        std::cout << "Added Compile Time: " << pipeline->getCompileTimeSec() << std::endl;
//        std::cout << "Added Execution Time: " << pipeline->getExecutionTimeSec() << std::endl;
        
        this->compilation_time_in_s+=pipeline->getCompileTimeSec();
        this->execution_time_in_s+=pipeline->getExecutionTimeSec();
    }
    
    void QueryContext::updateStatistics(QueryContextPtr context){
        this->compilation_time_in_s+=context->getCompileTimeSec();
        this->execution_time_in_s+=context->getExecutionTimeSec();
    }
    
    void QueryContext::addExecutionTime(double time_in_seconds){
        this->execution_time_in_s+=time_in_seconds;
    }
    
    double QueryContext::getCompileTimeSec() const{
        return this->compilation_time_in_s;
    }
    double QueryContext::getExecutionTimeSec() const{
        return this->execution_time_in_s;
    }
    
}; //end namespace CoGaDB
