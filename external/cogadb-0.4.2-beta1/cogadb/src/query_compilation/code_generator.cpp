

#include <core/global_definitions.hpp>
#include <core/operator_parameter_types.hpp>
#include <query_compilation/code_generator.hpp>
#include <query_compilation/code_generators/cpp_code_generator.hpp>
#include <query_compilation/code_generators/cuda_c_code_generator.hpp>
#include <util/getname.hpp>
#include <core/variable_manager.hpp>
#include <sstream>
#include <boost/make_shared.hpp>

namespace CoGaDB {

    std::string getComputedAttributeVarName(const AttributeReference& left_attr,
            const AttributeReference& right_attr,
            const ColumnAlgebraOperation& alg_op) {
        std::stringstream ss;
        ss << left_attr.getVersionedAttributeName()
                << "_" << util::getName(alg_op) << "_"
                << right_attr.getVersionedAttributeName();
        return ss.str();
    }

    std::string getComputedAttributeVarName(const AttributeReference& attr,
            const AggregationFunction agg_func) {
        std::stringstream ss;
        ss << attr.getVersionedAttributeName()
                << "_" << util::getName(agg_func);
        return ss.str();
    }

    AttributeReference createComputedAttribute(const AttributeReference& left_attr,
            const AttributeReference& right_attr,
            const ColumnAlgebraOperation& alg_op) {

        std::stringstream new_name;
        new_name << getComputedAttributeVarName(left_attr, right_attr, alg_op);
        return AttributeReference(new_name.str(), DOUBLE, new_name.str(), 1);
    }

    AttributeReference createComputedAttribute(const AttributeReference& left_attr,
            const boost::any& constant,
            const ColumnAlgebraOperation& alg_op) {

        std::stringstream new_name;
        new_name << left_attr.getVersionedAttributeName() << util::getName(alg_op) << toCPPExpression(constant);
        return AttributeReference(new_name.str(), DOUBLE, new_name.str(), 1);
    }

    AttributeReference createComputedAttribute(const boost::any& constant,
            const AttributeReference& right_attr,
            const ColumnAlgebraOperation& alg_op) {

        std::stringstream new_name;
        new_name << toCPPExpression(constant) << util::getName(alg_op) << right_attr.getVersionedAttributeName();
        return AttributeReference(new_name.str(), DOUBLE, new_name.str(), 1);
    }

    AttributeReference createComputedAttribute(const AttributeReference& attr,
            const AggregationFunction& agg_func) {

        std::stringstream new_name;
        new_name << getComputedAttributeVarName(attr, agg_func);
        return AttributeReference(new_name.str(), DOUBLE, new_name.str(), 1);
    }
    
    AttributeReference createComputedAttribute(const AttributeReference& attr,
            const AggregationFunction& agg_func, const std::string& result_name) {

        std::stringstream new_name;
        new_name << getComputedAttributeVarName(attr, agg_func);
        return AttributeReference(new_name.str(), DOUBLE, result_name, 1);
    }

    CodeGenerator::CodeGenerator(const ProjectionParam& _param,
            const TablePtr table,
            const uint32_t version)
    : param(_param),
    scanned_attributes(),
    pipe_end(MATERIALIZE_FROM_ARRAY_TO_ARRAY),
    input_table(table),
    input_table_version(version),
    tuples_produced(false),
    is_empty_pipeline(true){
        assert(input_table != NULL);
        for (size_t i = 0; i < param.size(); ++i) {
            addToScannedAttributes(param[i]);
        }

    }


    CodeGenerator::CodeGenerator(const ProjectionParam& _param)
    : param(_param),
    scanned_attributes(),
    pipe_end(MATERIALIZE_FROM_ARRAY_TO_ARRAY),
    input_table(),
    input_table_version(0),
    tuples_produced(false),
    is_empty_pipeline(true)
    {
        for (size_t i = 0; i < param.size(); ++i) {
            addToScannedAttributes(param[i]);
        }
    }


    CodeGenerator::~CodeGenerator() {
    }

    bool CodeGenerator::addAttributeProjection(const AttributeReference& attr) {
        bool debug_code_generator = VariableManager::instance().getVariableValueBoolean("debug_code_generator");
        if(debug_code_generator)
            std::cout << "[DEBUG]: addAttributeProjection: add " << attr.getVersionedTableName() << "." << attr.getVersionedAttributeName() << std::endl;
        if (attr.getAttributeReferenceType() == INPUT_ATTRIBUTE)
            addToScannedAttributes(attr);
        bool found = false;
        for (size_t i = 0; i < param.size(); ++i) {
            if (param[i].getVersionedAttributeName() == attr.getVersionedAttributeName()
                    && param[i].getAttributeReferenceType() == INPUT_ATTRIBUTE) {
                found = true;
            }
        }

        if (found) {
            return false;
        } else {
            param.push_back(attr);
            return true;
        }
    }

    AttributeReferencePtr CodeGenerator::getProjectionAttributeByName(const std::string& name) const{

        for(size_t i=0;i<param.size();++i){
            if(param[i].getUnversionedAttributeName()==name){
                return AttributeReferencePtr(new AttributeReference(param[i]));
            }
        }
        return AttributeReferencePtr();
    }

    AttributeReferencePtr CodeGenerator::getScannedAttributeByName(const std::string& name) const{

        for(size_t i=0;i<scanned_attributes.size();++i){
            if(scanned_attributes[i].getUnversionedAttributeName()==name){
                return AttributeReferencePtr(new AttributeReference(scanned_attributes[i]));
            }
        }
        return AttributeReferencePtr();
    }

    bool CodeGenerator::isEmpty() const{
        return is_empty_pipeline;
    }
    
    
    void CodeGenerator::print() const{
        std::ostream& out = std::cout;
        out << "Code Generator: " << std::endl;
        out << "\tScanned Attributes: " << std::endl;
        for(size_t i=0;i<scanned_attributes.size();++i){
            out << "\t\t" << toString(scanned_attributes[i]) << std::endl;
        }
        out << "\tProjected Attributes: " << std::endl;
        for(size_t i=0;i<param.size();++i){
            out << "\t\t" << toString(param[i]) << std::endl;
        }
        out << "\tIs Empty Pipeline: " << is_empty_pipeline << std::endl;
        if(input_table){
            out << "\t" << input_table->getName() << std::endl;
        }
//        out << "\tPipeline End Type: " << getName(pipe_end) << std::endl;
    }

    bool CodeGenerator::dropProjectionAttributes(){

        param.clear();

        return true;
    }

    bool CodeGenerator::createForLoop(){
        return createForLoop_impl(this->input_table, this->input_table_version);
    }

    bool CodeGenerator::createForLoop(const TablePtr table, uint32_t version){


        if(!table)
            return false;
        if(!this->input_table){
            this->input_table=table;
            this->input_table_version=version;
        }
        return createForLoop_impl(table, version);
    }

    bool CodeGenerator::addToScannedAttributes(const AttributeReference& attr) {
        bool found = false;
        /* ignore all attributes that are computed and add the input columns only */
        if (attr.getAttributeReferenceType() != INPUT_ATTRIBUTE) {
            return false;
        }
        bool debug_code_generator = VariableManager::instance()
            .getVariableValueBoolean("debug_code_generator");
        for (size_t i = 0; i < scanned_attributes.size(); ++i) {
            if (scanned_attributes[i].getVersionedAttributeName() == attr.getVersionedAttributeName()) {
                found = true;
                if(debug_code_generator){
                    std::cout << "[DEBUG]: found attribute in scanned attributes: "
                            << attr.getVersionedTableName() << "."
                            << attr.getVersionedAttributeName() << std::endl;
                }
            }
        }
        if (found) {
            if(debug_code_generator){
                std::cout << "[DEBUG]: addToScannedAttributes: I will NOT add "
                        << attr.getVersionedTableName() << "."
                        << attr.getVersionedAttributeName() << std::endl;
            }
            return false;
        } else {
            if(debug_code_generator){
                std::cout << "[DEBUG]: addToScannedAttributes: add "
                        << attr.getVersionedTableName() << "."
                        << attr.getVersionedAttributeName() << std::endl;
            }
            scanned_attributes.push_back(attr);
            return true;
        }
    }

    bool CodeGenerator::consumeSelection(const PredicateExpressionPtr pred_expr) {
        if (!pred_expr) {
            return false;
        }
        is_empty_pipeline=false;
        std::vector<AttributeReferencePtr> scanned = pred_expr->getScannedAttributes();
        for (size_t i = 0; i < scanned.size(); ++i) {
            if (scanned[i])
                this->addToScannedAttributes(*scanned[i]);
        }
        return consumeSelection_impl(pred_expr);
    }

    bool CodeGenerator::consumeBuildHashTable(const AttributeReference& attr) {

        this->pipe_end = MATERIALIZE_FROM_ARRAY_TO_JOIN_HASH_TABLE_AND_ARRAY;
        is_empty_pipeline=false;
        this->addToScannedAttributes(attr);

        return consumeBuildHashTable_impl(attr);
    }

    bool CodeGenerator::consumeProbeHashTable(const AttributeReference& hash_table_attr,
            const AttributeReference& probe_attr) {

        assert(hash_table_attr.getHashTable() != NULL);
        assert(hash_table_attr.getTable()->getHashTablebyName(hash_table_attr.getUnversionedAttributeName()) != NULL);
        is_empty_pipeline=false;
        this->addToScannedAttributes(hash_table_attr);
        this->addToScannedAttributes(probe_attr);

        return consumeProbeHashTable_impl(hash_table_attr, probe_attr);
    }

    bool CodeGenerator::consumeHashGroupAggregate(const GroupByAggregateParam& groupby_param) {
        is_empty_pipeline=false;
        /* check whether we have not inserted a pipeline breaking operator before
           if we did, return with error, otherwise, mark that we inserted a
           pipeline breaker */
        if (pipe_end != MATERIALIZE_FROM_ARRAY_TO_ARRAY) {
            COGADB_FATAL_ERROR("Cannot insert two pipeline breaking operations into the same pipeline!", "");
            return false;
        }
        pipe_end = MATERIALIZE_FROM_AGGREGATION_HASH_TABLE_TO_ARRAY;

        param.clear();

        const AggregateSpecifications& aggr_specs = groupby_param.aggregation_specs;
        const GroupingAttributes& grouping_attrs = groupby_param.grouping_attrs;
        bool ret = true;

        for (size_t i = 0; i < grouping_attrs.size(); ++i) {
            AttributeReference attr_ref = grouping_attrs[i];
            ret = this->addAttributeProjection(attr_ref);
            this->addToScannedAttributes(attr_ref);
            assert(ret == true);
        }
        for (size_t i = 0; i < aggr_specs.size(); ++i) {
            AttributeReference computed_attr = aggr_specs[i].result_attr;
            this->addToScannedAttributes(aggr_specs[i].scan_attr);
            ret = this->addAttributeProjection(computed_attr);
            assert(ret == true);
        }

        return consumeHashGroupAggregate_impl(groupby_param);
    }

    bool CodeGenerator::consumeAggregate(const AggregateSpecifications& aggr_specs) {
        is_empty_pipeline=false;
        /* check whether we have not inserted a pipeline breaking operator before
           if we did, return with error, otherwise, mark that we inserted a
           pipeline breaker */
        if (pipe_end != MATERIALIZE_FROM_ARRAY_TO_ARRAY) {
            COGADB_FATAL_ERROR("Cannot insert two pipeline breaking operations into the same pipeline!", "");
            return false;
        }
        pipe_end = MATERIALIZE_FROM_AGGREGATION_HASH_TABLE_TO_ARRAY;

        param.clear();

        bool ret = true;
        for (size_t i = 0; i < aggr_specs.size(); ++i) {
            this->addToScannedAttributes(aggr_specs[i].scan_attr);
            ret = this->addAttributeProjection(aggr_specs[i].result_attr);
            assert(ret == true);
        }


        return consumeAggregate_impl(aggr_specs);
    }

    const std::pair<bool, AttributeReference> CodeGenerator::consumeAlgebraComputation(const AttributeReference& left_attr,
            const AttributeReference& right_attr,
            const ColumnAlgebraOperation& alg_op) {

        is_empty_pipeline=false;
        this->addToScannedAttributes(left_attr);
        this->addToScannedAttributes(right_attr);

        return consumeAlgebraComputation_impl(left_attr, right_attr, alg_op);
    }

    const std::pair<bool, AttributeReference>
    CodeGenerator::consumeAlgebraComputation(const AttributeReference& left_attr,
            const boost::any constant,
            const ColumnAlgebraOperation& alg_op) {

        is_empty_pipeline=false;
        this->addToScannedAttributes(left_attr);

        return consumeAlgebraComputation_impl(left_attr, constant, alg_op);
    }

    const std::pair<bool, AttributeReference>
    CodeGenerator::consumeAlgebraComputation(const boost::any constant,
            const AttributeReference& right_attr,
            const ColumnAlgebraOperation& alg_op) {

        is_empty_pipeline=false;
        this->addToScannedAttributes(right_attr);

        return consumeAlgebraComputation_impl(constant, right_attr, alg_op);
    }

    bool CodeGenerator::canOmitCompilation() const{
        /* did we add operators into the pipeline? */
        if(this->isEmpty()){
            /* When a pipeline has the same input and output and does not do 
             * anything with the input, we can omit the compilation step. */
            assert(this->input_table!=NULL);
            if(isEquivalent(param,this->input_table->getSchema())){
                return true;
            }else{
                return false;
            }
        }else{
            return false;
        }
    }    
    
    const CodeGeneratorPtr createCodeGenerator(const CodeGeneratorType code_gen,
            const ProjectionParam& param,
            const TablePtr input_table,
            const boost::any& generic_code_gen_param,
            uint32_t version) {

		CodeGeneratorPtr resultCodeGenerator;

        if (code_gen == CPP_CODE_GENERATOR) {
            resultCodeGenerator = boost::make_shared<CPPCodeGenerator>(param, input_table, version);
        }
        else if (code_gen == CUDA_C_CODE_GENERATOR) {
            resultCodeGenerator = boost::make_shared<CUDA_C_CodeGenerator>(param, input_table, version);
        } else {
            COGADB_FATAL_ERROR("Unknown Code Generator!", "");
            return CodeGeneratorPtr();
        }

        resultCodeGenerator->createForLoop();
        return resultCodeGenerator;

    }

    const CodeGeneratorPtr createCodeGenerator(const CodeGeneratorType code_gen,
               const ProjectionParam& param,
               const boost::any& generic_code_gen_param) {

        if (code_gen == CPP_CODE_GENERATOR) {
            return boost::make_shared<CPPCodeGenerator>(param);
        }
        else if (code_gen == CUDA_C_CODE_GENERATOR) {
            return boost::make_shared<CUDA_C_CodeGenerator>(param);
        } else {
            COGADB_FATAL_ERROR("Unknown Code Generator!", "");
            return CodeGeneratorPtr();
        }
    }
    
    bool isEquivalent(const ProjectionParam& projected_attributes,
        const TableSchema& input_schema){
        if(projected_attributes.size()!=input_schema.size())
            return false;
        for(size_t i=0;i<projected_attributes.size();++i){
            bool found=false; 
            TableSchema::const_iterator cit;
            for(cit=input_schema.begin();cit!=input_schema.end();++cit){
                if(projected_attributes[i].getUnversionedAttributeName()==cit->second){
                    found=true;
                }
            }
            if(!found){
                return false;
            }
        }     
        return true;
    }


}; //end namespace CoGaDB
