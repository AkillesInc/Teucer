
#include <list>
#include <sstream>
#include <iomanip>
#include <set>

#include <core/selection_expression.hpp>
#include <query_compilation/code_generators/cpp_code_generator.hpp>
#include <util/getname.hpp>
#include <util/iostream.hpp>

#include <core/data_dictionary.hpp>
#include <stdlib.h>
#include <dlfcn.h>
#include <util/time_measurement.hpp>
#include <util/functions.hpp>

#include <boost/uuid/uuid.hpp>            // uuid class
#include <boost/uuid/uuid_generators.hpp> // generators
#include <boost/uuid/uuid_io.hpp>
#include <boost/filesystem/operations.hpp>
#include <ctime>

#include <google/dense_hash_map>         // streaming operators etc.

#include <util/code_generation.hpp>
#include <core/variable_manager.hpp>

namespace CoGaDB {

    CPPCodeGenerator::CPPCodeGenerator(const ProjectionParam& _param,
            const TablePtr table,
            uint32_t version)
    : CodeGenerator(_param, table, version),
    header_and_types_block(),
    fetch_input_code_block(),
    generated_code(),
    upper_code_block(),
    lower_code_block(),
    after_for_loop_block(),
    create_result_table_code_block(),
    cleanup_code() {

        init();
    }


    CPPCodeGenerator::CPPCodeGenerator(const ProjectionParam& _param)
    : CodeGenerator(_param),
    header_and_types_block(),
    fetch_input_code_block(),
    generated_code(),
    upper_code_block(),
    lower_code_block(),
    after_for_loop_block(),
    create_result_table_code_block(),
    cleanup_code()
    {
        init();
    }


    void CPPCodeGenerator::init(){
        std::stringstream ss;
        ss << "#include <query_compilation/minimal_api.hpp>" << std::endl;
//        ss << "#include <core/attribute_reference.hpp>" << std::endl;
//        ss << "#include <boost/unordered_map.hpp>" << std::endl;
//        ss << "extern \"C\" {" << std::endl;
//        ss << "#include <hardware_optimizations/main_memory_joins/serial_hash_join/hashtable/hashtable.h>" << std::endl;
//        ss << "}" << std::endl;
        ss << "using namespace CoGaDB;" << std::endl;
        ss << std::endl;

        header_and_types_block << ss.str();
        /* write function signature */
        this->fetch_input_code_block << "const TablePtr compiled_query(const ScanParam& param){" << std::endl;
    }


    const std::string toCPPType(const AttributeType& attr) {

        if (attr == INT) {
            return "int32_t";
        } else if (attr == FLOAT) {
            return "float";
        } else if (attr == DOUBLE) {
            return "double";
        } else if (attr == OID) {
            return "uint64_t";
        } else if (attr == VARCHAR) {
            return "string";
        } else if (attr == DATE || attr == UINT32) {
            return "uint32_t";
        } else if (attr == CHAR) {
            return "char";
        } else {
            COGADB_FATAL_ERROR("", "");
        }
        return "<INVALID_TYPE>";
    }

//    const std::string toCPPOperator(const ColumnAlgebraOperation& op) {
//        //enum ColumnAlgebraOperation{ADD,SUB,MUL,DIV};
//        const char * const names[] = {"+", "-", "*", "/"};
//        return std::string(names[op]);
//    }

    const std::string toCPPExpression(const boost::any constant) {
        std::stringstream ss;
        assert(!constant.empty());
        if (constant.type() == typeid (int32_t)) {
            ss << boost::any_cast<int32_t>(constant);
        } else if (constant.type() == typeid (uint32_t)) {
            ss << boost::any_cast<uint32_t>(constant);
        } else if (constant.type() == typeid (int64_t)) {
            ss << boost::any_cast<int64_t>(constant);
        } else if (constant.type() == typeid (uint64_t)) {
            ss << boost::any_cast<uint64_t>(constant);
        } else if (constant.type() == typeid (int16_t)) {
            ss << boost::any_cast<int16_t>(constant);
        } else if (constant.type() == typeid (uint16_t)) {
            ss << boost::any_cast<uint16_t>(constant);
        } else if (constant.type() == typeid (float)) {
            ss << boost::any_cast<float>(constant) << "f";
        } else if (constant.type() == typeid (double)) {
            ss << boost::any_cast<double>(constant);
        } else if (constant.type() == typeid (char)) {
            ss << boost::any_cast<char>(constant);
        } else if (constant.type() == typeid (std::string)) {
            ss << boost::any_cast<std::string>(constant);
        } else {
            COGADB_FATAL_ERROR("Unknown constant type: " << constant.type().name(), "");
        }
        return ss.str();
    }

    const std::string getCPPExpression(LogicalOperation log_op) {
        const char * const names[] = {"&&", "||"};
        return std::string(names[log_op]);
    }

    const std::string getCPPExpression(ValueComparator x) {
        //enum ValueComparator{LESSER,GREATER,EQUAL,LESSER_EQUAL,GREATER_EQUAL, UNEQUAL};
        const char * const names[] = {"<", ">", "==", "<=", ">=", "!="};
        return std::string(names[x]);
    }

    const std::string getTupleIDVarName(const AttributeReference& attr_ref) {
        std::stringstream ss;
        ss << "tuple_id_" << attr_ref.getVersionedTableName();
        return ss.str();
    }

    const std::string getTupleIDVarName(const TablePtr table, uint32_t version) {
        //        if(!table) return "<ERROR>";
        assert(table != NULL);
        std::stringstream ss;
        ss << "tuple_id_" << table->getName() << version;
        return ss.str();
    }

    const std::string getInputArrayVarName(const AttributeReference& attr_ref) {
        std::stringstream ss;
        ss << "array_" << attr_ref.getVersionedAttributeName();
        return ss.str();
    }

    const std::string getInputColumnVarName(const AttributeReference& attr_ref) {
        std::stringstream ss;
        ss << "col_" << attr_ref.getVersionedAttributeName();
        return ss.str();
    }

    const std::string getTableVarName(const AttributeReference& attr_ref) {
        std::stringstream ss;
        ss << "table_" << attr_ref.getVersionedTableName();
        return ss.str();
    }

    const std::string getTableVarName(const TablePtr table, uint32_t version) {
        assert(table != NULL);
        std::stringstream ss;
        ss << "table_" << table->getName() << version;
        return ss.str();
    }

    const std::string getResultArrayVarName(const AttributeReference& attr_ref) {
        std::stringstream ss;
        ss << "result_array_" << attr_ref.getResultAttributeName();
        return ss.str();
    }

    const std::string getHashTableVarName(const AttributeReference& attr_ref) {
        std::stringstream ss;
        ss << "ht_" << attr_ref.getVersionedAttributeName();
        return ss.str();
    }

    const std::string getComputedColumnVarName(const AttributeReference& attr_ref) {
        assert(attr_ref.getAttributeReferenceType() == COMPUTED_ATTRIBUTE);
        std::stringstream ss;
        ss << "computed_var_" << attr_ref.getVersionedAttributeName();
        return ss.str();
    }

    const std::string getVarName(const AttributeReference& attr_ref) {
        if (attr_ref.getAttributeReferenceType() == INPUT_ATTRIBUTE) {
            //            std::stringstream ss;
            //            ss << getInputArrayVarName(attr_ref)
            //               << "[" << getTupleIDVarName(attr_ref) << "]";
            return getInputArrayVarName(attr_ref);
        } else if (attr_ref.getAttributeReferenceType() == COMPUTED_ATTRIBUTE) {
            return getComputedColumnVarName(attr_ref);
        } else {
            COGADB_FATAL_ERROR("", "");
            return std::string();
        }
    }

    const std::string getElementAccessExpression(const AttributeReference& attr_ref) {
        if (attr_ref.getAttributeReferenceType() == INPUT_ATTRIBUTE) {
            std::stringstream ss;
            ss << getInputArrayVarName(attr_ref)
                    << "[" << getTupleIDVarName(attr_ref) << "]";
            return ss.str();
        } else if (attr_ref.getAttributeReferenceType() == COMPUTED_ATTRIBUTE) {
            return getComputedColumnVarName(attr_ref);
        } else {
            COGADB_FATAL_ERROR("", "");
            return std::string();
        }
    }

    const std::string getCompressedElementAccessExpression(const AttributeReference& attr_ref) {
        /* non input attributes are never compressed */
        if(!isInputAttribute(attr_ref)){
            return getElementAccessExpression(attr_ref);
        }
        /* check which compression the input attribute has */
        ColumnType col_type = getColumnType(attr_ref);
        if(col_type==PLAIN_MATERIALIZED){
            return getElementAccessExpression(attr_ref);
        }else if(col_type==DICTIONARY_COMPRESSED){
            std::stringstream ss;
            ss << getInputArrayVarName(attr_ref) << "_dict_ids"
                    << "[" << getTupleIDVarName(attr_ref) << "]";
            return ss.str();
        }else{
            COGADB_FATAL_ERROR("Encountered compression scheme not supported by "
                    "query compiler: " << util::getName(col_type),"");
            return std::string();
        }
    }

    const std::string getGroupTIDVarName(const AttributeReference& attr_ref) {
        std::stringstream ss;
        ss << "group_tid_" << attr_ref.getVersionedAttributeName();
        return ss.str();
    }

//    bool isGroupingAttribute(const GroupbyParam& groupby_param,
//            const AttributeReference& attr_ref) {
//        GroupingColumns::const_iterator cit;
//        for (cit = groupby_param.grouping_columns.begin();
//                cit != groupby_param.grouping_columns.end();
//                ++cit) {
//            if ((*cit) == attr_ref.getVersionedAttributeName()) {
//                return true;
//            }
//        }
//        return false;
//    }
//
//    bool isAggregationAttribute(const GroupbyParam& groupby_param,
//            const AttributeReference& attr_ref) {
//        AggregationFunctions::const_iterator cit;
//        for (cit = groupby_param.aggregation_functions.begin();
//                cit != groupby_param.aggregation_functions.end();
//                ++cit) {
//            if (cit->first == attr_ref.getVersionedAttributeName()) {
//                return true;
//            }
//        }
//        return false;
//    }

    const AggregationParam getAggregationParam(const GroupbyParam& groupby_param,
            const AttributeReference& attr_ref) {
        AggregationFunctions::const_iterator cit;
        for (cit = groupby_param.aggregation_functions.begin();
                cit != groupby_param.aggregation_functions.end();
                ++cit) {
            if (cit->first == attr_ref.getVersionedAttributeName()) {
                return cit->second;
            }
        }
        COGADB_FATAL_ERROR("Not found attribute "
                << attr_ref.getVersionedAttributeName()
                << " in aggregation functions of groupby parameter!", "");
        return AggregationParam(ProcessorSpecification(hype::PD0),
                COUNT,
                HASH_BASED_AGGREGATION,
                "");
    }

    bool CPPCodeGenerator::produceTuples(const ScanParam& param) {

        if (this->tuples_produced)
            return true;

        this->tuples_produced = true;

        //        std::set<TablePtr> scanned_tables;
        std::set<std::string> scanned_tables;

        bool debug_code_generator = VariableManager::instance()
            .getVariableValueBoolean("debug_code_generator");
        
        size_t param_id = 0;
        ScanParam::const_iterator cit;
        for (cit = param.begin(); cit != param.end(); ++cit, ++param_id) {
            if (cit->getTable() == NULL) {
                COGADB_FATAL_ERROR("Found no valid TablePtr in Scan Attribute: " << cit->getVersionedAttributeName(), "");
            }
            if (scanned_tables.find(cit->getTable()->getName()) == scanned_tables.end()) {
                //we assume that the same ScanParam passed to this function
                //is also passed to the compiled query function
                fetch_input_code_block << "TablePtr " << getTableVarName(*cit) << "=param[" << param_id << "].getTable();" << std::endl;
                fetch_input_code_block << "assert(" << getTableVarName(*cit) << "!=NULL);    " << std::endl;
                scanned_tables.insert(cit->getTable()->getName()); //cit->getTable());
                if(debug_code_generator)
                    std::cout << "[DEBUG:] Produce Tuples for Table " << getTableVarName(*cit) << std::endl;
            }
            fetch_input_code_block << "ColumnPtr " << getInputColumnVarName(*cit)
                    << " = getColumn(" << getTableVarName(*cit)
                    << ", \"" << cit->getUnversionedAttributeName()
                    << "\");" << std::endl;
            fetch_input_code_block << "if(!" << getInputColumnVarName(*cit)
                    << ") {std::cout << \"Column '"
                    << getInputColumnVarName(*cit)
                    << "' not found!\" << std::endl; return TablePtr();}"
                    << std::endl;
            AttributeType attr = cit->getAttributeType();
            fetch_input_code_block << toCPPType(attr) << "* "
                    << getInputArrayVarName(*cit)
                    << " = getArrayFromColumn_" << toCPPType(attr)
                    << "(" << getInputColumnVarName(*cit) << ");"
                    << std::endl;

            if(getColumnType(*cit)==DICTIONARY_COMPRESSED){
                fetch_input_code_block << "ColumnPtr " << getInputColumnVarName(*cit) << "_dict_ids"
                    << " = getDictionaryCompressedColumn(" << getTableVarName(*cit)
                    << ", \"" << cit->getUnversionedAttributeName()
                    << "\");" << std::endl;
                fetch_input_code_block << "uint32_t* "
                    << getInputArrayVarName(*cit) << "_dict_ids"
                    << " = getArrayCompressedKeysFromColumn_string"
                    << "(" << getInputColumnVarName(*cit) << "_dict_ids"  << ");"
                    << std::endl;
            }
        }

        return true;
    }

    bool CPPCodeGenerator::createForLoop_impl(const TablePtr table,
            uint32_t version) {


        std::string loop_table_tuple_id = getTupleIDVarName(table, version);
        std::stringstream for_loop;
        for_loop << "size_t num_elements_" << table->getName() << "_" << version
                << " = getNumberOfRows(table_" << table->getName() << version
                << ");" << std::endl;
        //            for_loop << "#pragma omp parallel for" << std::endl;
        for_loop << "for(size_t " << loop_table_tuple_id
                << "=0;" << loop_table_tuple_id
                << "<num_elements_" << table->getName() << "_" << version
                << ";++" << loop_table_tuple_id << "){";
        this->upper_code_block.push_back(for_loop.str());

        this->lower_code_block.push_front("}");
        return true;
    }

    const std::string getCodeMallocResultMemory(const ProjectionParam& param) {
        std::stringstream ss;
        for (size_t i = 0; i < param.size(); ++i) {
            AttributeType attr = param[i].getAttributeType();
            if (attr == VARCHAR) {
                ss << toCPPType(attr) << "* ";
                ss << getResultArrayVarName(param[i]);
                ss << " = stringMalloc(allocated_result_elements);" << std::endl;
            } else {
                ss << toCPPType(attr) << "* ";
                ss << getResultArrayVarName(param[i]);
                ss << " = (" << toCPPType(attr)
                        << "*) realloc(NULL, allocated_result_elements*sizeof("
                        << toCPPType(attr) << "));" << std::endl;
            }
        }
        return ss.str();
    }

    const std::string getCodeReallocResultMemory(const ProjectionParam& param) {
        std::stringstream ss;
        for (size_t i = 0; i < param.size(); ++i) {
            AttributeType attr = param[i].getAttributeType();
            if (attr == VARCHAR) {
                ss << getResultArrayVarName(param[i]);
                ss << " = stringRealloc(" << getResultArrayVarName(param[i])
                        << ",  allocated_result_elements);" << std::endl;
            } else {
                ss << getResultArrayVarName(param[i])
                        << " = (" << toCPPType(attr) << "*) realloc("
                        << getResultArrayVarName(param[i])
                        << ", allocated_result_elements*sizeof(" << toCPPType(attr)
                        << "));" << std::endl;
            }
        }
        return ss.str();
    }

    const std::string CPPCodeGenerator::getCodeAllocateResultTable() const {

        std::stringstream ss;
        ss << "size_t current_result_size=0;" << std::endl;
        ss << "size_t allocated_result_elements=10000;" << std::endl;
        ss << getCodeMallocResultMemory(param);
        return ss.str();
    }

    const std::string CPPCodeGenerator::getCodeWriteResult() const {

        std::stringstream ss;
        ss << "if(current_result_size>=allocated_result_elements){" << std::endl;
        ss << "allocated_result_elements*=1.4;" << std::endl;
        ss << getCodeReallocResultMemory(param);
        ss << "}" << std::endl;
        for (size_t i = 0; i < param.size(); ++i) {
            AttributeType attr = param[i].getAttributeType();
            ss << getResultArrayVarName(param[i]) << "[current_result_size]="
                    << getElementAccessExpression(param[i]) << ";" << std::endl;
        }
        ss << "current_result_size++;" << std::endl;
        return ss.str();
    }

    const std::string CPPCodeGenerator::getCodeWriteResultFromHashTable() const {

        std::stringstream write_result;
        /* write result */

        write_result << "if(aggregation_hash_table.size()>=allocated_result_elements){" << std::endl;
        write_result << "allocated_result_elements=aggregation_hash_table.size();" << std::endl;
        write_result << getCodeReallocResultMemory(param);
        write_result << "}" << std::endl;

        write_result << "for (it = aggregation_hash_table.begin(); it != aggregation_hash_table.end(); ++it) {" << std::endl;

        write_result << "current_result_size++;" << std::endl;

        write_result << "}" << std::endl;


        return write_result.str();

    }

    const std::string CPPCodeGenerator::createResultTable() const {

        std::stringstream ss;
        ss << "std::vector<ColumnPtr> result_columns;" << std::endl;
        for (size_t i = 0; i < param.size(); ++i) {
            AttributeType attr = param[i].getAttributeType();
            ss << "result_columns.push_back(createResultArray_" << toCPPType(attr) << "("
                    << "\"" << param[i].getResultAttributeName() << "\", "
                    << getResultArrayVarName(param[i])
                    << ", current_result_size));" << std::endl;
        }
        ss << "TablePtr result_table=createTableFromColumns(\""
                << this->input_table->getName() << "\", &result_columns[0], result_columns.size());" << std::endl;
        ss << "/* Add build hash tables to result table. */" << std::endl;
        ss << create_result_table_code_block.str() << std::endl;
        ss << "/* Clean up resources. */" << std::endl;
        ss << cleanup_code.str() << std::endl;
        ss << "return result_table;" << std::endl;
        return ss.str();
    }

    const std::string CPPCodeGenerator::getCPPExpression(ValueComparator x) const {
        //enum ValueComparator{LESSER,GREATER,EQUAL,LESSER_EQUAL,GREATER_EQUAL, UNEQUAL};
        const char * const names[] = {"<", ">", "==", "<=", ">=", "!="};
        return std::string(names[x]);
    }

    const std::string CPPCodeGenerator::getCPPExpression(const Predicate& pred) const {
        std::stringstream ss;
        if (pred.getPredicateType() == ValueValuePredicate) {
            AttributeReference attr1 = getAttributeReference(pred.getColumn1Name());
            AttributeReference attr2 = getAttributeReference(pred.getColumn2Name());
            ss << getInputArrayVarName(attr1) << "[" << getTupleIDVarName(attr1) << "]"; //column1_name_;
            ss << getCPPExpression(pred.getValueComparator());
            ss << getInputArrayVarName(attr2) << "[" << getTupleIDVarName(attr2) << "]";
        } else if (pred.getPredicateType() == ValueConstantPredicate) {
            AttributeReference attr1 = getAttributeReference(pred.getColumn1Name());
            ss << getInputArrayVarName(attr1) << "[" << getTupleIDVarName(attr1) << "]";
            ss << getCPPExpression(pred.getValueComparator());
            //workaround to avoid errors between float and double representations
            if (pred.getConstant().type() == typeid (float)) {
                ss << pred.getConstant() << "f";
            } else if (pred.getConstant().type() == typeid (std::string)) {
                ss << "\"" << pred.getConstant() << "\"";
            } else {
                ss << pred.getConstant();
            }
        } else if (pred.getPredicateType() == ValueRegularExpressionPredicate) {
            COGADB_FATAL_ERROR("Unsupported PredicateType: ValueRegularExpressionPredicate", "");
            //                result+=column1_name_;
            //                if(this->comp_==EQUAL){
            //                    result+=" LIKE ";
            //                }else if(this->comp_==UNEQUAL){
            //                    result+=" NOT LIKE ";
            //                }else{
            //                    COGADB_FATAL_ERROR("Detected invalid parameter combination! ValueComparator may only be EQUAL or UNEQUAL for ValueRegularExpressionPredicates!","");
            //                }
            //                result+=util::getName(comp_);
            //                std::stringstream ss;
            //                ss << constant_;
            //                result+=ss.str();
        } else {
            COGADB_FATAL_ERROR("Invalid PredicateType!", "");
        }
        return ss.str();
    }

    const AttributeReference CPPCodeGenerator::getAttributeReference(const std::string& column_name) const {

        COGADB_FATAL_ERROR("Called Obsolete Function!", "");
        size_t num_occurences = 0;

        for (size_t i = 0; i < scanned_attributes.size(); ++i) {
            if (scanned_attributes[i].getVersionedAttributeName() == column_name) {
                num_occurences++;
            }
        }
        if (num_occurences == 0) {
            COGADB_FATAL_ERROR("Could not found attribute '" << column_name
                    << "' in scanned attributes!", "");
            return scanned_attributes[0];
        } else if (num_occurences == 1) {
            for (size_t i = 0; i < scanned_attributes.size(); ++i) {
                if (scanned_attributes[i].getVersionedAttributeName() == column_name) {
                    return scanned_attributes[i];
                }
            }
        } else {
            COGADB_FATAL_ERROR("Ambiguous Attribute Name: Found attribute " << column_name
                    << "multiple times in scanned attributes!", "");
            return scanned_attributes[0];
        }

    }

    bool CPPCodeGenerator::consumeSelection_impl(const PredicateExpressionPtr pred_expr) {

        std::stringstream ss;
        ss << "if(" << pred_expr->getCPPExpression() << "){";
        this->upper_code_block.push_back(ss.str());
        this->lower_code_block.push_front("}");

        return true;
    }

    bool CPPCodeGenerator::createHashTable(const AttributeReference& attr) {
        generated_code << "hashtable_t* " << getHashTableVarName(attr) << "=hash_new (10000);" << std::endl;
        return true;
    }

#define USE_SIMPLE_HASH_TABLE_FOR_JOINS
//#define USE_GOOGLE_HASH_TABLE_FOR_JOINS

    bool CPPCodeGenerator::consumeBuildHashTable_impl(const AttributeReference& attr) {

        /* add code for building hash table inside for loop */
        std::stringstream hash_table;

#ifdef USE_SIMPLE_HASH_TABLE_FOR_JOINS
        /* add code for hash table creation before for loop */
        generated_code << "hashtable_t* " << getHashTableVarName(attr) << "=hash_new (10000);" << std::endl;
        /* insert values into hash table */
        hash_table << "tuple_t t_" << attr.getVersionedAttributeName()
                << " = {" << getInputArrayVarName(attr)
                << "[" << getTupleIDVarName(attr) << "], "
                //                   <<  getTupleIDVarName(attr) << "};" << std::endl;
                << "current_result_size};" << std::endl;
        hash_table << "hash_put(" << getHashTableVarName(attr);
        hash_table << ", t_" << attr.getVersionedAttributeName() << ");" << std::endl;

#endif

#ifdef USE_GOOGLE_HASH_TABLE_FOR_JOINS
        /* add code for hash table creation before for loop */
        generated_code << "std::tr1::unordered_multimap<TID,TID>* "
                << getHashTableVarName(attr)
                << "=new std::tr1::unordered_multimap<TID,TID>();" << std::endl;
//        generated_code << getHashTableVarName(attr)
//                << "->set_empty_key(std::numeric_limits<TID>::max());"
//                << std::endl;
        /* insert values into hash table */
        hash_table << getHashTableVarName(attr)
                << "->insert(std::make_pair(" << getInputArrayVarName(attr)
                << "[" << getTupleIDVarName(attr) << "], "
                //                   <<  getTupleIDVarName(attr) << "};" << std::endl;
                << "current_result_size));" << std::endl;
//        hash_table << "hash_put(" << getHashTableVarName(attr);
//        hash_table << ", t_" << attr.getVersionedAttributeName() << ");" << std::endl;

#endif

        this->upper_code_block.push_back(hash_table.str());

        /* add result creation code*/
        this->create_result_table_code_block
                << "if(!addHashTable(result_table, \"" << attr.getResultAttributeName() << "\", "
                << "createSystemHashTable(" << getHashTableVarName(attr) << "))){"
                << "std::cout << \"Error adding hash table for attribute '"
                << attr.getResultAttributeName() << "' to result!\" << std::endl;"
                << "return TablePtr();"
                << "}"
                << std::endl;

        return true;
    }

    bool CPPCodeGenerator::consumeProbeHashTable_impl(const AttributeReference& hash_table_attr,
            const AttributeReference& probe_attr) {
        /* code for hash table probes */
        std::stringstream hash_probe;
        /* closing brackets and pointer chasing */
        std::stringstream hash_probe_lower;

        this->generated_code << "TID " << getTupleIDVarName(hash_table_attr) << "=0;" << std::endl;
        this->generated_code << "HashTablePtr generic_hashtable_"
                << hash_table_attr.getVersionedAttributeName()
                << "=getHashTable(" << getTableVarName(hash_table_attr) << ", "
                << "\"" << hash_table_attr.getUnversionedAttributeName() << "\");"
                << std::endl;

#ifdef USE_SIMPLE_HASH_TABLE_FOR_JOINS
        this->generated_code << "hashtable_t* hashtable_"
                << hash_table_attr.getVersionedAttributeName()
                << "= (hashtable_t*) getHashTableFromSystemHashTable(generic_hashtable_"
                << hash_table_attr.getVersionedAttributeName() << ");"
                << std::endl;

        hash_probe << "unsigned long hash_"
                << hash_table_attr.getVersionedAttributeName()
                << " = HASH(array_" << probe_attr.getVersionedAttributeName()
                << "[" << getTupleIDVarName(probe_attr) << "]) & hashtable_"
                << hash_table_attr.getVersionedAttributeName() << "->mask;"
                << std::endl;
        hash_probe << "hash_bucket_t *bucket_"
                << hash_table_attr.getVersionedAttributeName()
                << " = &hashtable_"
                << hash_table_attr.getVersionedAttributeName()
                << "->buckets[hash_"
                << hash_table_attr.getVersionedAttributeName() << "];"
                << std::endl;

        hash_probe << "while (bucket_"
                << hash_table_attr.getVersionedAttributeName() << ") {"
                << std::endl;
        hash_probe << "  for (size_t bucket_tid_"
                << hash_table_attr.getVersionedAttributeName() << " = 0; "
                << "bucket_tid_" << hash_table_attr.getVersionedAttributeName()
                << " < bucket_" << hash_table_attr.getVersionedAttributeName() << "->count; "
                << "bucket_tid_" << hash_table_attr.getVersionedAttributeName() << "++) {" << std::endl;
        hash_probe << "     if (bucket_" << hash_table_attr.getVersionedAttributeName()
                << "->tuples[bucket_tid_"
                << hash_table_attr.getVersionedAttributeName()
                << "].key == array_" << probe_attr.getVersionedAttributeName()
                << "[" << getTupleIDVarName(probe_attr) << "]) {" << std::endl;
        hash_probe << "          " << getTupleIDVarName(hash_table_attr) << "="
                << "bucket_" << hash_table_attr.getVersionedAttributeName()
                << "->tuples[bucket_tid_"
                << hash_table_attr.getVersionedAttributeName()
                << "].value;" << std::endl;

        hash_probe_lower << "            }" << std::endl;
        hash_probe_lower << "        }" << std::endl;
        hash_probe_lower << "        bucket_" << hash_table_attr.getVersionedAttributeName() << " = bucket_" << hash_table_attr.getVersionedAttributeName() << "->next;" << std::endl;
        hash_probe_lower << "    }" << std::endl;

#endif

#ifdef USE_GOOGLE_HASH_TABLE_FOR_JOINS
        this->generated_code << "std::tr1::unordered_multimap<TID,TID>* hashtable_"
                << hash_table_attr.getVersionedAttributeName()
                << "= (std::tr1::unordered_multimap<TID,TID>*) getHashTableFromSystemHashTable(generic_hashtable_"
                << hash_table_attr.getVersionedAttributeName() << ");"
                << std::endl;

        this->generated_code << "std::tr1::unordered_multimap<TID,TID>::const_iterator it_"
                << hash_table_attr.getVersionedAttributeName() << ";" << std::endl;

        hash_probe << "std::pair<std::tr1::unordered_multimap<TID,TID>::const_iterator,"
                << "std::tr1::unordered_multimap<TID,TID>::const_iterator> range_"
                << hash_table_attr.getVersionedAttributeName()
                << " = hashtable_" << hash_table_attr.getVersionedAttributeName()
                << "->equal_range(array_" << probe_attr.getVersionedAttributeName()
                << "[" << getTupleIDVarName(probe_attr) << "]);"
                << std::endl;

        hash_probe << "for(it_"
                << hash_table_attr.getVersionedAttributeName()
                << "=range_" << hash_table_attr.getVersionedAttributeName() << ".first;"
                << "it_" << hash_table_attr.getVersionedAttributeName()
                << "!=range_" << hash_table_attr.getVersionedAttributeName() << ".second;"
                << "++it_" << hash_table_attr.getVersionedAttributeName() <<  "){"
                << std::endl;

        hash_probe << getTupleIDVarName(hash_table_attr) << "="
                << "it_" << hash_table_attr.getVersionedAttributeName()
                << "->second;" << std::endl;
        /* close bracked of for loop */
        hash_probe_lower << "}" << std::endl;
        /* delete hash table */
//        this->cleanup_code << "delete hashtable_"
//                << hash_table_attr.getVersionedAttributeName() << ";" << std::endl;

#endif


        /* conceptionally, other code is inserted here */

        this->upper_code_block.push_back(hash_probe.str());
        this->lower_code_block.push_front(hash_probe_lower.str());

        return true;
    }

    const std::string getAggregationPayloadFieldVarName(const AttributeReference& attr_ref, const AggregationFunction& agg_func) {
        std::stringstream ss;
        ss << util::getName(agg_func) << "_OF_"
                << attr_ref.getVersionedAttributeName();
        return ss.str();
    }

    const std::string getAggregationPayloadFieldVarName(const AttributeReference& attr_ref, const AggregationParam& param) {
        return getAggregationPayloadFieldVarName(attr_ref, param.agg_func);
    }

    const std::string getAggregationGroupTIDPayloadFieldCode(const AttributeReference& attr_ref);

    const std::string CPPCodeGenerator::getAggregationCode(const GroupingAttributes& grouping_columns,
            const AggregateSpecifications& aggregation_specs,
            const std::string access_ht_entry_expression) const {
        std::stringstream hash_aggregate;
        for (size_t i = 0; i < aggregation_specs.size(); ++i) {
            AttributeReference attr_ref = aggregation_specs[i].result_attr;
            if (aggregation_specs[i].agg_func == MIN) {
                hash_aggregate << access_ht_entry_expression //"it->second."
                        << getAggregationPayloadFieldVarName(attr_ref, aggregation_specs[i].agg_func)
                        << "= std::min(" << access_ht_entry_expression
                        << getAggregationPayloadFieldVarName(attr_ref, aggregation_specs[i].agg_func)
                        << "," << getElementAccessExpression(aggregation_specs[i].scan_attr) << ";"
                        << std::endl;
            } else if (aggregation_specs[i].agg_func == MAX) {
                hash_aggregate << access_ht_entry_expression //"it->second."
                        << getAggregationPayloadFieldVarName(attr_ref, aggregation_specs[i].agg_func)
                        << "= std::max(" << access_ht_entry_expression
                        << getAggregationPayloadFieldVarName(attr_ref, aggregation_specs[i].agg_func)
                        << "," << getElementAccessExpression(aggregation_specs[i].scan_attr) << ");"
                        //                        << "[" << getTupleIDVarName(aggregation_specs[i].scan_attr) << "]);"
                        << std::endl;
            } else if (aggregation_specs[i].agg_func == COUNT) {
                hash_aggregate << access_ht_entry_expression //"it->second."
                        << getAggregationPayloadFieldVarName(attr_ref, aggregation_specs[i].agg_func) << "++;" << std::endl;
                //                        <<  cit->second.new_column_name <<  "++;" << std::endl;

            } else if (aggregation_specs[i].agg_func == SUM || aggregation_specs[i].agg_func == AVERAGE) {

                hash_aggregate << access_ht_entry_expression //"it->second."
                        << getAggregationPayloadFieldVarName(attr_ref, aggregation_specs[i].agg_func)
                        << "+=" << getElementAccessExpression(aggregation_specs[i].scan_attr) << ";"
                        << std::endl;

                if (aggregation_specs[i].agg_func == AVERAGE) {
                    hash_aggregate << access_ht_entry_expression //"it->second."
                            << getAggregationPayloadFieldVarName(attr_ref, COUNT) << "++;" << std::endl;
                }
            } else {
                COGADB_FATAL_ERROR("Unsupported Aggregation Function: "
                        << util::getName(aggregation_specs[i].agg_func), "");
            }

            for (size_t i = 0; i < grouping_columns.size(); ++i) {

                AttributeReference attr_ref = grouping_columns[i];
                hash_aggregate << access_ht_entry_expression
                        << getGroupTIDVarName(attr_ref) << " = "
                        << getTupleIDVarName(attr_ref) << ";"
                        << std::endl;
            }
        }
        return hash_aggregate.str();
    }

    const std::string getAggregationPayloadFieldCode(const AttributeReference& attr_ref,
            AggregationFunction agg_func) {
        std::stringstream field_code;
        if (agg_func == MIN
                || agg_func == MAX) {
            field_code << toCPPType(attr_ref.getAttributeType()) << " "
                    << getAggregationPayloadFieldVarName(attr_ref, agg_func) << ";" << std::endl;
        } else if (agg_func == COUNT) {
            field_code << "size_t "
                    << getAggregationPayloadFieldVarName(attr_ref, agg_func) << ";" << std::endl;
        } else if (agg_func == SUM || agg_func == AVERAGE) {
            field_code << "double "
                    << getAggregationPayloadFieldVarName(attr_ref, agg_func) << ";" << std::endl;
        } else {
            COGADB_FATAL_ERROR("Unsupported Aggregation Function: "
                    << util::getName(agg_func), "");
        }

        return field_code.str();
    }

    const std::string getAggregationGroupTIDPayloadFieldCode(const AttributeReference& attr_ref) {
        std::stringstream field_code;
        field_code << "TID " << getGroupTIDVarName(attr_ref) << ";" << std::endl;
        return field_code.str();
    }

    const std::string getComputeGroupIDExpression(const GroupingAttributes& grouping_attrs){

        if(grouping_attrs.empty()){
            COGADB_FATAL_ERROR("Invalid argument: at least one grouping attribute"
                    << " must exist to compute the group id!","");
            return std::string();
        }

        size_t bits_per_column[grouping_attrs.size()];
        for(size_t i=0;i<grouping_attrs.size();++i){
              bits_per_column[i]=getNumberOfRequiredBits(grouping_attrs[i]);
        }

        size_t total_number_of_bits = std::accumulate(bits_per_column,bits_per_column+grouping_attrs.size(),0);

        //can we apply our bit packing?
        if(total_number_of_bits>sizeof(uint64_t)*8){
            COGADB_ERROR("Maximum Number of Bits for optimized groupby exceeded: max value: " << sizeof(uint64_t)*8 << " Got: " << total_number_of_bits ,"");
            return std::string();
        }

        /* special case: if we have only one group by attribute, use value as
         * group key */
        if(grouping_attrs.size()==1){
            return getCompressedElementAccessExpression(grouping_attrs.front());
        }

        //we get the number of bits to shift for each column by computing
        //the prefix sum of each columns number of bits
        size_t bits_to_shift[grouping_attrs.size()+1];
        serial_prefixsum(bits_per_column, grouping_attrs.size(), bits_to_shift);

        std::stringstream expr;
        for(size_t i=0;i<grouping_attrs.size();++i){
            expr << "(TID(" << getCompressedElementAccessExpression(grouping_attrs[i])
                 << ") << " <<  bits_to_shift[i] << ")";
            if(i+1<grouping_attrs.size()){
                expr << " | ";
            }
        }

        return expr.str();
    }

    bool isBitpackedGroupbyOptimizationApplicable(const GroupingAttributes& grouping_attrs){
        if(grouping_attrs.empty()) return false;

        size_t bits_per_column[grouping_attrs.size()];
        for(size_t i=0;i<grouping_attrs.size();++i){
              bits_per_column[i]=getNumberOfRequiredBits(grouping_attrs[i]);
        }

        size_t total_number_of_bits = std::accumulate(bits_per_column,bits_per_column+grouping_attrs.size(),0);
//        std::cout << "Number of Required Bits: " << total_number_of_bits << std::endl;
        //can we apply our bit packing?
        if(total_number_of_bits>sizeof(uint64_t)*8){
//            COGADB_ERROR("Maximum Number of Bits for optimized groupby exceeded: max value: " << sizeof(uint64_t)*8 << " Got: " << total_number_of_bits ,"");
            return false;
        }else{
            return true;
        }
    }

    void CPPCodeGenerator::generateCode_BitpackedGroupingKeyComputation(
            const GroupingAttributes& grouping_attrs){
        std::stringstream hash_aggregate;
        hash_aggregate << "TID group_key = "
                << getComputeGroupIDExpression(grouping_attrs) << ";"
                << std::endl;
        this->upper_code_block.push_back(hash_aggregate.str());          
    }

    void CPPCodeGenerator::generateCode_GenericGroupingKeyComputation(
            const GroupingAttributes& grouping_attrs){
        
         generated_code << "typedef std::map<std::string,uint64_t> Dictionary;" << std::endl;
         generated_code << "const size_t number_of_grouping_columns=" << grouping_attrs.size() << ";" << std::endl;
         generated_code << "uint64_t max_group_id=0;" << std::endl;  
         generated_code << "Dictionary group_key_dict;" << std::endl;
         generated_code << "Dictionary::const_iterator group_key_dict_cit;" << std::endl;
         generated_code << "" << std::endl;
         
         std::stringstream compute_group_id;
         compute_group_id << "std::string group;" << std::endl;
         for(size_t col_id = 0; col_id<grouping_attrs.size();++col_id){    
            compute_group_id << "group.append(boost::lexical_cast<std::string>(" 
                             << getCompressedElementAccessExpression(grouping_attrs[col_id]) 
                             << "));" << std::endl;
         }
         compute_group_id << "group_key_dict_cit=group_key_dict.find(group);" << std::endl;
         compute_group_id << "if(group_key_dict_cit==group_key_dict.end()){" << std::endl;
         compute_group_id << "++max_group_id;" << std::endl;
         compute_group_id << "std::pair<Dictionary::const_iterator,bool> ret"
                          << " = group_key_dict.insert(std::make_pair(group,max_group_id));" 
                          << std::endl;
         compute_group_id << "group_key_dict_cit=ret.first;" << std::endl;
         compute_group_id << "}" << std::endl;
         compute_group_id << "TID group_key = group_key_dict_cit->second;" << std::endl;
                
        this->upper_code_block.push_back(compute_group_id.str());         
                
                
                
        
    }
    
    bool CPPCodeGenerator::consumeHashGroupAggregate_impl(const GroupByAggregateParam& groupby_param) {

        const AggregateSpecifications& aggr_specs = groupby_param.aggregation_specs;
        const GroupingAttributes& grouping_attrs = groupby_param.grouping_attrs;

        /* generate struct that serves as payload based on aggregation functions */
        header_and_types_block << "struct AggregationPayload{" << std::endl;

        for (size_t i = 0; i < grouping_attrs.size(); ++i) {
            AttributeReference attr_ref = grouping_attrs[i];
            header_and_types_block << getAggregationGroupTIDPayloadFieldCode(attr_ref)
                    << ";";
        }
        for (size_t i = 0; i < aggr_specs.size(); ++i) {
            AttributeReference computed_attr = aggr_specs[i].result_attr;
            header_and_types_block << getAggregationPayloadFieldCode(computed_attr, aggr_specs[i].agg_func);
        }

        header_and_types_block << "};" << std::endl;

        /* produce global variables for the query */
#ifdef USE_GOOGLE_HASH_TABLE_FOR_HASH_GROUPBY
        generated_code << "typedef google::dense_hash_map<TID," << std::endl
                << "AggregationPayload> AggregationHashTable;"
                << std::endl;
#else
        generated_code << "typedef boost::unordered_map<TID," << std::endl
                << "AggregationPayload," << std::endl
                << "boost::hash<TID>, std::equal_to<TID> > AggregationHashTable;"
                << std::endl;
#endif
        /* create hash table */
        generated_code << "AggregationHashTable aggregation_hash_table;" << std::endl;
//        generated_code << "std::pair<AggregationHashTable::iterator, AggregationHashTable::iterator> range;" << std::endl;
        generated_code << "AggregationHashTable::iterator it;" << std::endl;
#ifdef USE_GOOGLE_HASH_TABLE_FOR_HASH_GROUPBY
        generated_code << "/* Google's hash table requires that " << std::endl
                "we sacrifice one value of the values domain. In return, we get " << std::endl
                "excellent performance. */" << std::endl;
        generated_code << "aggregation_hash_table.set_empty_key(std::numeric_limits<TID>::max());" << std::endl;
#endif

        /* determine the grouping key according to grouping columns*/
        std::stringstream hash_aggregate;

        if(isBitpackedGroupbyOptimizationApplicable(grouping_attrs)){
            generateCode_BitpackedGroupingKeyComputation(grouping_attrs);
        }else{
            generateCode_GenericGroupingKeyComputation(grouping_attrs);
            std::cout << "[INFO]: Cannot use bitpacked grouping, fallback "
                      << "to generic implementation..." << std::endl;
        }

        /* do the usual hash table probe, aggregate using the custom payload for
         * each aggregation function. This performs only one lookup per tuple. */
        hash_aggregate << "it = aggregation_hash_table.find(group_key);" << std::endl;
        hash_aggregate << "if (it != aggregation_hash_table.end()) {" << std::endl;
        hash_aggregate << getAggregationCode(grouping_attrs,
                aggr_specs,
                "it->second.")
                << std::endl;
        hash_aggregate << "} else {" << std::endl;
        hash_aggregate << "AggregationPayload payload;" << std::endl;
        /* init payload fields by using the type default constructor (T()) */
        for (size_t i = 0; i < aggr_specs.size(); ++i) {
            AttributeReference attr_ref = aggr_specs[i].result_attr; // getAttributeReference(cit->first);
            std::string cpp_type = toCPPType(attr_ref.getAttributeType());
            /* make an exception for COUNT, this will always be size_t */
            if (aggr_specs[i].agg_func == COUNT)
                cpp_type = "size_t";
            hash_aggregate << "payload."
                    << getAggregationPayloadFieldVarName(attr_ref, aggr_specs[i].agg_func) << " = "
                    << cpp_type
                    << "();" << std::endl;
        }
        /* insert new key and payload in hash table */
        hash_aggregate << "std::pair<AggregationHashTable::iterator, bool> ret "
                << " = aggregation_hash_table.insert(std::make_pair(group_key, payload));" << std::endl;
//        hash_aggregate << "aggregation_hash_table.insert(std::make_pair(group_key, payload));" << std::endl;
        hash_aggregate << getAggregationCode(grouping_attrs,
                aggr_specs,
                "ret.first->second.")
                << std::endl;
        hash_aggregate << "}" << std::endl;

        this->upper_code_block.push_back(hash_aggregate.str());
        /* write result from hash table to output arrays */
        after_for_loop_block << "if(aggregation_hash_table.size()>=allocated_result_elements){" << std::endl;
        after_for_loop_block << "allocated_result_elements=aggregation_hash_table.size();" << std::endl;
        after_for_loop_block << getCodeReallocResultMemory(param) << std::endl;
//        for (size_t i = 0; i < param.size(); ++i) {
//            AttributeType attr = param[i].getAttributeType();
//            after_for_loop_block << getResultArrayVarName(param[i])
//                    << "= (" << toCPPType(attr) << "*) realloc("
//                    << getResultArrayVarName(param[i])
//                    << ", allocated_result_elements*sizeof(" << toCPPType(attr)
//                    << "));" << std::endl;
//        }
        after_for_loop_block << "}" << std::endl;

        after_for_loop_block << "for (it = aggregation_hash_table.begin(); it != aggregation_hash_table.end(); ++it) {" << std::endl;

        for (size_t i = 0; i < aggr_specs.size(); ++i) {

            after_for_loop_block << "" << getResultArrayVarName(aggr_specs[i].result_attr) << "[current_result_size] = "
                    << "it->second." << getAggregationPayloadFieldVarName(aggr_specs[i].result_attr, aggr_specs[i].agg_func) << ";";
        }

        for (size_t i = 0; i < grouping_attrs.size(); ++i) {
            after_for_loop_block << "" << getResultArrayVarName(grouping_attrs[i])
                    << "[current_result_size] = "
                    << getInputArrayVarName(grouping_attrs[i]) << "["
                    << "it->second." << getGroupTIDVarName(grouping_attrs[i])
                    << "];"
                    << std::endl;
        }

        after_for_loop_block << "current_result_size++;" << std::endl;
        after_for_loop_block << "}" << std::endl;

        return true;
    }

    bool CPPCodeGenerator::consumeAggregate_impl(const AggregateSpecifications& aggr_specs) {

        for (size_t i = 0; i < aggr_specs.size(); ++i) {
            AttributeReference computed_attr = aggr_specs[i].result_attr;
            header_and_types_block << getAggregationPayloadFieldCode(computed_attr, aggr_specs[i].agg_func); // << std::endl;;
            generated_code << getAggregationPayloadFieldVarName(computed_attr, aggr_specs[i].agg_func)
                    << " = " << toCPPType(computed_attr.getAttributeType()) << "(0);" << std::endl;

            std::stringstream compute_expr;
            if (aggr_specs[i].agg_func == COUNT || aggr_specs[i].agg_func == AVERAGE) {
                compute_expr << getAggregationPayloadFieldVarName(computed_attr,
                        aggr_specs[i].agg_func) << "++";
            } else if (aggr_specs[i].agg_func == SUM || aggr_specs[i].agg_func == AVERAGE) {
                compute_expr << getAggregationPayloadFieldVarName(computed_attr,
                        aggr_specs[i].agg_func) << " += "
                        << getElementAccessExpression(aggr_specs[i].scan_attr);
            } else if (aggr_specs[i].agg_func == MIN) {
                compute_expr << getAggregationPayloadFieldVarName(computed_attr,
                        aggr_specs[i].agg_func) << " = std::min("
                        << getAggregationPayloadFieldVarName(computed_attr,
                        aggr_specs[i].agg_func) << ","
                        << getElementAccessExpression(aggr_specs[i].scan_attr) << ")";
            } else if (aggr_specs[i].agg_func == MAX) {
                compute_expr << getAggregationPayloadFieldVarName(computed_attr,
                        aggr_specs[i].agg_func) << " = std::max("
                        << getAggregationPayloadFieldVarName(computed_attr,
                        aggr_specs[i].agg_func) << ","
                        << getElementAccessExpression(aggr_specs[i].scan_attr) << ")";
            } else {
                COGADB_FATAL_ERROR("Unknown Aggregation Function!", "");
            }

            compute_expr << ";";

            this->upper_code_block.push_back(compute_expr.str());
        }
        /* write result to output arrays */
        for (size_t i = 0; i < aggr_specs.size(); ++i) {
            after_for_loop_block << "" << getResultArrayVarName(aggr_specs[i].result_attr) << "[current_result_size] = "
                    << getAggregationPayloadFieldVarName(aggr_specs[i].result_attr, aggr_specs[i].agg_func) << ";";
        }
        after_for_loop_block << "current_result_size++;" << std::endl;

        return true;
    }

    const std::pair<bool, AttributeReference> CPPCodeGenerator::consumeAlgebraComputation_impl(const AttributeReference& left_attr,
            const AttributeReference& right_attr,
            const ColumnAlgebraOperation& alg_op) {

        AttributeReference computed_attr = createComputedAttribute(left_attr, right_attr, alg_op);

        generated_code << "double " << getElementAccessExpression(computed_attr)
                << " = double(0);" << std::endl;

        std::stringstream compute_expr;
        compute_expr << getElementAccessExpression(computed_attr) << " = "
                << getElementAccessExpression(left_attr) //getInputArrayVarName(left_attr) << "[" << getTupleIDVarName(left_attr) << "]"
                << " " << toCPPOperator(alg_op) << " "
                << getElementAccessExpression(right_attr) << ";"; //getInputArrayVarName(right_attr) << "[" << getTupleIDVarName(right_attr) << "];";

        this->upper_code_block.push_back(compute_expr.str());

        return std::make_pair(true, computed_attr);
    }

    const std::pair<bool, AttributeReference> CPPCodeGenerator::consumeAlgebraComputation_impl(const AttributeReference& left_attr,
            const boost::any constant,
            const ColumnAlgebraOperation& alg_op) {

        AttributeReference computed_attr = createComputedAttribute(left_attr, constant, alg_op);
        generated_code << "double " << getElementAccessExpression(computed_attr) << " = double(0);" << std::endl;
        std::stringstream compute_expr;
        compute_expr << getElementAccessExpression(computed_attr) << " = "
                << getElementAccessExpression(left_attr) //getInputArrayVarName(left_attr) << "[" << getTupleIDVarName(left_attr) << "]"
                << " " << toCPPOperator(alg_op) << " "
                << toCPPExpression(constant) << ";"; //getInputArrayVarName(right_attr) << "[" << getTupleIDVarName(right_attr) << "];";

        this->upper_code_block.push_back(compute_expr.str());

        return std::make_pair(true, computed_attr);
    }

    const std::pair<bool, AttributeReference> CPPCodeGenerator::consumeAlgebraComputation_impl(const boost::any constant,
            const AttributeReference& right_attr,
            const ColumnAlgebraOperation& alg_op) {

        AttributeReference computed_attr = createComputedAttribute(constant, right_attr, alg_op);
        generated_code << "double " << getElementAccessExpression(computed_attr) << " = double(0);" << std::endl;

        std::stringstream compute_expr;
        compute_expr << getElementAccessExpression(computed_attr) << " = "
                << toCPPExpression(constant) //getInputArrayVarName(left_attr) << "[" << getTupleIDVarName(left_attr) << "]"
                << " " << toCPPOperator(alg_op) << " "
                << getElementAccessExpression(right_attr) << ";"; //getInputArrayVarName(right_attr) << "[" << getTupleIDVarName(right_attr) << "];";

        this->upper_code_block.push_back(compute_expr.str());

        return std::make_pair(true, computed_attr);
    }

    void CPPCodeGenerator::printCode(std::ostream& out) {

        bool ret = this->produceTuples(this->scanned_attributes);
        assert(ret == true);
        /* all imports and declarations */
        out << this->header_and_types_block.str() << std::endl;
        out << this->fetch_input_code_block.str() << std::endl;
        /* all code for query function definition and input array retrieval */
        out << this->generated_code.str() << std::endl;
        /* reserve memory for each attribute in projection param */
        out << this->getCodeAllocateResultTable() << std::endl;
        /* add for loop and it's contents */
        std::list<std::string>::const_iterator cit;
        for (cit = this->upper_code_block.begin();
                cit != this->upper_code_block.end();
                ++cit) {
            out << *cit << std::endl;
        }
        /* if we do not materialize into a hash table during aggregation,
           write result regularely */
        if (pipe_end != MATERIALIZE_FROM_AGGREGATION_HASH_TABLE_TO_ARRAY) {
            out << this->getCodeWriteResult() << std::endl;
        }
        /* generate closing brackets, pointer chasing, and cleanup operations */
        for (cit = this->lower_code_block.begin();
                cit != this->lower_code_block.end();
                ++cit) {
            out << *cit << std::endl;
        }
        /* if we do materialize into a hash table during aggregation,
           write copy result from hash table to output arrays */
        if (pipe_end == MATERIALIZE_FROM_AGGREGATION_HASH_TABLE_TO_ARRAY) {
            out << after_for_loop_block.str();
        }
        /* generate code that builds the reslt table using the minimal API */
        out << this->createResultTable() << std::endl;
        out << "}" << std::endl;
    }

    const PipelinePtr CPPCodeGenerator::compile() {

        char* error = NULL;
        
        bool show_generated_code = VariableManager::instance().getVariableValueBoolean("show_generated_code");
        bool debug_code_generator = VariableManager::instance().getVariableValueBoolean("debug_code_generator");
        

        ScanParam& param = scanned_attributes;

        assert(this->input_table!=NULL);
        
        if(canOmitCompilation()){
            if(debug_code_generator)
                std::cout << "[Falcon]: Omit compilation of empty pipeline..." << std::endl;
            return PipelinePtr(new DummyPipeline(this->input_table, scanned_attributes));
        }
        
        boost::uuids::uuid uuid = boost::uuids::random_generator()();
        std::stringstream ss;
        ss << "gen_query_" << uuid;

        std::string filename = ss.str() + ".cpp";

        std::ofstream generated_file(filename.c_str(), std::ios::trunc | std::ios::out);
        printCode(generated_file);
        generated_file.close();

        std::string format_command = std::string("astyle -q ") + filename;
        int ret = system(format_command.c_str());

        if(show_generated_code || debug_code_generator){
            std::string cat_command = std::string("cat ") + filename;
            ret = system(cat_command.c_str());
        }

        std::string copy_last_query_command = std::string("cp '") + filename + std::string("' last_generated_query.cpp");
        ret = system(copy_last_query_command.c_str());

        Timestamp begin_compile = getTimestamp();
        std::string path_to_precompiled_header = "minimal_api.hpp.pch";
        std::string path_to_minimal_api_header
                = std::string(PATH_TO_COGADB_SOURCE_CODE)+"/include/query_compilation/minimal_api.hpp";

        bool rebuild_precompiled_header = false;

        if(!boost::filesystem::exists(path_to_precompiled_header)){
            rebuild_precompiled_header = true;
        }else{
            std::time_t last_access_pch = boost::filesystem::last_write_time(path_to_precompiled_header);
            std::time_t last_access_header = boost::filesystem::last_write_time(path_to_minimal_api_header);
            /* pre-compiled header outdated? */
            if(last_access_header>last_access_pch){
                std::cout << "Pre-compiled header '"
                          << path_to_precompiled_header  << "' is outdated!"
                          << std::endl;
                rebuild_precompiled_header = true;
            }
        }

        if(rebuild_precompiled_header){
            std::cout << "Precompiled Header not found! Building Precompiled Header now..." << std::endl;
            std::stringstream precompile_header;

            precompile_header << "clang++ -g -O3 -fpic "
                    << PATH_TO_COGADB_SOURCE_CODE << "/include/query_compilation/minimal_api.hpp -I "
                    << PATH_TO_COGADB_SOURCE_CODE << "/include/  -o minimal_api.hpp.pch" << std::endl;
            ret=system(precompile_header.str().c_str());
            if (ret != 0) {
                std::cout << "Compilation of precompiled header failed!" << std::endl;
                return PipelinePtr();
            } else {
                std::cout << "Compilation of precompiled header successful!" << std::endl;
            }
        }

        std::stringstream compile_command;
        compile_command << "clang++ -Wno-parentheses-equality -g -O3 -include minimal_api.hpp -I "
                        << PATH_TO_COGADB_SOURCE_CODE << "/include/ -I "
                        << PATH_TO_COGADB_SOURCE_CODE << "/../hype-library/include/ -c -fpic ";
        compile_command << filename << " -o " << ss.str() << ".o";
        //        ret=system("clang -g -I ../cogadb/include/ -I ../hype-library/include/ -c -fpic gen_query.cpp -o gen_query.o");
        ret = system(compile_command.str().c_str());
        if (ret != 0) {
            std::cout << "Compilation Failed!" << std::endl;
            return PipelinePtr();
        } else {
            if(debug_code_generator)
                std::cout << "Compilation Successful!" << std::endl;
        }
        std::stringstream linking_command;
        linking_command << "g++ -shared " << ss.str() << ".o -o " << ss.str() << ".so" << std::endl;
        ret = system(linking_command.str().c_str());

        Timestamp end_compile = getTimestamp();

        std::stringstream shared_lib;
        shared_lib << "./" << ss.str() << ".so";
        if(debug_code_generator)
            std::cout << "Loading shared library '" << shared_lib.str() << "'" << std::endl;
        void *myso = dlopen(shared_lib.str().c_str(), RTLD_NOW);
        assert(myso != NULL);
        CompiledQueryPtr query = (CompiledQueryPtr) dlsym(myso, "_Z14compiled_queryRKSt6vectorIN6CoGaDB18AttributeReferenceESaIS1_EE"); //"compiled_query");
        error = dlerror();
        if (error) {
            std::cerr << error << std::endl;
            return PipelinePtr();
        }
        assert(query != NULL);

        if(debug_code_generator){
            std::cout << "Attributes with Hash Tables: " << std::endl;
            for (size_t i = 0; i < param.size(); ++i) {
                std::cout << param[i].getVersionedTableName()
                        << "." << param[i].getVersionedAttributeName()
                        << ": " << param[i].hasHashTable() << std::endl;
            }
        }

        double compile_time_in_sec = double(end_compile - begin_compile) / (1000 * 1000 * 1000);
        return PipelinePtr(new SharedLibPipeline(query, param, compile_time_in_sec, myso, ss.str()));
    }

}; //end namespace CoGaDB
