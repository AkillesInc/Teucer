
#include <core/attribute_reference.hpp>
#include <core/table.hpp>
#include <core/hash_table.hpp>
#include <boost/make_shared.hpp>

#include "core/foreign_key_constraint.hpp"
#include "query_compilation/minimal_api.hpp"
#include "core/data_dictionary.hpp"
#include <util/getname.hpp>

namespace CoGaDB{
        
    std::pair<bool,AttributeType> getAttributeType(const TablePtr table, const std::string& column_name){

        TableSchema::const_iterator cit;
        TableSchema schema = table->getSchema();
        for(cit=schema.begin();cit!=schema.end();++cit){
            if(cit->second==column_name){
                return std::make_pair(true,cit->first);
            }
        }
        return std::make_pair(false,INT);
    }    
    
    
    AttributeReference::AttributeReference(const TablePtr _table,
            const std::string& _input_attribute_name,
            const std::string& _result_attribute_name,
            uint32_t _version_id)
    : table_ptr_(_table), input_attribute_name(_input_attribute_name), type_(),
    result_attribute_name(_result_attribute_name), version_id(_version_id), 
    attr_ref_type(INPUT_ATTRIBUTE)
    {
        assert(table_ptr_!=NULL && "Invalid TablePtr!");
        if(!table_ptr_->hasColumn(input_attribute_name)){
            COGADB_FATAL_ERROR("Column " << input_attribute_name << " not found "
                    << " in table " << table_ptr_->getName() << "!","");
        } 
        /* automatically derive the type of the attribute */
        std::pair<bool,AttributeType> ret=::CoGaDB::getAttributeType(table_ptr_, 
                input_attribute_name);
        assert(ret.first==true);
        type_ = ret.second;
        /* If the user did not specifiy a result name, then the parameter was 
         * default constructed. If it is empty, take input attribute name as 
         * result attribute name. */
        if(_result_attribute_name.empty()){
            result_attribute_name = input_attribute_name;
        }
        
    }
    
    AttributeReference::AttributeReference(const std::string& _input_attribute_name,
            const AttributeType& _type, 
            const std::string& _result_attribute_name,
            uint32_t _version_id)
    : table_ptr_(), input_attribute_name(_input_attribute_name), type_(_type),
    result_attribute_name(_result_attribute_name), version_id(_version_id), 
    attr_ref_type(COMPUTED_ATTRIBUTE)    
    {
        
    }     

    AttributeReference::AttributeReference(const AttributeReference& other)
    : table_ptr_(other.table_ptr_),
    input_attribute_name(other.input_attribute_name),
    type_(other.type_),
    result_attribute_name(other.result_attribute_name),
    version_id(other.version_id),
    attr_ref_type(other.attr_ref_type)
    {
    }

    AttributeReference& AttributeReference::operator=(const AttributeReference& other) {
        if (this != &other) // protect against invalid self-assignment
        {
            table_ptr_ = other.table_ptr_;
            input_attribute_name = other.input_attribute_name;
            type_ = other.type_;
            result_attribute_name = other.result_attribute_name;
            version_id = other.version_id;
            attr_ref_type = other.attr_ref_type;
        }
    }

    const std::string AttributeReference::getUnversionedTableName() const {
        if(attr_ref_type==INPUT_ATTRIBUTE){
            return table_ptr_->getName();
        }else{
            return std::string("");
        }
    }

    const std::string AttributeReference::getVersionedTableName() const {
        std::stringstream ss;
        ss << getUnversionedTableName() << version_id;
        return ss.str();
    }

    const std::string AttributeReference::getUnversionedAttributeName() const {
        return input_attribute_name;
    }

    const std::string AttributeReference::getVersionedAttributeName() const {
//        if(attr_ref_type==INPUT_ATTRIBUTE){        
            std::stringstream ss;
            ss << input_attribute_name;
            if(attr_ref_type==INPUT_ATTRIBUTE) 
                ss << version_id;
            return ss.str();
//        }else{
//            return std::string("");
//        }
    }

    const std::string AttributeReference::getResultAttributeName() const throw () {
        return result_attribute_name;
    }

    AttributeType AttributeReference::getAttributeType() const throw(){
        return type_;
    }    
    
    AttributeReferenceType AttributeReference::getAttributeReferenceType() const throw(){
        return attr_ref_type;
    }    
    
    const TablePtr AttributeReference::getTable() const{
        return table_ptr_;
    }

    const ColumnPtr AttributeReference::getColumn() {
        return table_ptr_->getColumnbyName(getUnversionedAttributeName());
    }

    const HashTablePtr AttributeReference::getHashTable() const{    
//        return hash_table;
        return table_ptr_->getHashTablebyName(this->input_attribute_name);
    }
    
    bool AttributeReference::hasHashTable() const{
        return (table_ptr_->getHashTablebyName(this->input_attribute_name)!=NULL);
    }
    
    uint32_t AttributeReference::getVersion() const{
        return version_id;
    }

    const AttributeReferencePtr createInputAttributeForNewTable(const AttributeReference& attr, 
        const TablePtr table){
        
        if(!table) return AttributeReferencePtr();
        
        return boost::make_shared<AttributeReference>(table, 
                attr.getUnversionedAttributeName(),
                attr.getResultAttributeName(),
                attr.getVersion());
    }
    
    const AttributeReferencePtr createInputAttribute(const TablePtr table,
            const std::string& input_attribute_name,
            const std::string& result_attribute_name,
            const uint32_t version_id){
        if(result_attribute_name.empty()){
            return boost::make_shared<AttributeReference>(table, 
                    input_attribute_name,
                    input_attribute_name,
                    version_id); 
        }else{
            return boost::make_shared<AttributeReference>(table, 
                    input_attribute_name,
                    result_attribute_name,
                    version_id);
        }
    }
    
    const AttributeReferencePtr createComputedAttribute(){
        return AttributeReferencePtr();
    }
    
    bool getColumnProperties(const TablePtr table, 
            const std::string& column_name, 
            ColumnProperties& ret_col_props){
        if(table){
            std::vector<ColumnProperties> col_props = table->getPropertiesOfColumns();
            for(size_t i=0;i<col_props.size();++i){
                if(column_name==col_props[i].name){
                    ret_col_props=col_props[i];
                    return true;
                }
            }
        }
        return false;
    }    
    
    bool getColumnProperties(const AttributeReference& attr, ColumnProperties& ret_col_props){
        return getColumnProperties(attr.getTable(), 
                attr.getUnversionedAttributeName(), 
                ret_col_props);
    }
    
    const AttributeReferencePtr getAttributeReference(const std::string& column_name, 
            uint32_t version){
        if(DataDictionary::instance().existColumn(column_name)){
            std::list<std::pair<ColumnPtr,TablePtr> > cols = DataDictionary::instance().getColumnsforColumnName(column_name);
            if(cols.size()>1){
                COGADB_FATAL_ERROR("Ambiguous column name detected: '" 
                        << column_name << "'","");
                return AttributeReferencePtr();
            }else{
                TablePtr table = cols.front().second;
                return createInputAttribute(table, column_name, column_name, version);
            }
        }else{
            return AttributeReferencePtr();
        }
    }    
    
    bool isComputed(const AttributeReference& attr){
        return (attr.getAttributeReferenceType()==COMPUTED_ATTRIBUTE);
    }
    bool isInputAttribute(const AttributeReference& attr){
        return (attr.getAttributeReferenceType()==INPUT_ATTRIBUTE);
    }
    
    AttributeType getAttributeType(const AttributeReference& attr){
        return attr.getAttributeType();
    }
    
    ColumnType getColumnType(const AttributeReference& attr){
        ColumnProperties col_props;
        if(!getColumnProperties(attr, col_props)){
            assert(isComputed(attr)==true);
            if(isComputed(attr)){
                if(getAttributeType(attr)==VARCHAR){
                    return DICTIONARY_COMPRESSED;
                }else{
                    return PLAIN_MATERIALIZED;
                }
            }else{
                COGADB_FATAL_ERROR("Cannot determine column type!","");
            }
        }
        return col_props.column_type;
    }

    bool isPrimaryKey(const AttributeReference& attr){
        if(attr.getTable()){
            return attr.getTable()->hasPrimaryKeyConstraint(attr.getUnversionedAttributeName());
        }
        return false;
    }
    bool isForeignKey(const AttributeReference& attr){
        if(attr.getTable()){
            return attr.getTable()->hasForeignKeyConstraint(attr.getUnversionedAttributeName());
        }
        return false;
    }
    const AttributeReferencePtr getForeignKeyAttribute(const AttributeReference& attr){
        if(isForeignKey(attr)){
            assert(attr.getTable()!=NULL);
            const ForeignKeyConstraint* foreign_key_constr = attr.getTable()->getForeignKeyConstraint(attr.getUnversionedAttributeName());
            TablePtr foreign_key_table = getTablebyName(foreign_key_constr->getNameOfForeignKeyTable());
            assert(foreign_key_table!=NULL);
            
            return boost::make_shared<AttributeReference>(foreign_key_table, 
                    foreign_key_constr->getNameOfForeignKeyColumn(), 
                    foreign_key_constr->getNameOfForeignKeyColumn(),
                    attr.getVersion());
        }else{
            return AttributeReferencePtr();
        }
    } 
    
    bool areStatisticsUpToDate(const AttributeReference& attr){
        ColumnProperties col_props;
        if(!getColumnProperties(attr, col_props)){
            return false;
        }       
        return col_props.statistics_up_to_date;
    }
    
    size_t getNumberOfRequiredBits(const AttributeReference& attr){

        if(!isComputed(attr)){
//            std::cout << util::getName(attr.getTable()->getColumnbyName(attr.getUnversionedAttributeName())->getColumnType()) << std::endl;
//            std::cout << "getNumberOfRequiredBits(" << toString(attr) << ")=" 
//                    << attr.getTable()->getColumnbyName(attr.getUnversionedAttributeName())->getNumberOfRequiredBits() << std::endl; 
            if(getColumnType(attr)==DICTIONARY_COMPRESSED){
               return 32;
            }
            return attr.getTable()->getColumnbyName(attr.getUnversionedAttributeName())->getNumberOfRequiredBits();
        }else{
            /* check common types of computed attributes */
            if(attr.getAttributeType()==DOUBLE){
                return sizeof(double)*8;
            }else if(attr.getAttributeType()==FLOAT){
                return sizeof(float)*8;
            }else{
                /* we do not know what this could be, wset to 65 to flag that we 
                 cannot apply bitbpacking for group by for this attribute */
                return 65;
            }
        }
    }
    
    bool isSortedAscending(const AttributeReference& attr){
        ColumnProperties col_props;
        if(!getColumnProperties(attr, col_props)){
            return false;
        }       
        return col_props.is_sorted_ascending;
    }
    
    bool isSortedDescending(const AttributeReference& attr){
        ColumnProperties col_props;
        if(!getColumnProperties(attr, col_props)){
            return false;
        }       
        return col_props.is_sorted_descending;
    }
    
    bool isDenseValueArrayStartingWithZero(const AttributeReference& attr){
        ColumnProperties col_props;
        if(!getColumnProperties(attr, col_props)){
            return false;
        }       
        return col_props.is_dense_value_array_starting_with_zero;
    }
    
    const std::string toString(const AttributeReference& attr){
        std::stringstream ss;
        if(isInputAttribute(attr)){
            ss << attr.getVersionedTableName() << "." << attr.getVersionedAttributeName();      
        }else{
            ss << "TEMPORARY." << attr.getVersionedAttributeName();      
        }
        return ss.str();
    }
    
}; //end namespace CoGaDB
