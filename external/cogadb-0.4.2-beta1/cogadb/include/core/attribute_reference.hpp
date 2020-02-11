/* 
 * File:   attribute_reference.hpp
 * Author: sebastian
 *
 * Created on 26. Juli 2015, 19:14
 */

#ifndef ATTRIBUTE_REFERENCE_HPP
#define	ATTRIBUTE_REFERENCE_HPP

#include <stdint.h>
#include <string>
#include <vector>
#include <boost/shared_ptr.hpp>

#include <core/global_definitions.hpp>

namespace CoGaDB {

    class ColumnBase;
    typedef boost::shared_ptr<ColumnBase> ColumnPtr;
    
    class BaseTable;
    typedef boost::shared_ptr<BaseTable> TablePtr;
    
    class HashTable;
    typedef boost::shared_ptr<HashTable> HashTablePtr;

    enum AttributeReferenceType{
        INPUT_ATTRIBUTE,
        COMPUTED_ATTRIBUTE
    };
    
    class AttributeReference {
    public:

        AttributeReference(const TablePtr _table,
                const std::string& _input_attribute_name,
                const std::string& _result_attribute_name = std::string(),
                uint32_t _version_id = 1);
        
        AttributeReference(const std::string& _input_attribute_name,
                const AttributeType& _type, 
                const std::string& _result_attribute_name,
                uint32_t _version_id = 1);        

        AttributeReference(const AttributeReference&);
        AttributeReference& operator=(const AttributeReference&);

        const std::string getUnversionedTableName() const;
        const std::string getVersionedTableName() const;
        const std::string getUnversionedAttributeName() const;
        const std::string getVersionedAttributeName() const;
        const std::string getResultAttributeName() const throw();

        AttributeType getAttributeType() const throw();
        AttributeReferenceType getAttributeReferenceType() const throw();
        
        const TablePtr getTable() const;
        const ColumnPtr getColumn();
        const HashTablePtr getHashTable() const;        
        bool hasHashTable() const;
        
        uint32_t getVersion() const;
//        void setHashTable(HashTablePtr);

    private:
        //std::string table_name;
        TablePtr table_ptr_;
        std::string input_attribute_name;
        AttributeType type_;
        /* When we query a table or an attribute multiple times, we need to 
         know which to read/write. */
        uint32_t version_id;
        /* the name of the output column produced from this column */
        std::string result_attribute_name;
        /* input attribute or computed attribute? */
        AttributeReferenceType attr_ref_type;
    };
    
    typedef boost::shared_ptr<AttributeReference> AttributeReferencePtr;
    
    const AttributeReferencePtr createInputAttributeForNewTable(const AttributeReference& attr, 
            const TablePtr table);
    
    const AttributeReferencePtr createInputAttribute(const TablePtr table,
            const std::string& input_attribute_name,
            const std::string& result_attribute_name = std::string(),
            const uint32_t version_id = 1);    
    

    const AttributeReferencePtr getAttributeReference(const std::string& column_name, 
            uint32_t version=1);
    
    bool isComputed(const AttributeReference&);
    bool isInputAttribute(const AttributeReference&);
    AttributeType getAttributeType(const AttributeReference&);
    ColumnType getColumnType(const AttributeReference&);
//    size_t getNumberOfRows(const AttributeReference&);
    bool isPrimaryKey(const AttributeReference&);
    bool isForeignKey(const AttributeReference&);
    const AttributeReferencePtr getForeignKeyAttribute(const AttributeReference&);
    
    bool areStatisticsUpToDate(const AttributeReference&);
    size_t getNumberOfRequiredBits(const AttributeReference&);
    bool isSortedAscending();
    bool isSortedDescending(const AttributeReference&);
    bool isDenseValueArrayStartingWithZero(const AttributeReference&);
    const std::string toString(const AttributeReference&);
    
    
    typedef std::vector<AttributeReference> ProjectionParam;
    typedef std::vector<AttributeReference> ScanParam;  

}; //end namespace CoGaDB

#endif	/* ATTRIBUTE_REFERENCE_HPP */

