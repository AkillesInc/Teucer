
#include <iostream>
#include <util/types.hpp>
#include <vector>
#include <sstream>
#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>


namespace CoGaDB{

    AttributeType getAttributeType(const std::type_info& type){
        if(type==typeid(int)){
            return INT;
        } else if (type == typeid(TID)) {
            return OID;
        } else if (type == typeid(uint64_t)) {
            return OID;
        } else if (type == typeid(uint32_t)) {
            return UINT32;
        }else if(type==typeid(float)){
            return FLOAT; 
        }else if(type==typeid(double)){
            return DOUBLE; 
        }else if(type==typeid(char)){
            return CHAR;
        }else if(type==typeid(std::string)){
            return VARCHAR;
        }else if(type==typeid(bool)){
            return BOOLEAN;     
        }else{
            COGADB_FATAL_ERROR("INVALID TYPE!","");
            return INT;
        }
    }  

    bool convertStringToInternalDateType(const std::string& value, uint32_t& result){
                    std::vector<std::string> strs;
                    boost::split(strs, value, boost::is_any_of("-"));
                    if(strs.size()!=3){
                        return false;
                    }
                    //we encode a date as integer of the form <year><month><day>
                    //e.g., the date '1998-01-05' will be encoded as integer 19980105
                    uint32_t res_value = boost::lexical_cast<uint32_t>(strs[2]);
                    res_value+= boost::lexical_cast<uint32_t>(strs[1])*100;
                    res_value+= boost::lexical_cast<uint32_t>(strs[0])*100*100;
                    result=res_value;
                    return true;
    }

    bool convertInternalDateTypeToString(const uint32_t& value, std::string& result){

        uint32_t year = value/(100*100);
        uint32_t month = (value/100)%100;
        uint32_t day = value%100;

        //TODO: check date!

        std::stringstream ss;
        ss << year << "-";
        if(month<10)
                ss << 0;
        ss <<  month << "-";
        if(day<10)
                ss << 0;
        ss << day;
        result=ss.str();
        return true;
    }
    
}; //end namespace CogaDB
