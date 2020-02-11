
//#include <persistance/storage_manager.hpp>
#include <persistence/storage_manager.hpp>
#include <core/runtime_configuration.hpp>
#include <core/variable_manager.hpp>
#include <lookup_table/join_index.hpp>
#include <util/filesystem.hpp>
#include <util/time_measurement.hpp>
#include <util/getname.hpp>

#include <fstream>
#include <iostream>

//serialization
#include <boost/archive/binary_oarchive.hpp>
#include <boost/archive/binary_iarchive.hpp>
#include <boost/serialization/binary_object.hpp>
#include <boost/serialization/vector.hpp>
#include <boost/serialization/list.hpp>
#include <boost/serialization/map.hpp>
#include <boost/filesystem.hpp>

#include <boost/lexical_cast.hpp>
#include <boost/utility.hpp>
#include <boost/algorithm/string.hpp>

namespace CoGaDB {

    using namespace std;

    const TablePtr loadTable(const std::string& table_name){
        return loadTable(table_name, RuntimeConfiguration::instance().getPathToDatabase());
    }    
    
    const TablePtr loadTable(const std::string& table_name, const std::string& path_to_database) {

        string path=path_to_database;
        if(path[path.size()-1]!='/'){
            path.append("/");
        }        
        if (!quiet && verbose && debug) cout << "Load table '" << table_name << "' ..." << endl;
        if (!quiet && verbose && debug) cout << "Searching for tables..." << endl;
        std::vector<std::string> v = getFilesinDirectory(path);
        for (unsigned int i = 0; i < v.size(); i++) {
            if (!quiet && verbose && debug) cout << "Found: " << v[i] << endl;
            if (table_name == v[i]) {
                path += "/";
                path += table_name;
                break;
            }
            if (i == v.size() - 1) {
                cout << "Could not find Table: '" << table_name << "' Aborting..." << endl;
                exit(-1);
            }
        }

        Timestamp begin = getTimestamp();
        //load table schema

        if (!quiet && verbose && debug) cout << "Loading Table Schema '" << table_name << "' from file '" << path << "'..." << endl;
        //string path("data/");
        //		path += "/";
        //		path += name_;

        //		TableSchema schema;
        //		CompressionSpecifications compress_spec;                
        //                 
        //		//cout << "Opening File '" << path << "'..." << endl;
        //		ifstream infile (path.c_str(),std::ios_base::binary | std::ios_base::in);
        //		boost::archive::binary_iarchive ia(infile);
        //		ia >> schema;
        //                ia >> compress_spec;
        //		infile.close();
        //
        //
        //		TablePtr tab(new Table(table_name,schema,compress_spec)); 
        bool error_occured = false;
        TablePtr tab(new Table(table_name, path_to_database, error_occured));
        assert(error_occured == false);
        tab->load(RuntimeConfiguration::instance().getTableLoaderMode());

        //tab->load();

        Timestamp end = getTimestamp();
        assert(end >= begin);
        cout << "Needed " << end - begin << "ns (" << double(end - begin) / (1000 * 1000 * 1000) << "s) to load Table '" << table_name << "' ..." << endl;
        return tab;

    }

    bool storeTable(const TablePtr tab) {

        using namespace boost::filesystem;
        string dir_path(RuntimeConfiguration::instance().getPathToDatabase());
        //if ( !exists( dir_path ) ) return false;
        //create directory if is does not exist
        if (!exists(dir_path))
            create_directory(dir_path);

        //create tables directory in database directory if is does not exist
        string table_dir_path = dir_path + "/tables/";
        if (!exists(table_dir_path))
            create_directory(table_dir_path);

        //store schema
        TableSchema schema = tab->getSchema();
        string table_name = tab->getName();

        // store additional compression specifications
        //                CompressionSpecifications compress_spec = tab->getCompressionSpecifications();

        //		string path(RuntimeConfiguration::instance().getPathToDatabase());
        //		path += "/";
        //		path += table_name;
        //		cout << "Storing Table schema of Table '" << table_name << "' in File '" << path << "' ..." << endl;
        //		ofstream outfile (path.c_str(),std::ios_base::binary | std::ios_base::out);
        //		boost::archive::binary_oarchive oa(outfile);
        //
        //		oa << schema;
        //                oa << compress_spec;
        //
        //		outfile.flush();
        //		outfile.close();             

        //store Table data
        bool error_code = tab->store(RuntimeConfiguration::instance().getPathToDatabase());
        //                if(error_code==true){
        //                    addToGlobalTableList();
        //                }
        return error_code;
        //		if(error_code==false){
        //                    return false;
        //                }


        //return true;
    }

    bool loadColumnFromDisk(ColumnPtr col, const std::string& table_name) {


        using namespace boost::filesystem;
        string dir_path(RuntimeConfiguration::instance().getPathToDatabase());
        if (!exists(dir_path)) return false;
        dir_path += "/tables/";
        dir_path += table_name;
        if (!exists(dir_path)) {
            cout << "No directory '" << dir_path << "' Aborting..." << endl;
            return false;
        }
        //		dir_path+="/";
        //		dir_path+=col->getName();
        //std::cout << "Loading Column: " << col->getName() << " from '" << dir_path << "'" << std::endl;
        //std::cout << "Column Type: " << col->getColumnType() << std::endl;
        //std::cout << "Attribute Type: " << util::getName(col->getType()) << std::endl;
        col->load(dir_path);
        col->setStatusLoadedInMainMemory(true);
        //std::cout << "Size in Bytes: " << col->getSizeinBytes() << std::endl;

        return true;
    }

    bool loadColumnFromDisk(ColumnPtr col) {
        if (!col) return false;
        if (col->isLoadedInMainMemory()) return true;
        std::vector<TablePtr>& tables = getGlobalTableList();
        for (unsigned int i = 0; i < tables.size(); ++i) {
            if (tables[i]->hasColumn(col->getName())) {
                loadColumnFromDisk(col, tables[i]->getName());
            }
        }
        return true;
    }

    bool loadColumnsInMainMemory(const std::vector<ColumnPtr>& columns, const std::string& table_name) {

        for (unsigned int i = 0; i < columns.size(); ++i) {
            if (!columns[i]) continue;
            if (!columns[i]->isLoadedInMainMemory()) {
                if (!loadColumnFromDisk(columns[i], table_name)) return false;
            }
        }

        return true;
    }

    bool loadColumnsInMainMemory(const std::vector<ColumnPtr>& columns) {

        for (unsigned int i = 0; i < columns.size(); ++i) {
            if (!columns[i]) continue;
            if (!columns[i]->isLoadedInMainMemory()) {
                if (!loadColumnFromDisk(columns[i])) return false;
            }
//            if(columns[i]->getMemoryID()!=hype::PD_Memory_0){
//                ColumnPtr tmp = copy(columns[i], hype::PD_Memory_0);
//                if(!tmp) return false;
//                ColumnPtr t = columns[i];
//                
//            }
        }

        return true;
    }

    bool loadTablesFromDirectory(const std::string& path_to_database, std::ostream& out, bool quiet){
        vector<TablePtr>& tables = getGlobalTableList();
        string path=path_to_database;
        if(path[path.size()-1]!='/'){
            path.append("/");
        }
        if(!quiet)
            out << "Searching for tables..." << endl;
        std::vector<std::string> v = getFilesinDirectory(path);
        for (size_t i = 0; i < v.size(); i++) {
            if (is_regular_file(path + v[i])) {
                if(!quiet)
                    out << "Loading table '" << v[i] << "' ..." << endl;
                const TablePtr tab = loadTable(v[i], path);
                assert(tab!=NULL);
                //ambiguous table name?
                for(size_t j=0;j<tables.size();++j){
                    if(tables[j]->getName()==tab->getName()){
                        COGADB_FATAL_ERROR("Cannot load table '" << tab->getName() << "'"
                                << ": Table with same name already exists! ","");
                    }
                }
                tables.push_back(tab);
            }
        }
        if(!quiet){
            out << "Tables:" << endl;
            for (size_t i = 0; i < tables.size(); i++) {
                out << tables[i]->getName() << endl;
            }
        }
        return true;
    }    
    
    bool loadTables(ClientPtr client) {
        return loadTablesFromDirectory(RuntimeConfiguration::instance().getPathToDatabase(),
                client->getOutputStream(), false);
    }

    std::vector<TablePtr>& getGlobalTableList() {
        static std::vector<TablePtr> tables;
        return tables;
    }
    
    const TablePtr getTablebyName(const std::string& name) {

        const vector<TablePtr>& tables = getGlobalTableList();
        vector<TablePtr>::const_iterator it;
        for (it = tables.begin(); it != tables.end(); it++) {
            if ((*it)->getName() == name) {
                return *it;
            }
        }
        
        typedef TablePtr (*SystemTableGenerator)();
        typedef std::map<std::string,SystemTableGenerator> SystemTables;
        
        SystemTables sys_tabs;
        sys_tabs.insert(std::make_pair(std::string("SYS_DATABASE_SCHEMA"), &getSystemTableDatabaseSchema));
        sys_tabs.insert(std::make_pair(std::string("SYS_JOIN_INDEXES"), &getSystemTableJoinIndexes));
        sys_tabs.insert(std::make_pair(std::string("SYS_VARIABLES"), &getSystemTableVariables));
        
        
        SystemTables::const_iterator cit = sys_tabs.find(name);
        if(cit!=sys_tabs.end()){
            TablePtr system_table = cit->second();
            return system_table;
        }
        
        //cout << "Table '" << name << "' not found..." << endl;
        return TablePtr();

    }

    bool addToGlobalTableList(TablePtr new_table) {
        if (!new_table) return false;
        TablePtr tab = getTablebyName(new_table->getName());
        //if table with the same name exist, do nothing
        if (tab) return false;
        //add to global table list
        getGlobalTableList().push_back(new_table);
        return true;

    }

    std::set<std::string> getColumnNamesOfSystemTables(){
        
        static std::set<std::string> column_names_of_sys_column;
        static bool initialized=false;
        
        
        
        if(!initialized){
            std::vector<std::string> system_tables;
            system_tables.push_back("SYS_DATABASE_SCHEMA");
            system_tables.push_back("SYS_JOIN_INDEXES");
            system_tables.push_back("SYS_VARIABLES");
            for(size_t i=0;i<system_tables.size();++i){
                TablePtr tab = getTablebyName(system_tables[i]);
                assert(tab!=NULL);
                TableSchema s = tab->getSchema();
                TableSchema::iterator it;
                for(it=s.begin();it!=s.end();++it){
                    column_names_of_sys_column.insert(it->second);
                }
            }
            initialized=true;
        }
        
        return column_names_of_sys_column;
    }
    
    TablePtr getSystemTableDatabaseSchema(){
        
        std::vector<TablePtr>& tables = getGlobalTableList();
        //out << "Tables in database '" << RuntimeConfiguration::instance().getPathToDatabase() << "':" << endl;

            TableSchema result_schema;
            result_schema.push_back(Attribut(VARCHAR,"TABLE_NAME"));
            result_schema.push_back(Attribut(VARCHAR,"COLUMN_NAME"));
            result_schema.push_back(Attribut(VARCHAR,"TYPE"));
            result_schema.push_back(Attribut(VARCHAR,"COMPRESSION_METHOD"));
            result_schema.push_back(Attribut(VARCHAR,"ROWS"));
            result_schema.push_back(Attribut(INT,"IN_MEMORY"));
            result_schema.push_back(Attribut(VARCHAR,"MAIN_MEMORY_FOOTPRINT_IN_BYTES"));

            TablePtr result_tab(new Table("SYS_DATABASE_SCHEMA",result_schema));
        
        for (size_t i = 0; i < tables.size(); i++) {
            std::vector<ColumnProperties> col_props=tables[i]->getPropertiesOfColumns();
            for (size_t j = 0; j < col_props.size(); j++) {
                Tuple t;
                t.push_back(tables[i]->getName());
                t.push_back(col_props[j].name);
                t.push_back(util::getName(col_props[j].attribute_type));
                t.push_back(util::getName(col_props[j].column_type));
                t.push_back(boost::lexical_cast<std::string>(col_props[j].number_of_rows));
                t.push_back((int)col_props[j].is_in_main_memory);
                t.push_back(boost::lexical_cast<std::string>(col_props[j].size_in_main_memory));
                result_tab->insert(t);
            }
        }
            
            
        
        return result_tab;
        
    }
    


    bool storeTableAsSelfContainedCSV(const TablePtr table, 
            const std::string& path_to_dir, 
            const std::string& file_name){
        if(!table){
            return false;
        }
        if(!boost::filesystem::exists(path_to_dir)){
            if(!boost::filesystem::create_directory(path_to_dir)){
                COGADB_ERROR("Path '" <<  path_to_dir <<"' does not exist "
                        << "and could not be created!","");
                return false;
            }
        }
        std::ofstream file;
        std::string path_to_file=path_to_dir;
        path_to_file.append("/").append(file_name);
        file.open(path_to_file.c_str(), std::ofstream::out | std::ofstream::trunc);
        if(!file.is_open()){
            COGADB_ERROR("Could not open file '" << file_name 
                    << "'for writing!","");
        }
        
        std::stringstream header;
        header << "#COGADB_CSV_TABLE\t";
        TableSchema schema = table->getSchema();
        TableSchema::const_iterator cit;
        for(cit=schema.begin();cit!=schema.end();++cit){
            header << cit->second << ":" << util::getName(cit->first);
            if (boost::next(cit) != schema.end()) {
                header << "\t";
            }
        }
        header << std::endl;

        std::string data_rows = table->toString("csv", false);
        file << header.str();
        file << data_rows;
        file.close();
        
        if(file.good()){
            return true;
        }else{
            return false;
        }
    }
    
    bool convertStringToAttributeType(const std::string& attribute_type_str, 
            AttributeType& attribute_type){
        if(attribute_type_str==util::getName(INT)){
            attribute_type = INT;
        }else if(attribute_type_str==util::getName(UINT32)){
            attribute_type = UINT32;
        }else if(attribute_type_str==util::getName(OID)){
            attribute_type = OID;
        }else if(attribute_type_str==util::getName(FLOAT)){
            attribute_type = FLOAT;
        }else if(attribute_type_str==util::getName(DOUBLE)){
            attribute_type = DOUBLE;  
        }else if(attribute_type_str==util::getName(CHAR)){
            attribute_type = CHAR;
        }else if(attribute_type_str==util::getName(VARCHAR)){
            attribute_type = VARCHAR;
        }else if(attribute_type_str==util::getName(DATE)){
            attribute_type = DATE;
        }else if(attribute_type_str==util::getName(BOOLEAN)){
            attribute_type = BOOLEAN;
        }else{
            return false;
        }
        return true;
    }
    
    const TablePtr loadTableFromSelfContainedCSV(const std::string& path_to_file){
        
        if(!boost::filesystem::exists(path_to_file)){
            COGADB_ERROR("Could not load table from csv: file '"
                    << path_to_file << "' not found!","");
            return TablePtr();
        }
        
        std::ifstream file;
        
        file.open(path_to_file.c_str(), std::ifstream::in);
        
        if(!file.is_open()){
            COGADB_ERROR("Could not load table from csv: failed to open file '"
                    << path_to_file << "'!","");
            return TablePtr();
        }
        
        std::string line;
        getline(file, line, '\n');
        
        vector<string> strs;
        boost::split(strs,line,boost::is_any_of("\t"));
        
        if(!strs.empty()){
            if(strs.front()=="#COGADB_CSV_TABLE"){
                TableSchema schema;
                std::string table_name;
                for(size_t i=1;i<strs.size();++i){
                    vector<string> tokens;
                    boost::split(tokens,strs[i],boost::is_any_of(":"));
                    assert(tokens.size()==2);
                    std::string column_name = tokens[0];
                    std::string attribute_type_str = tokens[1];
                    
                    AttributeType attribute_type;
                    if(!convertStringToAttributeType(attribute_type_str, 
                            attribute_type)){
                        COGADB_ERROR("'" << attribute_type_str << "' is not "
                                << "a valid attribute type name!","");
                        return TablePtr();
                    }
                    schema.push_back(std::make_pair(attribute_type, column_name));
                }   
                TablePtr table(new Table(table_name, schema));
                if(!table->loadDatafromFile(path_to_file)){
                        COGADB_ERROR("Failed to load data from file '" 
                                << path_to_file << "'!","");
                    return TablePtr();
                }
                return table;
            }else{
                COGADB_ERROR("File '" << path_to_file << "'is not a CoGaDB " 
                        << "self contained csv file!","");
                return TablePtr();
            }
        }else{
            COGADB_ERROR("File '" << path_to_file << "'is not a CoGaDB " 
                    << "self contained csv file!","");
            return TablePtr(); 
        }
    }
    

}; //end namespace CogaDB


