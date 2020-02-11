//#include <core/table.hpp>
#include <iostream>
#include <sql/server/sql_driver.hpp>
#include <sql/server/sql_parsetree.hpp>
#include <parser/commandline_interpreter.hpp>

#include <vector>
#include <fstream>

#include <dirent.h>
#include <sys/types.h>
#include <sys/stat.h>

#include <gtest/gtest.h>
#include <util/filesystem.hpp>
#include <core/variable_manager.hpp>
#include <util/tests.hpp>

namespace CoGaDB {
        
    const std::string TESTDATA_PATH = std::string(PATH_TO_COGADB_EXECUTABLE) +
                                        "/test/testdata/regression test/";
    
    class GenericRegressionTest : public testing::TestWithParam<std::pair<hype::DeviceTypeConstraint,std::string> > {
               
    public:
        static void SetUpTestCase() {   
        
            loadDatabase();
            /* depending on join order, aggregation results can slighly change,
             * so we need to ensure that we use always the same optimizer pipeline */
            RuntimeConfiguration::instance().setOptimizer("default_optimizer");
        }
        
        static void TearDownTestCase(){
            std::vector<TablePtr>& globalTables = CoGaDB::getGlobalTableList();
            globalTables.clear();
            
        }
        
        TablePtr createTableFromFile(const std::string& deviceDir,const std::string&);
        TablePtr assembleTableFromQuery(const std::string& deviceDir,const std::string&);
        std::string getFileContent(const std::string&);
        
    private:
        const static ClientPtr client;
        
        static void loadDatabase() {
            
            loadReferenceDatabaseStarSchemaScaleFactor1(client);
                        
        }
    };
    
    const ClientPtr GenericRegressionTest::client = ClientPtr(new LocalClient());
        
    const std::vector<std::pair<hype::DeviceTypeConstraint,std::string> > readTestcasesFromDir(hype::DeviceTypeConstraint device,
            const std::string& dirname) {
        
        std::vector<std::pair<hype::DeviceTypeConstraint,std::string> > files;
        /* execute tests for GPU only when we have compiled with GPU support and 
         * we found at least one GPU in the machine 
         */
        if(device == hype::GPU_ONLY 
                && CoGaDB::HardwareDetector::instance().getNumberOfGPUs()==0){
            std::cout << "GPU not found, will not execute GPU regression tests!" << std::endl;
            return files;
        }
        
        DIR* dirP;
        struct dirent *dir;
        struct stat filestatus;
                
        std::string deviceDir;
        if(device == hype::CPU_ONLY) {
            deviceDir = "cpu/";
            
        } else if(device == hype::GPU_ONLY) {
            deviceDir = "gpu/";
        } else {
            COGADB_ERROR("GlobalDeviceConstraint is whether CPU_ONLY nor GPU_ONLY.","");
            exit(-1);
        }
               
        std::string dirPath = std::string(TESTDATA_PATH);
        dirPath.append(deviceDir);
        dirPath.append(dirname);
        
        dirP = opendir(dirPath.c_str());
        if(dirP == NULL) {
            COGADB_ERROR("Could not open the directory " << dirPath << 
                    " Errno: " << errno << std::endl,"");
            exit(-1);
            
        }
        
        while((dir = readdir( dirP ))) {
            std::string filepath = dirPath + "/" + dir->d_name;
            //check if file is valid and not a directory
            if(stat( filepath.c_str(), &filestatus))
                continue;
                       
            if(S_ISDIR( filestatus.st_mode )) 
                continue;
            
            std::string fileNameRaw(dir->d_name);
            
            //ignore temp files
            if(fileNameRaw.find('~',fileNameRaw.length()-1) != std::string::npos)
                continue;
            
            int indexOfFileEnding = fileNameRaw.find_last_of(".");
            
            if(indexOfFileEnding == std::string::npos) {
                std::cout << "Testdata File " << fileNameRaw << 
                        " is not corretctly formatted. " << std::endl;
                continue;
            }
            
            std::string fileName(fileNameRaw.substr(0,indexOfFileEnding));
            files.push_back(std::pair<hype::DeviceTypeConstraint,std::string>(device,fileName));
     
        }
        closedir(dirP);
        
        return files;
        
    } 
        
    std::string GenericRegressionTest::getFileContent(const std::string& filepath) {
        //returns the content of the file which is only the first line of it
        
        std::ifstream fin(filepath.c_str());

        if (!fin.is_open()) {
            std::cout << "Error: could not open file " << filepath << std::endl;
            return "";
        }

        std::string buffer;
        getline(fin, buffer, '\n');

        return buffer;  
    }
    
    TablePtr GenericRegressionTest::createTableFromFile(const std::string& deviceDir,
            const std::string& testname) {
        
        std::string testSchemaFile(TESTDATA_PATH);
        testSchemaFile.append(deviceDir);
        testSchemaFile.append("/schema/").append(testname).append(".sql");
        std::ifstream fin(testSchemaFile.c_str());
        
        if (!fin.is_open()) {
            std::cout << "Error: could not open file " << testSchemaFile << std::endl;
            return TablePtr();
        }
        
        std::string buffer;
        getline(fin, buffer, '\n');
        
        TablePtr resultTable = SQL::executeSQL(buffer, client);
        
        std::string testDataFile(TESTDATA_PATH);
        testDataFile.append(deviceDir);
        testDataFile.append("/results/").append(testname).append(".csv");
        resultTable->loadDatafromFile(testDataFile,true);
                
        return resultTable;
    }
    
    
    TablePtr GenericRegressionTest::assembleTableFromQuery(const std::string& deviceDir,
            const std::string& testname) { 
    
        std::string filepath = std::string(TESTDATA_PATH);
        filepath.append(deviceDir);
        filepath.append("/sql/").append(testname).append(".sql");
        
        std::string query = getFileContent(filepath);
        
        if(!quiet && debug)
            std::cout << "Query: " << query << std::endl;
        
        TablePtr resultTable = SQL::executeSQL(query,client);
        
        return resultTable;
    }
     
#ifdef ENABLE_GPU_ACCELERATION
        INSTANTIATE_TEST_CASE_P(GPUTestSuite_GPU,
                        GenericRegressionTest,
                        ::testing::ValuesIn(readTestcasesFromDir(hype::GPU_ONLY,"sql")));
#endif    
    
    INSTANTIATE_TEST_CASE_P(CPUTestSuite,
                        GenericRegressionTest,
                        ::testing::ValuesIn(readTestcasesFromDir(hype::CPU_ONLY,"sql")));
    
    TEST_P(GenericRegressionTest,QueryTest) {
                

        std::vector<TablePtr>& globalTables = CoGaDB::getGlobalTableList();
        
        std::string deviceDir;
        if(GetParam().first == hype::CPU_ONLY) {
            RuntimeConfiguration::instance().setGlobalDeviceConstraint(hype::CPU_ONLY);
            deviceDir = "/cpu";

        } else {
            RuntimeConfiguration::instance().setGlobalDeviceConstraint(hype::GPU_ONLY);
            deviceDir = "/gpu";
        }
        
        TablePtr newResultTable = assembleTableFromQuery(deviceDir,GetParam().second);
        TablePtr oldResultTable = createTableFromFile(deviceDir,GetParam().second);
                
        bool equal = oldResultTable->equals(oldResultTable,newResultTable); 
        
        if(!equal){
            std::cout << "Reference Result:" << std::endl;
            oldResultTable->print();
            std::cout << "Computed Result:" << std::endl;
            newResultTable->print();
        }
        
        //delete the recently created table. this is a hack due to Issue #36
        globalTables.pop_back();        
        
        ASSERT_TRUE(equal);
                
     }    
    
} // end namespace



