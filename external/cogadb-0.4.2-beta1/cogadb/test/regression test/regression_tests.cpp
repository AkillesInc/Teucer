#include <core/table.hpp>
#include <iostream>
#include <sql/server/sql_driver.hpp>
#include <sql/server/sql_parsetree.hpp>
#include <parser/commandline_interpreter.hpp>
#include "gtest/gtest.h"

namespace CoGaDB {    
    
const std::string TESTDATA_PATH = "./testdata/regression test"; 
    
CommandLineInterpreter cli;

void test(TableSchema schema,std::string filepath) {
    
//    TableSchema schema;
//    headerSSB21(schema);
    
    SQL::Driver driver;
       
    Table storedResult("REGRESSION_TEST", schema);
    
    try {
        bool processedDataFile = storedResult.loadDatafromFile(filepath);
        if(!processedDataFile) {
            std::cerr << "Fehler beim Einlesen der Datei" << filepath << std::endl;
        }    
        ASSERT_TRUE(processedDataFile);
    }
    catch (const std::exception &e) {
        std::cerr << "Exception beim Einlesen des Files: " << e.what() << std::endl;
    }
   // std::cout << "Eingelesene Daten: " << storedResult.toString() << std::endl;
    
//    RuntimeConfiguration::instance().setPathToDatabase("/home/florian/databases/cogadb-sf1");
//    ClientPtr client = ClientPtr(new LocalClient());
//    loadTables(client);
//    
//    //SQL::ParseTree::SequencePtr seq(driver.parse("select sum(lo_revenue), d_year, p_brand from lineorder, dates, part, supplier where lo_orderdate = d_datekey and lo_partkey = p_partkey and lo_suppkey = s_suppkey and p_brand between 'MFGR#2221' and 'MFGR#2228' and s_region = 'ASIA' group by d_year, p_brand order by d_year, p_brand;"));
//    //seq->execute(client);
//    
//    // Hier kommt Vergleich der Regressionsdaten mit den aktuellen QueryDaten hin
//    TablePtr table = SQL::executeSQL("select sum(lo_revenue), d_year, p_brand from lineorder, dates, part, supplier where lo_orderdate = d_datekey and lo_partkey = p_partkey and lo_suppkey = s_suppkey and p_brand between 'MFGR#2221' and 'MFGR#2228' and s_region = 'ASIA' group by d_year, p_brand order by d_year, p_brand;",client);
//    
//    if(!table) {
//        std::cerr << "Konnte SQL nicht ausführen" << std::endl;
//    }
//    else {
//        std::cout << "Tabelle wurde marterialisiert: " << table->toString() << std::endl;
//    }
}

TEST(Regressiontest, SSB11) {
    
    //benchmark regression ssb11
    TableSchema schemaSSB11;
    
    //Header for query SSB11
    schemaSSB11.push_back(Attribut(VARCHAR, "REVENUE"));
            
    test(schemaSSB11,TESTDATA_PATH + "/regression_test_ssb11.csv");
}

TEST(Regressiontest, SSB12) {
    
    //benchmark regression ssb12
    TableSchema schemaSSB12;
    
    //Header for query SSB12
    schemaSSB12.push_back(Attribut(VARCHAR, "REVENUE"));
            
    test(schemaSSB12,TESTDATA_PATH + "/regression_test_ssb12.csv");
}

TEST(Regressiontest, SSB13) {
    
    //benchmark regression ssb13
    TableSchema schemaSSB13;
    
    //Header for query SSB13
    schemaSSB13.push_back(Attribut(VARCHAR, "REVENUE"));
            
    test(schemaSSB13,TESTDATA_PATH + "/regression_test_ssb13.csv");
}

TEST(Regressiontest, SSB21) {
    
    //benchmark regression ssb21
    TableSchema schemaSSB21;
    
    //Header for query SSB21
    schemaSSB21.push_back(Attribut(VARCHAR, "D_YEAR"));
    schemaSSB21.push_back(Attribut(VARCHAR, "P_BRAND"));
    schemaSSB21.push_back(Attribut(VARCHAR, "LO_REVENUE"));
            
    test(schemaSSB21,TESTDATA_PATH + "/regression_test_ssb21.csv");
}

TEST(Regressiontest, SSB22) {
    
    //benchmark regression ssb22
    TableSchema schemaSSB22;
    
    //Header for query SSB22
    schemaSSB22.push_back(Attribut(VARCHAR, "D_YEAR"));
    schemaSSB22.push_back(Attribut(VARCHAR, "P_BRAND"));
    schemaSSB22.push_back(Attribut(VARCHAR, "LO_REVENUE"));
            
    test(schemaSSB22,TESTDATA_PATH + "/regression_test_ssb22.csv");
}

TEST(Regressiontest, SSB23) {
    
    //benchmark regression ssb23
    TableSchema schemaSSB23;
    
    //Header for query SSB23
    schemaSSB23.push_back(Attribut(VARCHAR, "D_YEAR"));
    schemaSSB23.push_back(Attribut(VARCHAR, "P_BRAND"));
    schemaSSB23.push_back(Attribut(VARCHAR, "LO_REVENUE"));
            
    test(schemaSSB23,TESTDATA_PATH + "/regression_test_ssb23.csv");
}
    
TEST(Regressiontest, SSB31) {
    
    //benchmark regression ssb31
    TableSchema schemaSSB31;
    
    //Header for query SSB31
    schemaSSB31.push_back(Attribut(VARCHAR, "C_NATION"));
    schemaSSB31.push_back(Attribut(VARCHAR, "S_NATION"));
    schemaSSB31.push_back(Attribut(VARCHAR, "D_YEAR"));
    schemaSSB31.push_back(Attribut(FLOAT, "LO_REVENUE"));
            
    test(schemaSSB31,TESTDATA_PATH + "/regression_test_ssb31.csv");
}

    
TEST(Regressiontest, SSB32) {
    
    //benchmark regression ssb32
    TableSchema schemaSSB32;
    
    //Header for query SSB32
    schemaSSB32.push_back(Attribut(VARCHAR, "C_CITY"));
    schemaSSB32.push_back(Attribut(VARCHAR, "S_CITY"));
    schemaSSB32.push_back(Attribut(VARCHAR, "D_YEAR"));
    schemaSSB32.push_back(Attribut(VARCHAR, "LO_REVENUE"));
            
    test(schemaSSB32,TESTDATA_PATH + "/regression_test_ssb32.csv");
}
    
TEST(Regressiontest, SSB33) {
    
    //benchmark regression ssb33
    TableSchema schemaSSB33;
    
    //Header for query SSB33
    schemaSSB33.push_back(Attribut(VARCHAR, "C_CITY"));
    schemaSSB33.push_back(Attribut(VARCHAR, "S_CITY"));
    schemaSSB33.push_back(Attribut(VARCHAR, "D_YEAR"));
    schemaSSB33.push_back(Attribut(VARCHAR, "LO_REVENUE"));
            
    test(schemaSSB33,TESTDATA_PATH + "/regression_test_ssb33.csv");
}

TEST(Regressiontest, SSB34) {
    
    //benchmark regression ssb34
    TableSchema schemaSSB34;
    
    //Header for query SSB34
    schemaSSB34.push_back(Attribut(VARCHAR, "C_CITY"));
    schemaSSB34.push_back(Attribut(VARCHAR, "S_CITY"));
    schemaSSB34.push_back(Attribut(VARCHAR, "D_YEAR"));
    schemaSSB34.push_back(Attribut(VARCHAR, "LO_REVENUE"));
            
    test(schemaSSB34,TESTDATA_PATH + "/regression_test_ssb34.csv");
}


}
