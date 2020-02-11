#pragma once
#include <core/table.hpp>
#include <util/time_measurement.hpp>
#include <string>
#include <parser/client.hpp>

namespace CoGaDB{


bool Unittest_Create_TPCH_Database(const std::string& path_to_files, ClientPtr client);

    bool TPCH_Q1(ClientPtr client);
    bool TPCH_Q2(ClientPtr client);
    bool TPCH_Q3(ClientPtr client);
    bool TPCH_Q4(ClientPtr client);
    bool TPCH_Q5(ClientPtr client);
    bool TPCH_Q6(ClientPtr client);
    bool TPCH_Q7(ClientPtr client);
    bool TPCH_Q9(ClientPtr client);
    bool TPCH_Q10(ClientPtr client);    
    bool TPCH_Q15(ClientPtr client);
    bool TPCH_Q17(ClientPtr client);
    bool TPCH_Q18(ClientPtr client);
    bool TPCH_Q20(ClientPtr client);
    bool TPCH_Q21(ClientPtr client);
    
    bool TPCH_Q1_hand_compiled_cpu_single_threaded(ClientPtr client);
    
}; //end namespace CogaDB

