
#include <util/star_schema_benchmark.hpp>
#include <persistence/storage_manager.hpp>
#include <core/table.hpp>
#include <parser/commandline_interpreter.hpp>
#include <boost/tokenizer.hpp>


#include <unittests/unittests.hpp>
#include <util/tpch_benchmark.hpp>
#include <util/star_schema_benchmark.hpp>
#include <core/runtime_configuration.hpp>

#include <query_processing/query_processor.hpp>

#include <boost/lexical_cast.hpp>

#include <boost/random.hpp>
#include <boost/generator_iterator.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/make_shared.hpp>

#include <utility>
#include <string>
#include <algorithm>

#include <parser/generated/Parser.h>
#include <query_processing/query_processor.hpp>

#include <sql/server/sql_driver.hpp>

using namespace std;



namespace CoGaDB {
    using namespace query_processing;

    bool Unittest_Create_Denormalized_Star_Schema_Benchmark_Database(ClientPtr client) {
        std::ostream& out = client->getOutputStream();
        
        //(const std::string& path_to_files) {
//        //if we provide a non empty path, we create a new SSBM database
//        if(!path_to_files.empty()){
//            Unittest_Create_Star_Schema_Benchmark_Database(path_to_files);
//        }
        
        out << "Denormalizing existing Star Schema Database: '" << RuntimeConfiguration::instance().getPathToDatabase() << "'" << std::endl;
        
        //join tables and store result in database
        TablePtr dates = getTablebyName("DATES");
        TablePtr supplier = getTablebyName("SUPPLIER");
        TablePtr part = getTablebyName("PART");
        TablePtr customer = getTablebyName("CUSTOMER");
        TablePtr lineorder = getTablebyName("LINEORDER");
        
        hype::ProcessingDeviceID id=hype::PD0;
        ProcessorSpecification proc_spec(id);      
        JoinParam param(proc_spec, HASH_JOIN);
                
        TablePtr tmp;
        out << "Join(DATES,LINEORDER)" << std::endl; 
        tmp=BaseTable::join(dates, "D_DATEKEY", lineorder, "LO_ORDERDATE", param);
        out << "Join(PART,TMP)" << std::endl; 
        tmp=BaseTable::join(part, "P_PARTKEY", tmp, "LO_PARTKEY", param);
        out << "Join(CUSTOMER,TMP)" << std::endl; 
        tmp=BaseTable::join(customer, "C_CUSTKEY", tmp, "LO_CUSTKEY", param);
        out << "Join(SUPPLIER,TMP)" << std::endl; 
        tmp=BaseTable::join(supplier, "S_SUPPKEY", tmp, "LO_SUPPKEY", param);
        if(!tmp){
            out << "Failed to create denormalized Star Schema Database! Did you load an existing Star Schema Database using 'loaddatabase'?" << std::endl;
            return false;
        }
        //eliminate key columns, which are no longer needed
        std::list<std::string> columns_to_select;
        columns_to_select.push_back("D_DATE");
        columns_to_select.push_back("D_DAYOFWEEK"); // fixed text, size 8, Sunday, Monday, ..., Saturday)
        columns_to_select.push_back("D_MONTH"); //  fixed text, size 9: January, ..., December
        columns_to_select.push_back("D_YEAR"); //  unique value 1992-1998
        columns_to_select.push_back("D_YEARMONTHNUM"); //  numeric (YYYYMM) -- e.g. 199803
        columns_to_select.push_back("D_YEARMONTH"); //  fixed text, size 7: Mar1998 for example
        columns_to_select.push_back("D_DAYNUMINWEEK"); //  numeric 1-7
        columns_to_select.push_back("D_DAYNUMINMONTH"); //  numeric 1-31
        columns_to_select.push_back("D_DAYNUMINYEAR"); //  numeric 1-366
        columns_to_select.push_back("D_MONTHNUMINYEAR"); //  numeric 1-12
        columns_to_select.push_back("D_WEEKNUMINYEAR"); //  numeric 1-53
        columns_to_select.push_back("D_SELLINGSEASON"); //  text, size 12 (Christmas, Summer,...)
        columns_to_select.push_back("D_LASTDAYINWEEKFL"); //  1 bit
        columns_to_select.push_back("D_LASTDAYINMONTHFL"); //  1 bit
        columns_to_select.push_back("D_HOLIDAYFL"); //  1 bit
        columns_to_select.push_back("D_WEEKDAYFL"); //  1 bit
        columns_to_select.push_back("P_NAME");
        columns_to_select.push_back("P_MFGR");
        columns_to_select.push_back("P_CATEGORY");
        columns_to_select.push_back("P_BRAND");
        columns_to_select.push_back("P_COLOR");
        columns_to_select.push_back("P_TYPE");
        columns_to_select.push_back("P_SIZE");
        columns_to_select.push_back("P_CONTAINER");
        columns_to_select.push_back("S_NAME");
        columns_to_select.push_back("S_ADDRESS");
        columns_to_select.push_back("S_CITY");
        columns_to_select.push_back("S_NATION");
        columns_to_select.push_back("S_REGION");
        columns_to_select.push_back("S_PHONE");
        columns_to_select.push_back("C_NAME");
        columns_to_select.push_back("C_ADDRESS");
        columns_to_select.push_back("C_CITY");
        columns_to_select.push_back("C_NATION");
        columns_to_select.push_back("C_REGION");
        columns_to_select.push_back("C_PHONE");
        columns_to_select.push_back("C_MKTSEGMENT");
        columns_to_select.push_back("LO_ORDERPRIORITY");
        columns_to_select.push_back("LO_SHIPPRIORITY");
        columns_to_select.push_back("LO_QUANTITY");
        columns_to_select.push_back("LO_EXTENDEDPRICE");
        columns_to_select.push_back("LO_ORDTOTALPRICE");
        columns_to_select.push_back("LO_DISCOUNT"); //l_quantity );
        columns_to_select.push_back("LO_REVENUE"); //l_extendedprice);
        columns_to_select.push_back("LO_SUPPLYCOST"); //l_discount);
        columns_to_select.push_back("LO_TAX"); //l_tax);
        columns_to_select.push_back("LO_COMMITDATE"); //l_commitdate);
        columns_to_select.push_back("LO_SHIPMODE"); //l_shipmode);
            
        tmp=BaseTable::projection(tmp, columns_to_select);
        out << "Materializing Table: " << std::endl;
        TablePtr denormalized_star_schema_database = tmp->materialize();
        denormalized_star_schema_database->setName("DENORMALIZED_SSBM_TABLE");
        //return denormalized_star_schema_database
        
        //create new table in separate database 
//        std::string current_path = RuntimeConfiguration::instance().getPathToDatabase();
//        unsigned found = current_path.find_last_of("/");
//        std::string new_path = current_path.substr(0,found);
//        RuntimeConfiguration::instance().setPathToDatabase(new_path+"_denormalized");
        
        RuntimeConfiguration::instance().setPathToDatabase(RuntimeConfiguration::instance().getPathToDatabase()+"_denormalized");
        out << "Store Denormalized Star Schema Database: '" << RuntimeConfiguration::instance().getPathToDatabase() << "'" << std::endl;
        storeTable(denormalized_star_schema_database);
        addToGlobalTableList(denormalized_star_schema_database);
        return true;
    }

     /*QUERIES*/

//     LogicalQueryPlanPtr Denormalized_SSB_Q11_plan(){
//
//         //*****************************************************************
//         //        --Q1.1
//         //        select sum(lo_extendedprice*lo_discount) as revenue
//         //        from lineorder, dates
//         //        where lo_orderdate =  d_datekey
//         //        and d_year = 1993
//         //        and lo_discount between 1 and 3
//         //        and lo_quantity < 25;
//         //*****************************************************************
//
//         hype::DeviceConstraint default_device_constraint = CoGaDB::RuntimeConfiguration::instance().getGlobalDeviceConstraint();
//         boost::shared_ptr<logical_operator::Logical_Scan> scan_denormalized_ssbm(new logical_operator::Logical_Scan("DENORMALIZED_SSBM_TABLE"));
//
//         //Attribut(INT,"YEARMONTHNUM")); //  numeric (YYYYMM) -- e.g. 199803
//
//         KNF_Selection_Expression knf_expr; //D_YEAR = 1993 AND LO_QUANTITY<25 AND LO_DISCOUNT>0.99 AND LO_DISCOUNT<3.01
//                  {
//                           Disjunction d;
//                           d.push_back(Predicate("D_YEAR", boost::any(1993), ValueConstantPredicate, EQUAL)); //YEAR =  1993
//                           knf_expr.disjunctions.push_back(d);
//                  }
//                  {
//                      Disjunction d;
//                      d.push_back(Predicate("LO_QUANTITY", boost::any(25), ValueConstantPredicate, LESSER)); //LO_DISCOUNT>0.99
//                      knf_expr.disjunctions.push_back(d);
//                  }
//                  {
//                      Disjunction d;
//                      d.push_back(Predicate("LO_DISCOUNT", boost::any(float(1)), ValueConstantPredicate, GREATER_EQUAL)); //LO_DISCOUNT>0.99
//                      knf_expr.disjunctions.push_back(d);
//                  }
//                  {
//                      Disjunction d;
//                      d.push_back(Predicate("LO_DISCOUNT", boost::any(float(3)), ValueConstantPredicate, LESSER_EQUAL)); //LO_DISCOUNT<3.01
//                      knf_expr.disjunctions.push_back(d);
//                  }
//         boost::shared_ptr<logical_operator::Logical_ComplexSelection> complex_selection_on_denormalized_ssbm(new logical_operator::Logical_ComplexSelection(knf_expr, LOOKUP, default_device_constraint)); //hype::DeviceConstraint(hype::CPU_ONLY)));
//
//         boost::shared_ptr<logical_operator::Logical_ColumnAlgebraOperator> column_algebra_operation_denormalized_ssbm(new logical_operator::Logical_ColumnAlgebraOperator("LO_EXTENDEDPRICE", "LO_DISCOUNT", "REVENUE", MUL, default_device_constraint));
//
//         std::list<std::string> sorting_column_names;
//         sorting_column_names.push_back("D_YEAR");
//
//         std::list<std::pair<string, AggregationMethod> > aggregation_functions;
//         aggregation_functions.push_back(make_pair("REVENUE", SUM));
//         boost::shared_ptr<logical_operator::Logical_Sort> order(new logical_operator::Logical_Sort(sorting_column_names, ASCENDING, LOOKUP, default_device_constraint));
//         boost::shared_ptr<logical_operator::Logical_Groupby> group(new logical_operator::Logical_Groupby(sorting_column_names, aggregation_functions, LOOKUP, default_device_constraint));
//
//         complex_selection_on_denormalized_ssbm->setLeft(scan_denormalized_ssbm);
//         column_algebra_operation_denormalized_ssbm->setLeft(complex_selection_on_denormalized_ssbm);
//
//         order->setLeft(column_algebra_operation_denormalized_ssbm);
//         group->setLeft(order);
//
//         std::list<std::string> column_list;
//         column_list.push_back("REVENUE");
//         boost::shared_ptr<logical_operator::Logical_Projection> projection(new logical_operator::Logical_Projection(column_list)); //GPU Projection not supported
//
//         projection->setLeft(group);
//         return boost::make_shared<LogicalQueryPlan>(projection);
//     }
//
//     bool Denormalized_SSB_Q11() {
// 	return optimize_execute_print("Denormalized SSB Query 1.1", *Denormalized_SSB_Q11_plan());
//     }
//
//     LogicalQueryPlanPtr Denormalized_SSB_Q12_plan() {
//         //*****************************************************************
//         //--Q1.2
//         //select sum(lo_extendedprice*lo_discount) as
//         //revenue
//         //from lineorder, dates
//         //where lo_orderdate =  d_datekey
//         //and d_yearmonthnum = 199401
//         //and lo_discount between 4 and 6
//         //and lo_quantity between 26 and 35;
//         //*****************************************************************
//         hype::DeviceConstraint default_device_constraint = CoGaDB::RuntimeConfiguration::instance().getGlobalDeviceConstraint();
//
//         boost::shared_ptr<logical_operator::Logical_Scan> scan_denormalized_ssbm(new logical_operator::Logical_Scan("DENORMALIZED_SSBM_TABLE"));
//
//         //Attribut(INT,"YEARMONTHNUM")); //  numeric (YYYYMM) -- e.g. 199803
//
//         KNF_Selection_Expression knf_expr;
//         {
//             Disjunction d;
//             d.push_back(Predicate("LO_QUANTITY", boost::any(26), ValueConstantPredicate, GREATER_EQUAL)); //LO_QUANTITY>=26
//             knf_expr.disjunctions.push_back(d);
//         }
//         {
//             Disjunction d;
//             d.push_back(Predicate("LO_QUANTITY", boost::any(35), ValueConstantPredicate, LESSER_EQUAL)); //LO_QUANTITY<=35
//             knf_expr.disjunctions.push_back(d);
//         }
//         {
//             Disjunction d;
//             d.push_back(Predicate("LO_DISCOUNT", boost::any(float(4)), ValueConstantPredicate, GREATER_EQUAL)); //LO_DISCOUNT>=4
//             knf_expr.disjunctions.push_back(d);
//         }
//         {
//             Disjunction d;
//             d.push_back(Predicate("LO_DISCOUNT", boost::any(float(6)), ValueConstantPredicate, LESSER_EQUAL)); //LO_DISCOUNT<=6
//             knf_expr.disjunctions.push_back(d);
//         }
//         {
//             Disjunction d;
//             d.push_back(Predicate("D_YEARMONTHNUM", boost::any(199401), ValueConstantPredicate, EQUAL)); //d_yearmonthnum = 199401
//             knf_expr.disjunctions.push_back(d);
//         }
//
//         boost::shared_ptr<logical_operator::Logical_ComplexSelection> complex_selection_on_denormalized_ssbm(new logical_operator::Logical_ComplexSelection(knf_expr, LOOKUP, default_device_constraint)); //hype::DeviceConstraint(hype::CPU_ONLY)));
//
//         boost::shared_ptr<logical_operator::Logical_ColumnAlgebraOperator> column_algebra_operation_denormalized_ssbm(new logical_operator::Logical_ColumnAlgebraOperator("LO_EXTENDEDPRICE", "LO_DISCOUNT", "REVENUE", MUL, default_device_constraint));
//
//         std::list<std::string> sorting_column_names;
//         sorting_column_names.push_back("D_YEAR");
//
//         std::list<std::pair<string, AggregationMethod> > aggregation_functions;
//         aggregation_functions.push_back(make_pair("REVENUE", SUM));
//
//         boost::shared_ptr<logical_operator::Logical_Sort> order(new logical_operator::Logical_Sort(sorting_column_names, ASCENDING, LOOKUP, default_device_constraint));
//         boost::shared_ptr<logical_operator::Logical_Groupby> group(new logical_operator::Logical_Groupby(sorting_column_names, aggregation_functions, LOOKUP, default_device_constraint));
//
//         complex_selection_on_denormalized_ssbm->setLeft(scan_denormalized_ssbm);
//
//         column_algebra_operation_denormalized_ssbm->setLeft(complex_selection_on_denormalized_ssbm);
//         order->setLeft(column_algebra_operation_denormalized_ssbm);
//         group->setLeft(order);
//
//         std::list<std::string> column_list;
//         column_list.push_back("REVENUE");
//         boost::shared_ptr<logical_operator::Logical_Projection> projection(new logical_operator::Logical_Projection(column_list)); //GPU Projection not supported
//
//         projection->setLeft(group);
//         return boost::make_shared<LogicalQueryPlan>(projection);
//     }
//
//     bool Denormalized_SSB_Q12() {
// 	return optimize_execute_print("Denormalized SSB Query 1.2", *Denormalized_SSB_Q12_plan());
//     }
//
//     LogicalQueryPlanPtr Denormalized_SSB_Q13_plan() {
//         //*****************************************************************
//         //--Q1.3
//         //select sum(lo_extendedprice*lo_discount) as
//         //revenue
//         //from lineorder, dates
//         //where lo_orderdate =  d_datekey
//         //and d_weeknuminyear = 6
//         //and d_year = 1994
//         //and lo_discount between 5 and 7
//         //and lo_quantity between 26 and 35;
//         //*****************************************************************
//
//         hype::DeviceConstraint default_device_constraint = CoGaDB::RuntimeConfiguration::instance().getGlobalDeviceConstraint();
//
//         boost::shared_ptr<logical_operator::Logical_Scan> scan_denormalized_ssbm(new logical_operator::Logical_Scan("DENORMALIZED_SSBM_TABLE"));
//
//         KNF_Selection_Expression knf_expr;
//         {
//             Disjunction d;
//             d.push_back(Predicate("LO_QUANTITY", boost::any(26), ValueConstantPredicate, GREATER_EQUAL)); //LO_DISCOUNT>0.99
//             knf_expr.disjunctions.push_back(d);
//         }
//         {
//             Disjunction d;
//             d.push_back(Predicate("LO_QUANTITY", boost::any(35), ValueConstantPredicate, LESSER_EQUAL)); //LO_DISCOUNT>0.99
//             knf_expr.disjunctions.push_back(d);
//         }
//         {
//             Disjunction d;
//             d.push_back(Predicate("LO_DISCOUNT", boost::any(float(5)), ValueConstantPredicate, GREATER_EQUAL)); //LO_DISCOUNT>0.99
//             knf_expr.disjunctions.push_back(d);
//         }
//         {
//             Disjunction d;
//             d.push_back(Predicate("LO_DISCOUNT", boost::any(float(7)), ValueConstantPredicate, LESSER_EQUAL)); //LO_DISCOUNT<3.01
//             knf_expr.disjunctions.push_back(d);
//         }
//         {
//             Disjunction d;
//             d.push_back(Predicate("D_YEAR", boost::any(1994), ValueConstantPredicate, EQUAL));
//             knf_expr.disjunctions.push_back(d);
//          }
//          {
//             Disjunction d;
//             d.push_back(Predicate("D_WEEKNUMINYEAR", boost::any(6), ValueConstantPredicate, EQUAL));
//             knf_expr.disjunctions.push_back(d);
//         }
//
//         boost::shared_ptr<logical_operator::Logical_ComplexSelection> complex_selection_on_denormalized_ssbm(new logical_operator::Logical_ComplexSelection(knf_expr, LOOKUP, default_device_constraint)); //hype::DeviceConstraint(hype::CPU_ONLY)));
//
//         boost::shared_ptr<logical_operator::Logical_ColumnAlgebraOperator> column_algebra_operation_denormalized_ssbm(new logical_operator::Logical_ColumnAlgebraOperator("LO_EXTENDEDPRICE", "LO_DISCOUNT", "REVENUE", MUL, default_device_constraint));
//
//         std::list<std::string> sorting_column_names;
//         sorting_column_names.push_back("D_YEAR");
//
//         std::list<std::pair<string, AggregationMethod> > aggregation_functions;
//         aggregation_functions.push_back(make_pair("REVENUE", SUM));
//
//         boost::shared_ptr<logical_operator::Logical_Sort> order(new logical_operator::Logical_Sort(sorting_column_names, ASCENDING, LOOKUP, default_device_constraint));
//         boost::shared_ptr<logical_operator::Logical_Groupby> group(new logical_operator::Logical_Groupby(sorting_column_names, aggregation_functions, LOOKUP, default_device_constraint));
//
//         complex_selection_on_denormalized_ssbm->setLeft(scan_denormalized_ssbm);
//
//         column_algebra_operation_denormalized_ssbm->setLeft(complex_selection_on_denormalized_ssbm);
//         order->setLeft(column_algebra_operation_denormalized_ssbm);
//         group->setLeft(order);
//
//         std::list<std::string> column_list;
//         column_list.push_back("REVENUE");
//         boost::shared_ptr<logical_operator::Logical_Projection> projection(new logical_operator::Logical_Projection(column_list)); //GPU Projection not supported
//
//         projection->setLeft(group);
//         return boost::make_shared<LogicalQueryPlan>(projection);
//     }
//
//     bool Denormalized_SSB_Q13() {
// 	return optimize_execute_print("Denormalized SSB Query 1.3", *Denormalized_SSB_Q13_plan());
//     }
//
//     //USE_INVISIBLE_JOIN_FOR_STORED_PROCEDURES defined
//     LogicalQueryPlanPtr Denormalized_SSB_Q21_plan() {
//         //*****************************************************************
//         //--Q2.1
//         //select sum(lo_revenue), d_year, p_brand
//         //from lineorder, dates, part, supplier
//         //where lo_orderdate =  d_datekey
//         //and lo_partkey = p_partkey
//         //and lo_suppkey = s_suppkey
//         //and p_category = 'MFGR#12'
//         //and s_region = 'AMERICA'
//         //group by d_year, p_brand
//         //order by d_year, p_brand;
//         //*****************************************************************
//         hype::DeviceConstraint default_device_constraint = CoGaDB::RuntimeConfiguration::instance().getGlobalDeviceConstraint();
//
//         boost::shared_ptr<logical_operator::Logical_Scan> scan_denormalized_ssbm(new logical_operator::Logical_Scan("DENORMALIZED_SSBM_TABLE"));
//
//         KNF_Selection_Expression knf_expr;
//         {
//             Disjunction d;
//             d.push_back(Predicate("S_REGION", boost::any(std::string("AMERICA")), ValueConstantPredicate, EQUAL));
//             knf_expr.disjunctions.push_back(d);
//         }
//         {
//             Disjunction d;
//             d.push_back(Predicate("P_CATEGORY", boost::any(std::string("MFGR#12")), ValueConstantPredicate, EQUAL)); //YEAR<2013
//             knf_expr.disjunctions.push_back(d);
//         }
//
//         boost::shared_ptr<logical_operator::Logical_ComplexSelection> complex_selection_on_denormalized_ssbm(new logical_operator::Logical_ComplexSelection(knf_expr, LOOKUP, default_device_constraint)); //hype::DeviceConstraint(hype::CPU_ONLY)));
//
//         std::list<std::string> sorting_column_names;
//         sorting_column_names.push_back("D_YEAR");
//         sorting_column_names.push_back("P_BRAND");
//
//         std::list<std::pair<string, AggregationMethod> > aggregation_functions;
//         aggregation_functions.push_back(make_pair("LO_REVENUE", SUM));
//
//         boost::shared_ptr<logical_operator::Logical_Groupby> group(new logical_operator::Logical_Groupby(sorting_column_names, aggregation_functions, LOOKUP, default_device_constraint));
//
//         boost::shared_ptr<logical_operator::Logical_Sort> order(new logical_operator::Logical_Sort(sorting_column_names, ASCENDING, LOOKUP, hype::DeviceConstraint(hype::CPU_ONLY)));
//
//         complex_selection_on_denormalized_ssbm->setLeft(scan_denormalized_ssbm);
//         order->setLeft(complex_selection_on_denormalized_ssbm);
//         group->setLeft(order);
//
//         return boost::make_shared<LogicalQueryPlan>(group);
//     }
//
//     bool Denormalized_SSB_Q21() {
// 	return optimize_execute_print("Denormalized SSB Query 2.1", *Denormalized_SSB_Q21_plan());
//     }
//
//     //USE_INVISIBLE_JOIN_FOR_STORED_PROCEDURES defined
//     LogicalQueryPlanPtr Denormalized_SSB_Q22_plan() {
//         //*****************************************************************
//         //--Q2.2
//         //select sum(lo_revenue), d_year, p_brand
//         //from lineorder, dates, part, supplier
//         //where lo_orderdate =  d_datekey
//         //and lo_partkey = p_partkey
//         //and lo_suppkey = s_suppkey
//         //and p_brand between 'MFGR#2221'
//         //and 'MFGR#2228'
//         //and s_region = 'ASIA'
//         //group by d_year, p_brand
//         //order by d_year, p_brand;
//         //*****************************************************************
//         hype::DeviceConstraint default_device_constraint = CoGaDB::RuntimeConfiguration::instance().getGlobalDeviceConstraint();
//
//         boost::shared_ptr<logical_operator::Logical_Scan> scan_denormalized_ssbm(new logical_operator::Logical_Scan("DENORMALIZED_SSBM_TABLE"));
//         KNF_Selection_Expression knf_expr;
//         {
//             Disjunction d;
//             d.push_back(Predicate("S_REGION", boost::any(std::string("ASIA")), ValueConstantPredicate, EQUAL));
//             knf_expr.disjunctions.push_back(d);
//         }
//         {
//             Disjunction d;
//             d.push_back(Predicate("P_BRAND", boost::any(std::string("MFGR#2221")), ValueConstantPredicate, GREATER_EQUAL));
//             knf_expr.disjunctions.push_back(d);
//         }
//         {
//             Disjunction d;
//             d.push_back(Predicate("P_BRAND", boost::any(std::string("MFGR#2228")), ValueConstantPredicate, LESSER_EQUAL));
//             knf_expr.disjunctions.push_back(d);
//         }
//
//         boost::shared_ptr<logical_operator::Logical_ComplexSelection> complex_selection_on_denormalized_ssbm(new logical_operator::Logical_ComplexSelection(knf_expr, LOOKUP, default_device_constraint)); //hype::DeviceConstraint(hype::CPU_ONLY)));
//
//         std::list<std::string> sorting_column_names;
//         sorting_column_names.push_back("D_YEAR");
//         sorting_column_names.push_back("P_BRAND");
//
//         std::list<std::pair<string, AggregationMethod> > aggregation_functions;
//         aggregation_functions.push_back(make_pair("LO_REVENUE", SUM));
//
//         boost::shared_ptr<logical_operator::Logical_Groupby> group(new logical_operator::Logical_Groupby(sorting_column_names, aggregation_functions, LOOKUP, default_device_constraint));
//
//         boost::shared_ptr<logical_operator::Logical_Sort> order(new logical_operator::Logical_Sort(sorting_column_names, ASCENDING, LOOKUP, hype::DeviceConstraint(hype::CPU_ONLY)));
//
//         complex_selection_on_denormalized_ssbm->setLeft(scan_denormalized_ssbm);
//         order->setLeft(complex_selection_on_denormalized_ssbm);
//         group->setLeft(order);
//
//         return boost::make_shared<LogicalQueryPlan>(group);
//     }
//     bool Denormalized_SSB_Q22() {
// 	return optimize_execute_print("Denormalized SSB Query 2.2", *Denormalized_SSB_Q22_plan());
//     }
//
//      //USE_INVISIBLE_JOIN_FOR_STORED_PROCEDURES defined
//     LogicalQueryPlanPtr Denormalized_SSB_Q23_plan() {
//         //*****************************************************************
//         //--Q2.3
//         //select sum(lo_revenue), d_year, p_brand
//         //from lineorder, dates, part, supplier
//         //where lo_orderdate =  d_datekey
//         //and lo_partkey = p_partkey
//         //and lo_suppkey = s_suppkey
//         //and p_brand= 'MFGR#2239'
//         //and s_region = 'EUROPE'
//         //group by d_year, p_brand
//         //order by d_year, p_brand;
//         //*****************************************************************
//         hype::DeviceConstraint default_device_constraint = CoGaDB::RuntimeConfiguration::instance().getGlobalDeviceConstraint();
//
//         boost::shared_ptr<logical_operator::Logical_Scan> scan_denormalized_ssbm(new logical_operator::Logical_Scan("DENORMALIZED_SSBM_TABLE"));
//
//         KNF_Selection_Expression knf_expr;
//         {
//             Disjunction d;
//             d.push_back(Predicate("S_REGION", boost::any(std::string("EUROPE")), ValueConstantPredicate, EQUAL));
//             knf_expr.disjunctions.push_back(d);
//         }
//         {
//             Disjunction d;
//             d.push_back(Predicate("P_BRAND", boost::any(std::string("MFGR#2239")), ValueConstantPredicate, EQUAL));
//             knf_expr.disjunctions.push_back(d);
//         }
//
//         boost::shared_ptr<logical_operator::Logical_ComplexSelection> complex_selection_on_denormalized_ssbm(new logical_operator::Logical_ComplexSelection(knf_expr, LOOKUP, default_device_constraint)); //hype::DeviceConstraint(hype::CPU_ONLY)));
//
//         std::list<std::string> sorting_column_names;
//         sorting_column_names.push_back("D_YEAR");
//         sorting_column_names.push_back("P_BRAND");
//
//         std::list<std::pair<string, AggregationMethod> > aggregation_functions;
//         aggregation_functions.push_back(make_pair("LO_REVENUE", SUM));
//
//         boost::shared_ptr<logical_operator::Logical_Groupby> group(new logical_operator::Logical_Groupby(sorting_column_names, aggregation_functions, LOOKUP, default_device_constraint));
//
//         boost::shared_ptr<logical_operator::Logical_Sort> order(new logical_operator::Logical_Sort(sorting_column_names, ASCENDING, LOOKUP, hype::DeviceConstraint(hype::CPU_ONLY)));
//
//         complex_selection_on_denormalized_ssbm->setLeft(scan_denormalized_ssbm);
//         order->setLeft(complex_selection_on_denormalized_ssbm);
//         group->setLeft(order);
//
//         return boost::make_shared<LogicalQueryPlan>(group);
//     }
//
//     bool Denormalized_SSB_Q23() {
// 	return optimize_execute_print("Denormalized SSB Query 2.3", *Denormalized_SSB_Q23_plan());
//     }
//
//     LogicalQueryPlanPtr Denormalized_SSB_Q31_plan() {
//         //*****************************************************************
//         //--Q3.1
//         //select c_nation, s_nation, d_year,
//         //sum(lo_revenue)  as  revenue
//         //from customer, lineorder, supplier, dates
//         //where lo_custkey = c_custkey
//         //and lo_suppkey = s_suppkey
//         //and lo_orderdate = d_datekey
//         //and c_region = 'ASIA'
//         //and s_region = 'ASIA'
//         //and d_year >= 1992 and d_year <= 1997
//         //group by c_nation, s_nation, d_year
//         //order by d_year asc,  revenue desc;
//         //*****************************************************************
//         hype::DeviceConstraint default_device_constraint = CoGaDB::RuntimeConfiguration::instance().getGlobalDeviceConstraint();
//
//         boost::shared_ptr<logical_operator::Logical_Scan> scan_denormalized_ssbm(new logical_operator::Logical_Scan("DENORMALIZED_SSBM_TABLE"));
//
//         KNF_Selection_Expression knf_expr;
//         {
//             Disjunction d;
//             d.push_back(Predicate("D_YEAR", boost::any(1992), ValueConstantPredicate, GREATER_EQUAL));
//             knf_expr.disjunctions.push_back(d);
//         }
//         {
//             Disjunction d;
//             d.push_back(Predicate("D_YEAR", boost::any(1997), ValueConstantPredicate, LESSER_EQUAL));
//             knf_expr.disjunctions.push_back(d);
//         }
//         {
//             Disjunction d;
//             d.push_back(Predicate("C_REGION", boost::any(std::string("ASIA")), ValueConstantPredicate, EQUAL));
//             knf_expr.disjunctions.push_back(d);
//         }
//         {
//             Disjunction d;
//             d.push_back(Predicate("S_REGION", boost::any(std::string("ASIA")), ValueConstantPredicate, EQUAL));
//             knf_expr.disjunctions.push_back(d);
//         }
//
//         boost::shared_ptr<logical_operator::Logical_ComplexSelection> complex_selection_on_denormalized_ssbm(new logical_operator::Logical_ComplexSelection(knf_expr, LOOKUP, default_device_constraint)); //hype::DeviceConstraint(hype::CPU_ONLY)));
//
//         std::list<std::string> groupsorting_column_names;
//         groupsorting_column_names.push_back("C_NATION");
//         groupsorting_column_names.push_back("S_NATION");
//         groupsorting_column_names.push_back("D_YEAR");
//         boost::shared_ptr<logical_operator::Logical_Sort> grouporder(new logical_operator::Logical_Sort(groupsorting_column_names, ASCENDING, LOOKUP, hype::DeviceConstraint(hype::CPU_ONLY)));
//
//
//         std::list<std::string> grouping_column_names;
//         grouping_column_names.push_back("C_NATION");
//         grouping_column_names.push_back("S_NATION");
//         grouping_column_names.push_back("D_YEAR");
//         std::list<std::pair<string, AggregationMethod> > aggregation_functions;
//         aggregation_functions.push_back(make_pair("LO_REVENUE", SUM));
//
//         boost::shared_ptr<logical_operator::Logical_Groupby> group(new logical_operator::Logical_Groupby(grouping_column_names, aggregation_functions, LOOKUP, default_device_constraint));
//
//
//         std::list<std::string> sorting_column_names;
//         sorting_column_names.push_back("D_YEAR");
//         sorting_column_names.push_back("LO_REVENUE");
//         boost::shared_ptr<logical_operator::Logical_Sort> order(new logical_operator::Logical_Sort(sorting_column_names, ASCENDING, LOOKUP, default_device_constraint));
//
//         complex_selection_on_denormalized_ssbm->setLeft(scan_denormalized_ssbm);
//
//         grouporder->setLeft(complex_selection_on_denormalized_ssbm);
//         group->setLeft(grouporder);
//         order->setLeft(group);
//
//         return boost::make_shared<LogicalQueryPlan>(order);
//
//     }
//
//     bool Denormalized_SSB_Q31() {
// 	return optimize_execute_print("Denormalized SSB Query 3.1", *Denormalized_SSB_Q31_plan());
//     }
//
//
//     LogicalQueryPlanPtr Denormalized_SSB_Q32_plan() {
//         //*****************************************************************
//         //--Q3.2
//         //select c_city, s_city, d_year, sum(lo_revenue)
//         //as  revenue
//         //from customer, lineorder, supplier, dates
//         //where lo_custkey = c_custkey
//         //and lo_suppkey = s_suppkey
//         //and lo_orderdate = d_datekey
//         //and c_nation = 'UNITED STATES'
//         //and s_nation = 'UNITED STATES'
//         //and d_year >= 1992 and d_year <= 1997
//         //group by c_city, s_city, d_year
//         //order by d_year asc,  revenue desc;
//         //*****************************************************************
//         hype::DeviceConstraint default_device_constraint = CoGaDB::RuntimeConfiguration::instance().getGlobalDeviceConstraint();
//
//         boost::shared_ptr<logical_operator::Logical_Scan> scan_denormalized_ssbm(new logical_operator::Logical_Scan("DENORMALIZED_SSBM_TABLE"));
//         KNF_Selection_Expression knf_expr;
//         {
//             Disjunction d;
//             d.push_back(Predicate("D_YEAR", boost::any(1992), ValueConstantPredicate, GREATER_EQUAL));
//             knf_expr.disjunctions.push_back(d);
//         }
//         {
//             Disjunction d;
//             d.push_back(Predicate("D_YEAR", boost::any(1997), ValueConstantPredicate, LESSER_EQUAL));
//             knf_expr.disjunctions.push_back(d);
//         }
//         {
//             Disjunction d;
//             d.push_back(Predicate("C_NATION", boost::any(std::string("UNITED STATES")), ValueConstantPredicate, EQUAL));
//             knf_expr.disjunctions.push_back(d);
//         }
//         {
//             Disjunction d;
//             d.push_back(Predicate("S_NATION", boost::any(std::string("UNITED STATES")), ValueConstantPredicate, EQUAL));
//             knf_expr.disjunctions.push_back(d);
//         }
//
//         boost::shared_ptr<logical_operator::Logical_ComplexSelection> complex_selection_on_denormalized_ssbm(new logical_operator::Logical_ComplexSelection(knf_expr, LOOKUP, default_device_constraint)); //hype::DeviceConstraint(hype::CPU_ONLY)));
//
//         std::list<std::string> ordersorting_column_names;
//         ordersorting_column_names.push_back("C_CITY");
//         ordersorting_column_names.push_back("S_CITY");
//         ordersorting_column_names.push_back("D_YEAR");
//         boost::shared_ptr<logical_operator::Logical_Sort> grouporder(new logical_operator::Logical_Sort(ordersorting_column_names, ASCENDING, LOOKUP, hype::DeviceConstraint(hype::CPU_ONLY)));
//
//
//         std::list<std::string> grouping_column_names;
//         grouping_column_names.push_back("C_CITY");
//         grouping_column_names.push_back("S_CITY");
//         grouping_column_names.push_back("D_YEAR");
//         std::list<std::pair<string, AggregationMethod> > aggregation_functions;
//         aggregation_functions.push_back(make_pair("LO_REVENUE", SUM));
//
//         boost::shared_ptr<logical_operator::Logical_Groupby> group(new logical_operator::Logical_Groupby(grouping_column_names, aggregation_functions, LOOKUP, default_device_constraint));
//
//         std::list<std::string> sorting_column_names;
//         sorting_column_names.push_back("D_YEAR");
//         sorting_column_names.push_back("LO_REVENUE");
//         boost::shared_ptr<logical_operator::Logical_Sort> order(new logical_operator::Logical_Sort(sorting_column_names, ASCENDING, LOOKUP, default_device_constraint));
//
//         complex_selection_on_denormalized_ssbm->setLeft(scan_denormalized_ssbm);
//
//         grouporder->setLeft(complex_selection_on_denormalized_ssbm);
//         group->setLeft(grouporder);
//         order->setLeft(group);
//
//         return boost::make_shared<LogicalQueryPlan>(order);
//
//     }
//
//     bool Denormalized_SSB_Q32() {
// 	return optimize_execute_print("Denormalized SSB Query 3.2", *Denormalized_SSB_Q32_plan());
//     }
//
//
//     LogicalQueryPlanPtr Denormalized_SSB_Q33_plan() {
//         //*****************************************************************
//         //--Q3.3
//         //select c_city, s_city, d_year, sum(lo_revenue)
//         //as  revenue
//         //from customer, lineorder, supplier, dates
//         //where lo_custkey = c_custkey
//         //and lo_suppkey = s_suppkey
//         //and lo_orderdate = d_datekey
//         //and  (c_city='UNITED KI1'
//         //or c_city='UNITED KI5')
//         //and (s_city='UNITED KI1'
//         //or s_city='UNITED KI5')
//         //and d_year >= 1992 and d_year <= 1997
//         //group by c_city, s_city, d_year
//         //order by d_year asc,  revenue desc;
//         //*****************************************************************
//         hype::DeviceConstraint default_device_constraint = CoGaDB::RuntimeConfiguration::instance().getGlobalDeviceConstraint();
//
//         boost::shared_ptr<logical_operator::Logical_Scan> scan_denormalized_ssbm(new logical_operator::Logical_Scan("DENORMALIZED_SSBM_TABLE"));
//         KNF_Selection_Expression knf_expr;
//         {
//             Disjunction d;
//             d.push_back(Predicate("D_YEAR", boost::any(1992), ValueConstantPredicate, GREATER_EQUAL));
//             knf_expr.disjunctions.push_back(d);
//         }
//         {
//             Disjunction d;
//             d.push_back(Predicate("D_YEAR", boost::any(1997), ValueConstantPredicate, LESSER_EQUAL));
//             knf_expr.disjunctions.push_back(d);
//         }
//         {
//             Disjunction d;
//             d.push_back(Predicate("C_CITY", boost::any(std::string("UNITED KI1")), ValueConstantPredicate, EQUAL)); //YEAR<2013
//             d.push_back(Predicate("C_CITY", boost::any(std::string("UNITED KI5")), ValueConstantPredicate, EQUAL)); //Vergleich wert in Spalte mit Konstante
//             knf_expr.disjunctions.push_back(d);
//         }
//         {
//             Disjunction d;
//             d.push_back(Predicate("S_CITY", boost::any(std::string("UNITED KI1")), ValueConstantPredicate, EQUAL)); //YEAR<2013
//             d.push_back(Predicate("S_CITY", boost::any(std::string("UNITED KI5")), ValueConstantPredicate, EQUAL));
//             knf_expr.disjunctions.push_back(d);
//         }
//
//         boost::shared_ptr<logical_operator::Logical_ComplexSelection> complex_selection_on_denormalized_ssbm(new logical_operator::Logical_ComplexSelection(knf_expr, LOOKUP, default_device_constraint)); //hype::DeviceConstraint(hype::CPU_ONLY)));
//
//
//         std::list<std::string> ordersorting_column_names;
//         ordersorting_column_names.push_back("C_CITY");
//         ordersorting_column_names.push_back("S_CITY");
//         ordersorting_column_names.push_back("D_YEAR");
//         boost::shared_ptr<logical_operator::Logical_Sort> grouporder(new logical_operator::Logical_Sort(ordersorting_column_names, ASCENDING, LOOKUP, hype::DeviceConstraint(hype::CPU_ONLY)));
//
//
//         std::list<std::string> grouping_column_names;
//         grouping_column_names.push_back("C_CITY");
//         grouping_column_names.push_back("S_CITY");
//         grouping_column_names.push_back("D_YEAR");
//         std::list<std::pair<string, AggregationMethod> > aggregation_functions;
//         aggregation_functions.push_back(make_pair("LO_REVENUE", SUM));
//
//         boost::shared_ptr<logical_operator::Logical_Groupby> group(new logical_operator::Logical_Groupby(grouping_column_names, aggregation_functions, LOOKUP, default_device_constraint));
//
//         std::list<std::string> sorting_column_names;
//         sorting_column_names.push_back("D_YEAR");
//         sorting_column_names.push_back("LO_REVENUE");
//         boost::shared_ptr<logical_operator::Logical_Sort> order(new logical_operator::Logical_Sort(sorting_column_names, ASCENDING, LOOKUP, hype::DeviceConstraint(hype::CPU_ONLY)));
//
//         complex_selection_on_denormalized_ssbm->setLeft(scan_denormalized_ssbm);
//
//         grouporder->setLeft(complex_selection_on_denormalized_ssbm);
//         group->setLeft(grouporder);
//         order->setLeft(group);
//
//         return boost::make_shared<LogicalQueryPlan>(order);
//
//     }
//
//     bool Denormalized_SSB_Q33() {
// 	return optimize_execute_print("Denormalized SSB Query 3.3", *Denormalized_SSB_Q33_plan());
//     }
//
//
//     LogicalQueryPlanPtr Denormalized_SSB_Q34_plan() {
//        //*****************************************************************
//         //--Q3.4
//         //select c_city, s_city, d_year, sum(lo_revenue)
//         //as  revenue
//         //from customer, lineorder, supplier, dates
//         //where lo_custkey = c_custkey
//         //and lo_suppkey = s_suppkey
//         //and lo_orderdate = d_datekey
//         //and  (c_city='UNITED KI1'
//         //or c_city='UNITED KI5')
//         //and (s_city='UNITED KI1'
//         //or s_city='UNITED KI5')
//         //and d_yearmonth = 'Dec1997'
//         //group by c_city, s_city, d_year
//         //order by d_year asc,  revenue desc;
//        //*****************************************************************
//         hype::DeviceConstraint default_device_constraint = CoGaDB::RuntimeConfiguration::instance().getGlobalDeviceConstraint();
//
//         boost::shared_ptr<logical_operator::Logical_Scan> scan_denormalized_ssbm(new logical_operator::Logical_Scan("DENORMALIZED_SSBM_TABLE"));
//         KNF_Selection_Expression knf_expr;
//         {
//             Disjunction d;
//             d.push_back(Predicate("D_YEARMONTH", boost::any(std::string("Dec1997")), ValueConstantPredicate, EQUAL));
//             knf_expr.disjunctions.push_back(d);
//         }
//         {
//             Disjunction d;
//             d.push_back(Predicate("C_CITY", boost::any(std::string("UNITED KI1")), ValueConstantPredicate, EQUAL)); //YEAR<2013
//             d.push_back(Predicate("C_CITY", boost::any(std::string("UNITED KI5")), ValueConstantPredicate, EQUAL)); //Vergleich wert in Spalte mit Konstante
//             knf_expr.disjunctions.push_back(d);
//         }
//         {
//             Disjunction d;
//             d.push_back(Predicate("S_CITY", boost::any(std::string("UNITED KI1")), ValueConstantPredicate, EQUAL)); //YEAR<2013
//             d.push_back(Predicate("S_CITY", boost::any(std::string("UNITED KI5")), ValueConstantPredicate, EQUAL));
//             knf_expr.disjunctions.push_back(d);
//         }
//
//         boost::shared_ptr<logical_operator::Logical_ComplexSelection> complex_selection_on_denormalized_ssbm(new logical_operator::Logical_ComplexSelection(knf_expr, LOOKUP, default_device_constraint)); //hype::DeviceConstraint(hype::CPU_ONLY)))
//
//         std::list<std::string> ordersorting_column_names;
//         ordersorting_column_names.push_back("C_CITY");
//         ordersorting_column_names.push_back("S_CITY");
//         ordersorting_column_names.push_back("D_YEAR");
//         boost::shared_ptr<logical_operator::Logical_Sort> grouporder(new logical_operator::Logical_Sort(ordersorting_column_names, ASCENDING, LOOKUP, hype::DeviceConstraint(hype::CPU_ONLY)));
//
//
//         std::list<std::string> grouping_column_names;
//         grouping_column_names.push_back("C_CITY");
//         grouping_column_names.push_back("S_CITY");
//         grouping_column_names.push_back("D_YEAR");
//         std::list<std::pair<string, AggregationMethod> > aggregation_functions;
//         aggregation_functions.push_back(make_pair("LO_REVENUE", SUM));
//
//         boost::shared_ptr<logical_operator::Logical_Groupby> group(new logical_operator::Logical_Groupby(grouping_column_names, aggregation_functions, LOOKUP, default_device_constraint));
//
//         std::list<std::string> sorting_column_names;
//         sorting_column_names.push_back("D_YEAR");
//         sorting_column_names.push_back("LO_REVENUE");
//         boost::shared_ptr<logical_operator::Logical_Sort> order(new logical_operator::Logical_Sort(sorting_column_names, ASCENDING, LOOKUP, default_device_constraint));
//
//         complex_selection_on_denormalized_ssbm->setLeft(scan_denormalized_ssbm);
//
//         grouporder->setLeft(complex_selection_on_denormalized_ssbm);
//         group->setLeft(grouporder);
//         order->setLeft(group);
//
//         return boost::make_shared<LogicalQueryPlan>(order);
//
//     }
//
//
//     bool Denormalized_SSB_Q34() {
// 	return optimize_execute_print("Denormalized SSB Query 3.4", *Denormalized_SSB_Q34_plan());
//     }
//
//
//
//     LogicalQueryPlanPtr Denormalized_SSB_Q41_plan() {
//         //*****************************************************************
//         //--Q4.1
//         //select d_year, c_nation,
//         //sum(lo_revenue - lo_supplycost) as profit
//         //from dates, customer, supplier, part, lineorder
//         //where lo_custkey = c_custkey
//         //and lo_suppkey = s_suppkey
//         //and lo_partkey = p_partkey
//         //and lo_orderdate = d_datekey
//         //and c_region = 'AMERICA'
//         //and s_region = 'AMERICA'
//         //and (p_mfgr = 'MFGR#1'
//         //or p_mfgr = 'MFGR#2')
//         //group by d_year, c_nation
//         //order by d_year, c_nation;
//         //*****************************************************************
//         hype::DeviceConstraint default_device_constraint = CoGaDB::RuntimeConfiguration::instance().getGlobalDeviceConstraint();
//
//         boost::shared_ptr<logical_operator::Logical_Scan> scan_denormalized_ssbm(new logical_operator::Logical_Scan("DENORMALIZED_SSBM_TABLE"));
//         KNF_Selection_Expression knf_expr;
//
//         {
//             Disjunction d;
//             d.push_back(Predicate("C_REGION", boost::any(std::string("AMERICA")), ValueConstantPredicate, EQUAL));
//             knf_expr.disjunctions.push_back(d);
//         }
//         {
//             Disjunction d;
//             d.push_back(Predicate("S_REGION", boost::any(std::string("AMERICA")), ValueConstantPredicate, EQUAL));
//             knf_expr.disjunctions.push_back(d);
//         }
//         {
//             Disjunction d;
//             d.push_back(Predicate("P_MFGR", boost::any(std::string("MFGR#1")), ValueConstantPredicate, EQUAL)); //YEAR<2013
//             d.push_back(Predicate("P_MFGR", boost::any(std::string("MFGR#2")), ValueConstantPredicate, EQUAL)); //Vergleich wert in Spalte mit Konstante
//             knf_expr.disjunctions.push_back(d);
//         }
//
//         boost::shared_ptr<logical_operator::Logical_ComplexSelection> complex_selection_on_denormalized_ssbm(new logical_operator::Logical_ComplexSelection(knf_expr, LOOKUP, default_device_constraint)); //hype::DeviceConstraint(hype::CPU_ONLY)))
//
//         boost::shared_ptr<logical_operator::Logical_ColumnAlgebraOperator> column_algebra_operation_denormalized_ssbm(new logical_operator::Logical_ColumnAlgebraOperator("LO_REVENUE", "LO_SUPPLYCOST", "PROFIT", SUB, default_device_constraint));
//
//         std::list<std::string> sorting_column_names;
//         sorting_column_names.push_back("D_YEAR");
//         sorting_column_names.push_back("C_NATION");
//         boost::shared_ptr<logical_operator::Logical_Sort> order(new logical_operator::Logical_Sort(sorting_column_names, ASCENDING, LOOKUP, hype::DeviceConstraint(hype::CPU_ONLY)));
//
//         std::list<std::string> grouping_column_names;
//         grouping_column_names.push_back("D_YEAR");
//         grouping_column_names.push_back("C_NATION");
//         std::list<std::pair<string, AggregationMethod> > aggregation_functions;
//         aggregation_functions.push_back(make_pair("PROFIT", SUM));
//         boost::shared_ptr<logical_operator::Logical_Groupby> group(new logical_operator::Logical_Groupby(grouping_column_names, aggregation_functions, LOOKUP, default_device_constraint));
//
//         complex_selection_on_denormalized_ssbm->setLeft(scan_denormalized_ssbm);
//
//         column_algebra_operation_denormalized_ssbm->setLeft(complex_selection_on_denormalized_ssbm);
//         order->setLeft(column_algebra_operation_denormalized_ssbm);
//         group->setLeft(order);
//
//         return boost::make_shared<LogicalQueryPlan>(group);
//
//     }
//
//     bool Denormalized_SSB_Q41() {
// 	return optimize_execute_print("Denormalized SSB Query 4.1", *Denormalized_SSB_Q41_plan());
//     }
//
//
//     LogicalQueryPlanPtr Denormalized_SSB_Q42_plan() {
//         //*****************************************************************
//         //--Q4.2
//         //select d_year, s_nation, p_category,
//         //sum(lo_revenue - lo_supplycost) as profit
//         //from dates, customer, supplier, part, lineorder
//         //where lo_custkey = c_custkey
//         //and lo_suppkey = s_suppkey
//         //and lo_partkey = p_partkey
//         //and lo_orderdate = d_datekey
//         //and c_region = 'AMERICA'
//         //and s_region = 'AMERICA'
//         //and (d_year = 1997 or d_year = 1998)
//         //and (p_mfgr = 'MFGR#1'
//         //or p_mfgr = 'MFGR#2')
//         //group by d_year, s_nation, p_category
//         //order by d_year, s_nation, p_category;
//         //*****************************************************************
//         hype::DeviceConstraint default_device_constraint = CoGaDB::RuntimeConfiguration::instance().getGlobalDeviceConstraint();
//
//         boost::shared_ptr<logical_operator::Logical_Scan> scan_denormalized_ssbm(new logical_operator::Logical_Scan("DENORMALIZED_SSBM_TABLE"));
//         KNF_Selection_Expression knf_expr;
//         {
//             Disjunction d;
//             d.push_back(Predicate("D_YEAR", boost::any(1997), ValueConstantPredicate, EQUAL));
//             d.push_back(Predicate("D_YEAR", boost::any(1998), ValueConstantPredicate, EQUAL));
//             knf_expr.disjunctions.push_back(d);
//         }
//         {
//             Disjunction d;
//             d.push_back(Predicate("C_REGION", boost::any(std::string("AMERICA")), ValueConstantPredicate, EQUAL));
//             knf_expr.disjunctions.push_back(d);
//         }
//         {
//             Disjunction d;
//             d.push_back(Predicate("S_REGION", boost::any(std::string("AMERICA")), ValueConstantPredicate, EQUAL));
//             knf_expr.disjunctions.push_back(d);
//         }
//         {
//             Disjunction d;
//             d.push_back(Predicate("P_MFGR", boost::any(std::string("MFGR#1")), ValueConstantPredicate, EQUAL)); //YEAR<2013
//             d.push_back(Predicate("P_MFGR", boost::any(std::string("MFGR#2")), ValueConstantPredicate, EQUAL)); //Vergleich wert in Spalte mit Konstante
//             knf_expr.disjunctions.push_back(d);
//         }
//
//         boost::shared_ptr<logical_operator::Logical_ComplexSelection> complex_selection_on_denormalized_ssbm(new logical_operator::Logical_ComplexSelection(knf_expr, LOOKUP, default_device_constraint)); //hype::DeviceConstraint(hype::CPU_ONLY)))
//
//         boost::shared_ptr<logical_operator::Logical_ColumnAlgebraOperator> column_algebra_operation_denormalized_ssbm(new logical_operator::Logical_ColumnAlgebraOperator("LO_REVENUE", "LO_SUPPLYCOST", "PROFIT", SUB, default_device_constraint));
//
//         std::list<std::string> sorting_column_names;
//         sorting_column_names.push_back("D_YEAR");
//         sorting_column_names.push_back("S_NATION");
//         sorting_column_names.push_back("P_CATEGORY");
//         boost::shared_ptr<logical_operator::Logical_Sort> order(new logical_operator::Logical_Sort(sorting_column_names, ASCENDING, LOOKUP, hype::DeviceConstraint(hype::CPU_ONLY))); //default_device_constraint));
//
//         std::list<std::string> grouping_column_names;
//         grouping_column_names.push_back("D_YEAR");
//         grouping_column_names.push_back("S_NATION");
//         grouping_column_names.push_back("P_CATEGORY");
//         std::list<std::pair<string, AggregationMethod> > aggregation_functions;
//         aggregation_functions.push_back(make_pair("PROFIT", SUM));
//         boost::shared_ptr<logical_operator::Logical_Groupby> group(new logical_operator::Logical_Groupby(grouping_column_names, aggregation_functions, LOOKUP, default_device_constraint));
//
//         complex_selection_on_denormalized_ssbm->setLeft(scan_denormalized_ssbm);
//
//         column_algebra_operation_denormalized_ssbm->setLeft(complex_selection_on_denormalized_ssbm);
//         order->setLeft(column_algebra_operation_denormalized_ssbm);
//         group->setLeft(order);
//
//         return boost::make_shared<LogicalQueryPlan>(group);
//
//     }
//
//     bool Denormalized_SSB_Q42() {
// 	return optimize_execute_print("Denormalized SSB Query 4.2", *Denormalized_SSB_Q42_plan());
//     }
//
//
//     LogicalQueryPlanPtr Denormalized_SSB_Q43_plan() {
//         //*****************************************************************
//         //--Q4.3
//         //select d_year, s_city, p_brand,
//         //sum(lo_revenue - lo_supplycost) as profit
//         //from dates, customer, supplier, part, lineorder
//         //where lo_custkey = c_custkey
//         //and lo_suppkey = s_suppkey
//         //and lo_partkey = p_partkey
//         //and lo_orderdate = d_datekey
//         //and s_nation = 'UNITED STATES'
//         //and (d_year = 1997 or d_year = 1998)
//         //and p_category = 'MFGR#14'
//         //group by d_year, s_city, p_brand
//         //order by d_year, s_city, p_brand;
//         //*****************************************************************
//         hype::DeviceConstraint default_device_constraint = CoGaDB::RuntimeConfiguration::instance().getGlobalDeviceConstraint();
//
//         boost::shared_ptr<logical_operator::Logical_Scan> scan_denormalized_ssbm(new logical_operator::Logical_Scan("DENORMALIZED_SSBM_TABLE"));
//
//         KNF_Selection_Expression knf_expr;
//         {
//             Disjunction d;
//             d.push_back(Predicate("D_YEAR", boost::any(1997), ValueConstantPredicate, EQUAL));
//             d.push_back(Predicate("D_YEAR", boost::any(1998), ValueConstantPredicate, EQUAL));
//             knf_expr.disjunctions.push_back(d);
//         }
//         {
//             Disjunction d;
//             d.push_back(Predicate("S_NATION", boost::any(std::string("UNITED STATES")), ValueConstantPredicate, EQUAL));
//             knf_expr.disjunctions.push_back(d);
//         }
//         {
//             Disjunction d;
//             d.push_back(Predicate("P_CATEGORY", boost::any(std::string("MFGR#14")), ValueConstantPredicate, EQUAL)); //YEAR<2013
//             knf_expr.disjunctions.push_back(d);
//         }
//
//         boost::shared_ptr<logical_operator::Logical_ComplexSelection> complex_selection_on_denormalized_ssbm(new logical_operator::Logical_ComplexSelection(knf_expr, LOOKUP, default_device_constraint)); //hype::DeviceConstraint(hype::CPU_ONLY)))
//
//
//         boost::shared_ptr<logical_operator::Logical_ColumnAlgebraOperator> column_algebra_operation_denormalized_ssbm(new logical_operator::Logical_ColumnAlgebraOperator("LO_REVENUE", "LO_SUPPLYCOST", "PROFIT", SUB, default_device_constraint));
//
//         std::list<std::string> sorting_column_names;
//         sorting_column_names.push_back("D_YEAR");
//         sorting_column_names.push_back("S_CITY");
//         sorting_column_names.push_back("P_BRAND");
//         boost::shared_ptr<logical_operator::Logical_Sort> order(new logical_operator::Logical_Sort(sorting_column_names, ASCENDING, LOOKUP, hype::DeviceConstraint(hype::CPU_ONLY))); //default_device_constraint));
//
//         std::list<std::string> grouping_column_names;
//         grouping_column_names.push_back("D_YEAR");
//         grouping_column_names.push_back("S_CITY");
//         grouping_column_names.push_back("P_BRAND");
//         std::list<std::pair<string, AggregationMethod> > aggregation_functions;
//         aggregation_functions.push_back(make_pair("PROFIT", SUM));
//         boost::shared_ptr<logical_operator::Logical_Groupby> group(new logical_operator::Logical_Groupby(grouping_column_names, aggregation_functions, LOOKUP, default_device_constraint));
//
//         complex_selection_on_denormalized_ssbm->setLeft(scan_denormalized_ssbm);
//
//         column_algebra_operation_denormalized_ssbm->setLeft(complex_selection_on_denormalized_ssbm);
//         order->setLeft(column_algebra_operation_denormalized_ssbm);
//         group->setLeft(order);
//
//         return boost::make_shared<LogicalQueryPlan>(group);
//     }
//
//     bool Denormalized_SSB_Q43() {
// 	return optimize_execute_print("Denormalized SSB Query 4.3", *Denormalized_SSB_Q43_plan());
//     }
     
    bool Denormalized_SSB_Q11(ClientPtr client) {
        return SQL::commandlineExec("select sum(lo_extendedprice*lo_discount) as revenue from denormalized_ssbm_table where d_year = 1993 and lo_discount between 1 and 3 and lo_quantity < 25;",
                client);
    }

    bool Denormalized_SSB_Q12(ClientPtr client) {
        return SQL::commandlineExec("select sum(lo_extendedprice*lo_discount) as revenue from denormalized_ssbm_table where d_yearmonthnum = 199401 and lo_discount between 4 and 6 and lo_quantity between 26 and 35;",
                client);
    }
    
    bool Denormalized_SSB_Q13(ClientPtr client) {
        return SQL::commandlineExec("select sum(lo_extendedprice*lo_discount) as revenue from denormalized_ssbm_table where d_weeknuminyear = 6 and d_year = 1994 and lo_discount between 5 and 7 and lo_quantity between 26 and 35;",
                client);
    }
    
    bool Denormalized_SSB_Q21(ClientPtr client) {
        return SQL::commandlineExec("select sum(lo_revenue), d_year, p_brand from denormalized_ssbm_table where p_category = 'MFGR#12' and s_region = 'AMERICA' group by d_year, p_brand order by d_year, p_brand;",
                client);
    }
    
    bool Denormalized_SSB_Q22(ClientPtr client) {
        return SQL::commandlineExec("select sum(lo_revenue), d_year, p_brand from denormalized_ssbm_table where p_brand between 'MFGR#2221' and 'MFGR#2228' and s_region = 'ASIA' group by d_year, p_brand order by d_year, p_brand;",
                client);
    }
    
    bool Denormalized_SSB_Q23(ClientPtr client) {
        return SQL::commandlineExec("select sum(lo_revenue), d_year, p_brand from denormalized_ssbm_table where p_brand= 'MFGR#2239' and s_region = 'EUROPE' group by d_year, p_brand order by d_year, p_brand;",
                client);
    }

    bool Denormalized_SSB_Q31(ClientPtr client) {
        return SQL::commandlineExec("select c_nation, s_nation, d_year, sum(lo_revenue) from denormalized_ssbm_table where c_region = 'ASIA' and s_region = 'ASIA' and d_year >= 1992 and d_year <= 1997 group by c_nation, s_nation, d_year order by d_year asc, lo_revenue desc;",
                client);
    }

    bool Denormalized_SSB_Q32(ClientPtr client) {
        return SQL::commandlineExec("select c_city, s_city, d_year, sum(lo_revenue) from denormalized_ssbm_table where c_nation = 'UNITED STATES' and s_nation = 'UNITED STATES' and d_year >= 1992 and d_year <= 1997 group by c_city, s_city, d_year order by d_year asc, lo_revenue desc;",
                client);
    }

    bool Denormalized_SSB_Q33(ClientPtr client) {
        return SQL::commandlineExec("select c_city, s_city, d_year, sum(lo_revenue) from denormalized_ssbm_table where (c_city='UNITED KI1' or c_city='UNITED KI5') and (s_city='UNITED KI1' or s_city='UNITED KI5') and d_year >= 1992 and d_year <= 1997 group by c_city, s_city, d_year order by d_year asc, lo_revenue desc;",
                client);
    }

    bool Denormalized_SSB_Q34(ClientPtr client) {
        return SQL::commandlineExec("select c_city, s_city, d_year, sum(lo_revenue) from denormalized_ssbm_table where (c_city='UNITED KI1' or c_city='UNITED KI5') and (s_city='UNITED KI1' or s_city='UNITED KI5') and d_yearmonth = 'Dec1997' group by c_city, s_city, d_year order by d_year asc, lo_revenue desc;",
                client);
    }

    bool Denormalized_SSB_Q41(ClientPtr client) {
        return SQL::commandlineExec("select d_year, c_nation, sum(lo_revenue - lo_supplycost) as profit from denormalized_ssbm_table where c_region = 'AMERICA' and s_region = 'AMERICA' and (p_mfgr = 'MFGR#1' or p_mfgr = 'MFGR#2') group by d_year, c_nation order by d_year, c_nation;",
                client);
    }

    bool Denormalized_SSB_Q42(ClientPtr client) {
        return SQL::commandlineExec("select d_year, s_nation, p_category, sum(lo_revenue - lo_supplycost) as profit from denormalized_ssbm_table where c_region = 'AMERICA' and s_region = 'AMERICA' and (d_year = 1997 or d_year = 1998) and (p_mfgr = 'MFGR#1' or p_mfgr = 'MFGR#2') group by d_year, s_nation, p_category order by d_year, s_nation, p_category;",
                client);
    }


    bool Denormalized_SSB_Q43(ClientPtr client) {
        return SQL::commandlineExec("select d_year, s_city, p_brand, sum(lo_revenue - lo_supplycost) as profit from denormalized_ssbm_table where s_nation = 'UNITED STATES' and (d_year = 1997 or d_year = 1998) and p_category = 'MFGR#14' group by d_year, s_city, p_brand order by d_year, s_city, p_brand;",
                client);
    }     
     
        
}; //end namespace CogaDB
