#include <iostream>
#include <fstream>

#include <../test/unittests/code_generation_ssbm.hpp>

#include <query_compilation/code_generator.hpp>

#include <stdlib.h>
#include <dlfcn.h>

#include <core/global_definitions.hpp>

#include <persistence/storage_manager.hpp>
#include <query_compilation/code_generators/cpp_code_generator.hpp>

#include <core/selection_expression.hpp>
#include <util/time_measurement.hpp>
#include <parser/commandline_interpreter.hpp>

#include <util/getname.hpp>
#include <boost/make_shared.hpp>
#include <iomanip>

#include <boost/program_options.hpp>

#include "core/runtime_configuration.hpp"
#include "core/variable_manager.hpp"

namespace CoGaDB{
    
    struct CompilerStatistics{
        CompilerStatistics();
        double compile_time_in_sec;
        double execution_time_in_sec; 
        
        std::string toString() const;
    };
    
    CompilerStatistics::CompilerStatistics() 
    : compile_time_in_sec(0), execution_time_in_sec(0)
    {
        
    }
    
    std::string CompilerStatistics::toString() const{
        std::stringstream ss;
        ss << "Compilation Time: " << compile_time_in_sec << "s" << std::endl;
        ss << "Execution Time: " << execution_time_in_sec << "s" << std::endl;
        ss << "Total Time: " << compile_time_in_sec+execution_time_in_sec << "s"; 
        return ss.str();
    }
    
    
    bool execute(PipelinePtr pipeline, CompilerStatistics& stats) {
        if (!pipeline) return false;
        bool ret = pipeline->execute();
        if(ret){
            stats.compile_time_in_sec+=pipeline->getCompileTimeSec();
            stats.execution_time_in_sec+=pipeline->getExecutionTimeSec();  
        }
        return ret;
    }

    TablePtr compiled_SSBM_Q11(CodeGeneratorType code_generator, CompilerStatistics& stats) {

        /*
        GROUPBY () USING (SUM((LO_EXTENDEDPRICE*LO_DISCOUNT)))	[CPU_ONLY]
                ColumnAlgebraOperator ((LO_EXTENDEDPRICE*LO_DISCOUNT)=MUL(LO_EXTENDEDPRICE,LO_DISCOUNT))	[CPU_ONLY]
                        COMPLEX_SELECTION(D_YEAR=1993) AND (LO_DISCOUNT>=1) AND (LO_DISCOUNT<=3) AND (LO_QUANTITY<25)	[CPU_ONLY]
                                JOIN (LO_ORDERDATE=D_DATEKEY)	[CPU_ONLY]
                                        SCAN LINEORDER
                                        SCAN DATES
        */

        PipelinePtr pipeline;

        { /*
           * build pipeline for DATES table
           */
            
            /* projection parameters */
            ProjectionParam param;
            param.push_back(AttributeReference(getTablebyName("DATES"), "D_DATEKEY", "D_DATEKEY"));

            CodeGeneratorPtr code_gen = createCodeGenerator(code_generator,
                    param, getTablebyName("DATES"));
            /* input attribute references */
            AttributeReferencePtr d_datekey = boost::make_shared<AttributeReference>(
                    getTablebyName("DATES"), "D_DATEKEY", "D_DATEKEY");
            AttributeReferencePtr d_year = boost::make_shared<AttributeReference>(
                    getTablebyName("DATES"), "D_YEAR", "D_YEAR");

            PredicateExpressionPtr selection_expr = createColumnConstantComparisonPredicateExpression(d_year, boost::any(int(1993)), EQUAL);

            if (!code_gen->consumeSelection(selection_expr))
                COGADB_FATAL_ERROR("", "");
            if (!code_gen->consumeBuildHashTable(*d_datekey))
                COGADB_FATAL_ERROR("", "");
            pipeline = code_gen->compile();

            if (!execute(pipeline, stats)) {
                std::cerr << "Could not execute query successfully!" << std::endl;
                return TablePtr();
            }
        }

        TablePtr result = pipeline->getResult();
        assert(result->getHashTablebyName("D_DATEKEY") != NULL);

        { /*
           * probe pipeline for LINEORDER table
           */
            ProjectionParam param;

            CodeGeneratorPtr code_gen = createCodeGenerator(code_generator,
                    param, getTablebyName("LINEORDER"));

            AttributeReferencePtr lo_extendedprice = boost::make_shared<AttributeReference>(
                    getTablebyName("LINEORDER"), "LO_EXTENDEDPRICE", "LO_EXTENDEDPRICE");
            AttributeReferencePtr lo_discount = boost::make_shared<AttributeReference>(
                    getTablebyName("LINEORDER"), "LO_DISCOUNT", "LO_DISCOUNT");
            AttributeReferencePtr lo_quantity = boost::make_shared<AttributeReference>(
                    getTablebyName("LINEORDER"), "LO_QUANTITY", "LO_QUANTITY");
            AttributeReferencePtr lo_orderdate = boost::make_shared<AttributeReference>(
                    getTablebyName("LINEORDER"), "LO_ORDERDATE", "LO_ORDERDATE");
            //        AttributeReferencePtr lo_ = boost::make_shared<AttributeReference>(
            //                getTablebyName("LINEORDER"), "", ""); 

            /* COMPLEX_SELECTION(D_YEAR=1993) AND (LO_DISCOUNT>=1) AND (LO_DISCOUNT<=3) AND (LO_QUANTITY<25) */
            std::vector<PredicateExpressionPtr> conjunctions;
            conjunctions.push_back(createColumnConstantComparisonPredicateExpression(lo_discount, boost::any(int(1)), GREATER_EQUAL));
            conjunctions.push_back(createColumnConstantComparisonPredicateExpression(lo_discount, boost::any(int(3)), LESSER_EQUAL));
            conjunctions.push_back(createColumnConstantComparisonPredicateExpression(lo_quantity, boost::any(int(25)), LESSER));
            PredicateExpressionPtr selection_expr = createPredicateExpression(conjunctions, LOGICAL_AND);
            if (!code_gen->consumeSelection(selection_expr))
                COGADB_FATAL_ERROR("", "");
            /* JOIN (LO_ORDERDATE=D_DATEKEY)	[CPU_ONLY] */
            if (!code_gen->consumeProbeHashTable(
                    AttributeReference(result, "D_DATEKEY"),
                    *lo_orderdate))
                COGADB_FATAL_ERROR("", "");

            /* ColumnAlgebraOperator ((LO_EXTENDEDPRICE*LO_DISCOUNT)=MUL(LO_EXTENDEDPRICE,LO_DISCOUNT))	[CPU_ONLY] */
            std::pair<bool, AttributeReference> algebra_ret = code_gen->consumeAlgebraComputation(
                    *lo_extendedprice,
                    *lo_discount,
                    MUL
                    );
            if (!algebra_ret.first) {
                COGADB_FATAL_ERROR("", "");
            }
            AttributeReference extended_price_mul_discount = algebra_ret.second;

            /* GROUPBY () USING (SUM((LO_EXTENDEDPRICE*LO_DISCOUNT)))	[CPU_ONLY] */
            AggregateSpecifications agg_specs;
            agg_specs.push_back(createAggregateSpecification(extended_price_mul_discount, SUM, "SUM_EXTENDEDPRICE"));

            if (!code_gen->consumeAggregate(agg_specs))
                COGADB_FATAL_ERROR("", "");

            pipeline = code_gen->compile();
            if (!execute(pipeline, stats)) {
                std::cerr << "Could not execute query successfully!" << std::endl;
                return TablePtr();
            }
        }
        result = pipeline->getResult();
        return result;
    }

    TablePtr compiled_SSBM_Q12(CodeGeneratorType code_generator, CompilerStatistics& stats) {
    /*
    GROUPBY () USING (SUM((LO_EXTENDEDPRICE*LO_DISCOUNT)))	[CPU_ONLY]
        ColumnAlgebraOperator ((LO_EXTENDEDPRICE*LO_DISCOUNT)=MUL(LO_EXTENDEDPRICE,LO_DISCOUNT))	[CPU_ONLY]
            JOIN (LO_ORDERDATE=D_DATEKEY)	[CPU_ONLY]
                COMPLEX_SELECTION(LO_QUANTITY<=35)	[CPU_ONLY]
                    COMPLEX_SELECTION(LO_QUANTITY>=26)	[CPU_ONLY]
                        COMPLEX_SELECTION(LO_DISCOUNT<=6)	[CPU_ONLY]
                            COMPLEX_SELECTION(LO_DISCOUNT>=4)	[CPU_ONLY]
                                SCAN LINEORDER
                COMPLEX_SELECTION(D_YEARMONTHNUM=199401)	[CPU_ONLY]
                    SCAN DATES
    */
        PipelinePtr pipeline;

        { /*
           * build pipeline for DATES table
           */
            
            /* projection parameters */
            ProjectionParam param;
            param.push_back(AttributeReference(getTablebyName("DATES"), "D_DATEKEY", "D_DATEKEY"));

            CodeGeneratorPtr code_gen = createCodeGenerator(code_generator,
                    param, getTablebyName("DATES"));
            /* input attribute references */
            AttributeReferencePtr d_datekey = boost::make_shared<AttributeReference>(
                    getTablebyName("DATES"), "D_DATEKEY", "D_DATEKEY");
            AttributeReferencePtr d_yearmonthnum = boost::make_shared<AttributeReference>(
                    getTablebyName("DATES"), "D_YEARMONTHNUM", "D_YEARMONTHNUM");

            PredicateExpressionPtr selection_expr = createColumnConstantComparisonPredicateExpression(d_yearmonthnum, boost::any(int(199401)), EQUAL);
            /*    COMPLEX_SELECTION(D_YEARMONTHNUM=199401)	[CPU_ONLY] */
            if (!code_gen->consumeSelection(selection_expr))
                COGADB_FATAL_ERROR("", "");
            if (!code_gen->consumeBuildHashTable(*d_datekey))
                COGADB_FATAL_ERROR("", "");
            pipeline = code_gen->compile();

            if (!execute(pipeline, stats)) {
                std::cerr << "Could not execute query successfully!" << std::endl;
                return TablePtr();
            }
        }

        TablePtr result = pipeline->getResult();
        assert(result->getHashTablebyName("D_DATEKEY") != NULL);

        { /*
           * probe pipeline for LINEORDER table
           */
            ProjectionParam param;

            CodeGeneratorPtr code_gen = createCodeGenerator(code_generator,
                    param, getTablebyName("LINEORDER"));

            AttributeReferencePtr lo_extendedprice = boost::make_shared<AttributeReference>(
                    getTablebyName("LINEORDER"), "LO_EXTENDEDPRICE", "LO_EXTENDEDPRICE");
            AttributeReferencePtr lo_discount = boost::make_shared<AttributeReference>(
                    getTablebyName("LINEORDER"), "LO_DISCOUNT", "LO_DISCOUNT");
            AttributeReferencePtr lo_quantity = boost::make_shared<AttributeReference>(
                    getTablebyName("LINEORDER"), "LO_QUANTITY", "LO_QUANTITY");
            AttributeReferencePtr lo_orderdate = boost::make_shared<AttributeReference>(
                    getTablebyName("LINEORDER"), "LO_ORDERDATE", "LO_ORDERDATE");
            //        AttributeReferencePtr lo_ = boost::make_shared<AttributeReference>(
            //                getTablebyName("LINEORDER"), "", ""); 

            /*  COMPLEX_SELECTION(LO_QUANTITY<=35)	[CPU_ONLY]
                    COMPLEX_SELECTION(LO_QUANTITY>=26)	[CPU_ONLY]
                        COMPLEX_SELECTION(LO_DISCOUNT<=6)	[CPU_ONLY]
                            COMPLEX_SELECTION(LO_DISCOUNT>=4)	[CPU_ONLY] */
            std::vector<PredicateExpressionPtr> conjunctions;
            conjunctions.push_back(createColumnConstantComparisonPredicateExpression(lo_discount, boost::any(int(4)), GREATER_EQUAL));
            conjunctions.push_back(createColumnConstantComparisonPredicateExpression(lo_discount, boost::any(int(6)), LESSER_EQUAL));
            conjunctions.push_back(createColumnConstantComparisonPredicateExpression(lo_quantity, boost::any(int(26)), GREATER_EQUAL));
            conjunctions.push_back(createColumnConstantComparisonPredicateExpression(lo_quantity, boost::any(int(35)), LESSER_EQUAL));
            PredicateExpressionPtr selection_expr = createPredicateExpression(conjunctions, LOGICAL_AND);
            if (!code_gen->consumeSelection(selection_expr))
                COGADB_FATAL_ERROR("", "");
            /* JOIN (LO_ORDERDATE=D_DATEKEY)	[CPU_ONLY] */
            if (!code_gen->consumeProbeHashTable(
                    AttributeReference(result, "D_DATEKEY"),
                    *lo_orderdate))
                COGADB_FATAL_ERROR("", "");

            /* ColumnAlgebraOperator ((LO_EXTENDEDPRICE*LO_DISCOUNT)=MUL(LO_EXTENDEDPRICE,LO_DISCOUNT))	[CPU_ONLY] */
            std::pair<bool, AttributeReference> algebra_ret = code_gen->consumeAlgebraComputation(
                    *lo_extendedprice,
                    *lo_discount,
                    MUL
                    );
            if (!algebra_ret.first) {
                COGADB_FATAL_ERROR("", "");
            }
            AttributeReference extended_price_mul_discount = algebra_ret.second;

            /* GROUPBY () USING (SUM((LO_EXTENDEDPRICE*LO_DISCOUNT)))	[CPU_ONLY] */
            AggregateSpecifications agg_specs;
            agg_specs.push_back(createAggregateSpecification(extended_price_mul_discount, SUM, "SUM_EXTENDEDPRICE"));

            if (!code_gen->consumeAggregate(agg_specs))
                COGADB_FATAL_ERROR("", "");

            pipeline = code_gen->compile();
            if (!execute(pipeline, stats)) {
                std::cerr << "Could not execute query successfully!" << std::endl;
                return TablePtr();
            }
        }
        result = pipeline->getResult();
        return result;
    }

    TablePtr compiled_SSBM_Q13(CodeGeneratorType code_generator, CompilerStatistics& stats) {
        /* 
        GROUPBY () USING (SUM((LO_EXTENDEDPRICE*LO_DISCOUNT)))	[CPU_ONLY]
            ColumnAlgebraOperator ((LO_EXTENDEDPRICE*LO_DISCOUNT)=MUL(LO_EXTENDEDPRICE,LO_DISCOUNT))	[CPU_ONLY]
                JOIN (LO_ORDERDATE=D_DATEKEY)	[CPU_ONLY]
                    COMPLEX_SELECTION(LO_QUANTITY<=35)	[CPU_ONLY]
                        COMPLEX_SELECTION(LO_QUANTITY>=26)	[CPU_ONLY]
                            COMPLEX_SELECTION(LO_DISCOUNT<=7)	[CPU_ONLY]
                                COMPLEX_SELECTION(LO_DISCOUNT>=5)	[CPU_ONLY]
                                    SCAN LINEORDER
                    COMPLEX_SELECTION(D_YEAR=1994)	[CPU_ONLY]
                        COMPLEX_SELECTION(D_WEEKNUMINYEAR=6)	[CPU_ONLY]
                            SCAN DATES
        */
        PipelinePtr pipeline;

        { /*
           * build pipeline for DATES table
           */
            
            /* projection parameters */
            ProjectionParam param;
            param.push_back(AttributeReference(getTablebyName("DATES"), "D_DATEKEY", "D_DATEKEY"));

            CodeGeneratorPtr code_gen = createCodeGenerator(code_generator,
                    param, getTablebyName("DATES"));
            /* input attribute references */
            AttributeReferencePtr d_datekey = boost::make_shared<AttributeReference>(
                    getTablebyName("DATES"), "D_DATEKEY", "D_DATEKEY");
            AttributeReferencePtr d_year = boost::make_shared<AttributeReference>(
                    getTablebyName("DATES"), "D_YEAR", "D_YEAR");
            AttributeReferencePtr d_weeknuminyear = boost::make_shared<AttributeReference>(
                    getTablebyName("DATES"), "D_WEEKNUMINYEAR", "D_WEEKNUMINYEAR");
            /*
                COMPLEX_SELECTION(D_YEAR=1994)	[CPU_ONLY]
                    COMPLEX_SELECTION(D_WEEKNUMINYEAR=6)	[CPU_ONLY]
            */
            std::vector<PredicateExpressionPtr> conjunctions;
            conjunctions.push_back(createColumnConstantComparisonPredicateExpression(d_year, boost::any(int(1994)), EQUAL));
            conjunctions.push_back(createColumnConstantComparisonPredicateExpression(d_weeknuminyear, boost::any(int(6)), EQUAL));        
            PredicateExpressionPtr selection_expr = createPredicateExpression(conjunctions, LOGICAL_AND);
            
            if (!code_gen->consumeSelection(selection_expr))
                COGADB_FATAL_ERROR("", "");
            if (!code_gen->consumeBuildHashTable(*d_datekey))
                COGADB_FATAL_ERROR("", "");
            pipeline = code_gen->compile();

            if (!execute(pipeline, stats)) {
                std::cerr << "Could not execute query successfully!" << std::endl;
                return TablePtr();
            }
        }

        TablePtr result = pipeline->getResult();
        assert(result->getHashTablebyName("D_DATEKEY") != NULL);

        { /*
           * probe pipeline for LINEORDER table
           */
            ProjectionParam param;

            CodeGeneratorPtr code_gen = createCodeGenerator(code_generator,
                    param, getTablebyName("LINEORDER"));

            AttributeReferencePtr lo_extendedprice = boost::make_shared<AttributeReference>(
                    getTablebyName("LINEORDER"), "LO_EXTENDEDPRICE", "LO_EXTENDEDPRICE");
            AttributeReferencePtr lo_discount = boost::make_shared<AttributeReference>(
                    getTablebyName("LINEORDER"), "LO_DISCOUNT", "LO_DISCOUNT");
            AttributeReferencePtr lo_quantity = boost::make_shared<AttributeReference>(
                    getTablebyName("LINEORDER"), "LO_QUANTITY", "LO_QUANTITY");
            AttributeReferencePtr lo_orderdate = boost::make_shared<AttributeReference>(
                    getTablebyName("LINEORDER"), "LO_ORDERDATE", "LO_ORDERDATE");
            //        AttributeReferencePtr lo_ = boost::make_shared<AttributeReference>(
            //                getTablebyName("LINEORDER"), "", ""); 

            /*
            COMPLEX_SELECTION(LO_QUANTITY<=35)	[CPU_ONLY]
                COMPLEX_SELECTION(LO_QUANTITY>=26)	[CPU_ONLY]
                    COMPLEX_SELECTION(LO_DISCOUNT<=7)	[CPU_ONLY]
                        COMPLEX_SELECTION(LO_DISCOUNT>=5)	[CPU_ONLY]
            */
            std::vector<PredicateExpressionPtr> conjunctions;
            conjunctions.push_back(createColumnConstantComparisonPredicateExpression(lo_discount, boost::any(int(5)), GREATER_EQUAL));
            conjunctions.push_back(createColumnConstantComparisonPredicateExpression(lo_discount, boost::any(int(7)), LESSER_EQUAL));
            conjunctions.push_back(createColumnConstantComparisonPredicateExpression(lo_quantity, boost::any(int(26)), GREATER_EQUAL));
            conjunctions.push_back(createColumnConstantComparisonPredicateExpression(lo_quantity, boost::any(int(35)), LESSER_EQUAL));
            PredicateExpressionPtr selection_expr = createPredicateExpression(conjunctions, LOGICAL_AND);
            if (!code_gen->consumeSelection(selection_expr))
                COGADB_FATAL_ERROR("", "");
            /* JOIN (LO_ORDERDATE=D_DATEKEY)	[CPU_ONLY] */
            if (!code_gen->consumeProbeHashTable(
                    AttributeReference(result, "D_DATEKEY"),
                    *lo_orderdate))
                COGADB_FATAL_ERROR("", "");

            /* ColumnAlgebraOperator ((LO_EXTENDEDPRICE*LO_DISCOUNT)=MUL(LO_EXTENDEDPRICE,LO_DISCOUNT))	[CPU_ONLY] */
            std::pair<bool, AttributeReference> algebra_ret = code_gen->consumeAlgebraComputation(
                    *lo_extendedprice,
                    *lo_discount,
                    MUL
                    );
            if (!algebra_ret.first) {
                COGADB_FATAL_ERROR("", "");
            }
            AttributeReference extended_price_mul_discount = algebra_ret.second;

            /* GROUPBY () USING (SUM((LO_EXTENDEDPRICE*LO_DISCOUNT)))	[CPU_ONLY] */
            AggregateSpecifications agg_specs;
            agg_specs.push_back(createAggregateSpecification(extended_price_mul_discount, SUM, "SUM_EXTENDEDPRICE"));

            if (!code_gen->consumeAggregate(agg_specs))
                COGADB_FATAL_ERROR("", "");

            pipeline = code_gen->compile();
            if (!execute(pipeline, stats)) {
                std::cerr << "Could not execute query successfully!" << std::endl;
                return TablePtr();
            }
        }
        result = pipeline->getResult();
        return result;
    }

    TablePtr compiled_SSBM_Q21(CodeGeneratorType code_generator, CompilerStatistics& stats) {
        return TablePtr();
    }

    TablePtr compiled_SSBM_Q22(CodeGeneratorType code_generator, CompilerStatistics& stats) {
        return TablePtr();
    }

    TablePtr compiled_SSBM_Q23(CodeGeneratorType code_generator, CompilerStatistics& stats) {
        return TablePtr();
    }

    TablePtr compiled_SSBM_Q31(CodeGeneratorType code_generator, CompilerStatistics& stats) {
        return TablePtr();
    }

    TablePtr compiled_SSBM_Q32(CodeGeneratorType code_generator, CompilerStatistics& stats) {
        return TablePtr();
    }

    TablePtr compiled_SSBM_Q33(CodeGeneratorType code_generator, CompilerStatistics& stats) {
        return TablePtr();
    }

    TablePtr compiled_SSBM_Q34(CodeGeneratorType code_generator, CompilerStatistics& stats) {
        return TablePtr();
    }

    TablePtr compiled_SSBM_Q41(CodeGeneratorType code_generator, CompilerStatistics& stats) {
        return TablePtr();
    }

    TablePtr compiled_SSBM_Q42(CodeGeneratorType code_generator, CompilerStatistics& stats) {
        return TablePtr();
    }

    TablePtr compiled_SSBM_Q43(CodeGeneratorType code_generator, CompilerStatistics& stats) {
        return TablePtr();
    }

    typedef TablePtr (*compiledQuerySSBM)(CodeGeneratorType code_generator, CompilerStatistics& stats);
    
    bool execute_compiled_SSB_query(const std::string& query_name, ClientPtr client){
        if(!client) return false;
        std::ostream& out = client->getOutputStream();
        CompilerStatistics stats;
        
        typedef std::map<std::string,compiledQuerySSBM> Map;
        Map map;
        map.insert(std::make_pair("SSBM_Q11",&compiled_SSBM_Q11));
        map.insert(std::make_pair("SSBM_Q12",&compiled_SSBM_Q12));   
        map.insert(std::make_pair("SSBM_Q13",&compiled_SSBM_Q13));    
        map.insert(std::make_pair("SSBM_Q21",&compiled_SSBM_Q21));
        map.insert(std::make_pair("SSBM_Q22",&compiled_SSBM_Q22));   
        map.insert(std::make_pair("SSBM_Q23",&compiled_SSBM_Q23)); 
        map.insert(std::make_pair("SSBM_Q31",&compiled_SSBM_Q31));
        map.insert(std::make_pair("SSBM_Q32",&compiled_SSBM_Q32));   
        map.insert(std::make_pair("SSBM_Q33",&compiled_SSBM_Q33));  
        map.insert(std::make_pair("SSBM_Q34",&compiled_SSBM_Q34));
        map.insert(std::make_pair("SSBM_Q41",&compiled_SSBM_Q41));   
        map.insert(std::make_pair("SSBM_Q42",&compiled_SSBM_Q42)); 
        map.insert(std::make_pair("SSBM_Q43",&compiled_SSBM_Q43));

        Map::const_iterator cit = map.find(query_name);
        if(cit==map.end()){
            COGADB_WARNING("Unknown query '" << query_name << "'","");
            return false;
        }
        std::string name_of_code_generator = VariableManager::instance().getVariableValueString("default_code_generator");
        CodeGeneratorType code_generator;
        if(name_of_code_generator=="cpp"){
            code_generator = CPP_CODE_GENERATOR;
        }else if(name_of_code_generator=="cuda") {
            code_generator = CUDA_C_CODE_GENERATOR;
        } else {    
            COGADB_FATAL_ERROR("","");
        }
                
        TablePtr result = (*cit->second)(code_generator, stats);
        if(!result){
            return false;
        }
        out << result->toString() << std::endl;
        out << stats.toString() << std::endl;
        return true;
    }
    
    bool SSB_Q11_compiled(ClientPtr client){
        return execute_compiled_SSB_query("SSBM_Q11", client);
    }
    
    bool SSB_Q12_compiled(ClientPtr client){
        return execute_compiled_SSB_query("SSBM_Q12", client);
    }

    bool SSB_Q13_compiled(ClientPtr client) {
        return execute_compiled_SSB_query("SSBM_Q13", client);
    }

    bool SSB_Q21_compiled(ClientPtr client) {
        return false;
    }

    bool SSB_Q22_compiled(ClientPtr client) {
        return false;
    }

    bool SSB_Q23_compiled(ClientPtr client) {
        return false;
    }

    bool SSB_Q31_compiled(ClientPtr client) {
        return false;
    }

    bool SSB_Q32_compiled(ClientPtr client) {
        return false;
    }

    bool SSB_Q33_compiled(ClientPtr client) {
        return false;
    }

    bool SSB_Q34_compiled(ClientPtr client) {
        return false;
    }

    bool SSB_Q41_compiled(ClientPtr client) {
        return false;
    }

    bool SSB_Q42_compiled(ClientPtr client) {
        return false;
    }

    bool SSB_Q43_compiled(ClientPtr client) {
        return false;
    }  

}; //end namespace CoGaDB
