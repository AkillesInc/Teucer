/* 
 * File:   code_generation_ssbm.hpp
 * Author: sebastian
 *
 * Created on 7. November 2015, 10:09
 */

#ifndef CODE_GENERATION_SSBM_HPP
#define	CODE_GENERATION_SSBM_HPP

#include <query_compilation/code_generator.hpp>

namespace CoGaDB {

    class BaseTable;
    typedef boost::shared_ptr<BaseTable> TablePtr;
    class Client;
    typedef boost::shared_ptr<Client> ClientPtr;
    class Pipeline;
    typedef boost::shared_ptr<Pipeline> PipelinePtr;
    class CompilerStatistics;

    bool execute(PipelinePtr);

    TablePtr compiled_SSBM_Q11(CodeGeneratorType code_generator, CompilerStatistics& stats);
    TablePtr compiled_SSBM_Q12(CodeGeneratorType code_generator, CompilerStatistics& stats);
    TablePtr compiled_SSBM_Q13(CodeGeneratorType code_generator, CompilerStatistics& stats);
    TablePtr compiled_SSBM_Q21(CodeGeneratorType code_generator, CompilerStatistics& stats);
    TablePtr compiled_SSBM_Q22(CodeGeneratorType code_generator, CompilerStatistics& stats);
    TablePtr compiled_SSBM_Q23(CodeGeneratorType code_generator, CompilerStatistics& stats);
    TablePtr compiled_SSBM_Q31(CodeGeneratorType code_generator, CompilerStatistics& stats);
    TablePtr compiled_SSBM_Q32(CodeGeneratorType code_generator, CompilerStatistics& stats);
    TablePtr compiled_SSBM_Q33(CodeGeneratorType code_generator, CompilerStatistics& stats);
    TablePtr compiled_SSBM_Q34(CodeGeneratorType code_generator, CompilerStatistics& stats);
    TablePtr compiled_SSBM_Q41(CodeGeneratorType code_generator, CompilerStatistics& stats);    
    TablePtr compiled_SSBM_Q42(CodeGeneratorType code_generator, CompilerStatistics& stats);  
    TablePtr compiled_SSBM_Q43(CodeGeneratorType code_generator, CompilerStatistics& stats);  

    bool SSB_Q11_compiled(ClientPtr);
    bool SSB_Q12_compiled(ClientPtr);
    bool SSB_Q13_compiled(ClientPtr);
    bool SSB_Q21_compiled(ClientPtr);
    bool SSB_Q22_compiled(ClientPtr);
    bool SSB_Q23_compiled(ClientPtr);
    bool SSB_Q31_compiled(ClientPtr);
    bool SSB_Q32_compiled(ClientPtr);
    bool SSB_Q33_compiled(ClientPtr);
    bool SSB_Q34_compiled(ClientPtr);
    bool SSB_Q41_compiled(ClientPtr);
    bool SSB_Q42_compiled(ClientPtr);
    bool SSB_Q43_compiled(ClientPtr);   
    
    
}; //end namespace CoGaDB

#endif	/* CODE_GENERATION_SSBM_HPP */

