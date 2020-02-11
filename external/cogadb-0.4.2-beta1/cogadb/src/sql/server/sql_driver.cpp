#include <stdio.h>
#include <string>
#include <iostream>
#include <exception>

#include <boost/iostreams/device/file_descriptor.hpp>
#include <boost/iostreams/stream.hpp>

#include "sql/server/sql_parsetree.hpp"
#include "sql/server/sql_driver.hpp"
#include "sql_parser.hpp"
#include "core/variable_manager.hpp"

#include <optimizer/optimizer.hpp>

//#define COGADB_MEASURE_ENERGY_CONSUMPTION
#ifdef COGADB_MEASURE_ENERGY_CONSUMPTION
    #include <IntelPerformanceCounterMonitorV2.7/cpucounters.h>
#endif

#define NDEBUG

using namespace boost;

namespace CoGaDB {
namespace SQL {

Driver::Driver()
	: buffer(NULL), istream(NULL), parser(*this, scanner)
{
	init_scan();
//	parser.set_debug_level(1);
#ifndef NDEBUG
	parser.set_debug_level(1);
#endif
}

ParseTree::SequencePtr
Driver::parse(std::istream &is)
{
	set_input(is);
	if (parser.parse())
		throw ParseError();

	return result;
}

ParseTree::SequencePtr
Driver::parse(const std::string &src)
{
	set_input(src);
	if (parser.parse())
		throw ParseError();

	return result;
}

Driver::~Driver()
{
	destroy_scan();
}

bool
commandlineExec(const std::string &input, ClientPtr client)
{
    	std::ostream& out = client->getOutputStream();
	Timestamp begin = getTimestamp();
        
#ifdef COGADB_MEASURE_ENERGY_CONSUMPTION
        PCM* pcm = PCM::getInstance();      

        SystemCounterState systemState_before;
        SystemCounterState systemState_after;


        systemState_before = pcm->getSystemCounterState(); 

#endif
        
        TablePtr result = executeSQL(input, client);
        
	Timestamp end = getTimestamp();
       
#ifdef COGADB_MEASURE_ENERGY_CONSUMPTION
        systemState_after = pcm->getSystemCounterState();
#endif
	assert(end >= begin);
        if(!result){
            out << "Error: Invalid Result Table (TablePtr is NULL)" << std::endl;
            return false;
        }
//	if (result)
//		result->print();
        bool print_query_result = VariableManager::instance().getVariableValueBoolean("print_query_result");        
        if(print_query_result)
            out << result->toString(); // << std::endl;
        //std::streamsize precision = std::cout.precision();
        //std::cout.precision(3);
        //std::cout << "Execution Time: "; // << double(end-begin)/(1000*1000) << "ms" << std::endl;
        //std::cout.precision(precision);
	if(VariableManager::instance().getVariableValueString("result_output_format")=="table"){
        	std::stringstream time_str;
        	time_str.precision(5);
        	time_str << double(end-begin)/(1000*1000);
        	out << std::endl << "Execution Time: " << time_str.str() << " ms" << std::endl;
	}
        
#ifdef COGADB_MEASURE_ENERGY_CONSUMPTION
        double consumed_joules_processor = getConsumedJoules(systemState_before,systemState_after);
        double consumed_joules_dram = getDRAMConsumedJoules(systemState_before,systemState_after);
        double total_joules_for_query = consumed_joules_processor + consumed_joules_dram;

        out << "Consumed Joules (CPU): " << consumed_joules_processor  << std::endl;
        out << "Consumed Joules (DRAM): " << consumed_joules_dram  << std::endl;
        out << "Consumed Joules (Total): " << total_joules_for_query  << std::endl;
        out << "Energy Product (Joule*seconds): " << total_joules_for_query*double(end-begin)/(1000*1000*1000)  << std::endl;
#endif
        
        //printf("Execution Time: %.5lf ms\n", double(end-begin)/(1000*1000));
//        double total_delay_time = plan->getTotalSchedulingDelay();
//        out << "Total Scheduling Delay Time: " << total_delay_time << "ms" << std::endl;
//        StatisticsManager::instance().addToValue("TOTAL_LOST_TIME_DUE_TO_DELAYED_SCHEDULING_IN_SECONDS",total_delay_time/(1000*1000*1000)); 
        //std::cout << std::endl;
	return true;
}

TablePtr 
executeSQL(const std::string &input, ClientPtr client) {
       
    Driver driver;
    ParseTree::SequencePtr seq;
    TablePtr result;

    std::ostream& out = client->getOutputStream();

    try {
            seq = driver.parse(input);
            result = seq->execute(client);
            
    } catch (const std::exception &e) {
            out << e.what() << std::endl;
            return TablePtr();
    }

    return result;
    
}

bool
commandlineExplain(const std::string &input, ClientPtr client)
{
        //mark as unused
        (void) client;
	Driver driver;
	ParseTree::SequencePtr seq;

	try {
		seq = driver.parse(input);
	} catch (const std::exception &e) {
		std::cerr << e.what() << std::endl;
		return false;
	}

	/*
	 * FIXME: this only works on POSIX
	 */
	FILE *pipe = popen("dot -Tgtk", "w");
	if (!pipe) {
		perror("Error opening `dot`");
		return false;
	}
	iostreams::file_descriptor_sink fd(fileno(pipe), iostreams::close_handle);
	iostreams::stream<iostreams::file_descriptor_sink> stream(fd);

	seq->explain(stream);
        
	pclose(pipe);
	return true;
}

bool commandlineExplainStatements(const std::string &input, ClientPtr client, bool optimize){
 
	std::ostream& out = client->getOutputStream();
	Driver driver;
	ParseTree::SequencePtr seq;

	try {
		seq = driver.parse(input);
	} catch (const std::exception &e) {
		std::cerr << e.what() << std::endl;
		return false;
	}
        std::list<CoGaDB::query_processing::LogicalQueryPlanPtr> plans = seq->getLogicalQueryPlans();
        std::list<CoGaDB::query_processing::LogicalQueryPlanPtr>::iterator it;
        unsigned int i=0;
        for(it=plans.begin();it!=plans.end();++it){
            if(plans.size()>1)
                 out << "Query Plan for Statement " << i++ <<" : " << std::endl;
            query_processing::LogicalQueryPlanPtr plan=*it;
            if(plan){
                plan->setOutputStream(out);
                if(!optimizer::checkQueryPlan(plan->getRoot())){
                    client->getOutputStream() << "Query Compilation Failed!" << std::endl;
                    return false;
                }
                if(optimize){
                    optimizer::Logical_Optimizer::instance().optimize(plan);
                }
                plan->print();
            }    
        }
        return true;
}

bool commandlineExplainStatementsWithoutOptimization(const std::string &input, ClientPtr client){
    return commandlineExplainStatements(input, client, false);
}

bool commandlineExplainStatementsWithOptimization(const std::string &input, ClientPtr client){
    return commandlineExplainStatements(input, client ,true);
}


} /* namespace SQL */
} /* namespace CoGaDB */
