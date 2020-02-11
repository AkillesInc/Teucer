
#include <core/table.hpp>
#include <lookup_table/lookup_table.hpp>
#include <core/lookup_array.hpp>
#include <iomanip>
#include <boost/smart_ptr/make_shared.hpp>

#include "util/getname.hpp"

using namespace std;

namespace CoGaDB {

    LookupTable::LookupTable(const std::string& name, const TableSchema& schema, const std::vector<LookupColumnPtr>& lookup_columns, const std::vector<ColumnPtr> lookup_arrays, const std::vector<ColumnPtr> dense_value_arrays)
    : BaseTable(name, schema), lookup_columns_(lookup_columns), lookup_arrays_to_real_columns_(lookup_arrays), appended_dense_value_columns_(dense_value_arrays), all_columns_() {
        all_columns_.insert(all_columns_.begin(), lookup_arrays_to_real_columns_.begin(), lookup_arrays_to_real_columns_.end());
        all_columns_.insert(all_columns_.end(), appended_dense_value_columns_.begin(), appended_dense_value_columns_.end());
    }

    //	LookupTable(const std::string& name, const std::vector<ColumnPtr>& columns); //if we already have columns, we can use this constructor to pass them in the table without any copy effort

    LookupTable::~LookupTable() {

    }
    /***************** utility functions *****************/
    //	const std::string& getName() const throw();

    //	const TableSchema getSchema() const throw();

    //	typedef std::vector<LookupArrayPtr> LoookupArrayVector;
    //	typedef shared_pointer_namespace::shared_ptr<LoookupArrayVector> LoookupArrayVectorPtr;

    const ColumnVectorPtr LookupTable::getLookupArrays() {

        ColumnVectorPtr lookup_array_vec_ptr(new ColumnVector());

        TableSchema::const_iterator it;

        for (it = this->schema_.begin(); it != this->schema_.end(); it++) {
            bool matching_column_found = false;
            for (unsigned int i = 0; i < lookup_columns_.size(); i++) {

                ColumnPtr col = lookup_columns_[i]->getLookupArrayforColumnbyName(*it);
                if (col) {
                    lookup_array_vec_ptr->push_back(col);
                    matching_column_found = true;
                }
            }
            if (!matching_column_found) {
                cout << "FATAL ERROR! could not find column " << it->second << " in any of the tables indexed by Lookup Table " << this->name_ << endl;
                return ColumnVectorPtr(); //return NULL Pointer
            }
        }

        return lookup_array_vec_ptr;
    }

    const ColumnVector& LookupTable::getDenseValueColumns() {
        return appended_dense_value_columns_;
    }

    bool LookupTable::copyColumnsInMainMemory(){
        
                ColumnVectorPtr new_lookup_arrays(new ColumnVector());

            for (unsigned int i = 0; i < lookup_columns_.size(); i++) {
                PositionListPtr tids = lookup_columns_[i]->getPositionList();
                tids = copy_if_required(tids, hype::PD_Memory_0);
                if(!tids){
                    COGADB_FATAL_ERROR("Failed to transfer column back " << lookup_columns_[i]->getPositionList()->getName() << " (" << util::getName(lookup_columns_[i]->getPositionList()->getColumnType()) << ") to main memory!","");
                }
                lookup_columns_[i]=LookupColumnPtr(new LookupColumn(lookup_columns_[i]->getTable(), tids));
                ColumnVectorPtr vec = lookup_columns_[i]->getLookupArrays();
                new_lookup_arrays->insert(new_lookup_arrays->end(), vec->begin(), vec->end());
            }
        lookup_arrays_to_real_columns_=*new_lookup_arrays;  
        all_columns_.clear();
        all_columns_.insert(all_columns_.begin(), lookup_arrays_to_real_columns_.begin(), lookup_arrays_to_real_columns_.end());
        all_columns_.insert(all_columns_.end(), appended_dense_value_columns_.begin(), appended_dense_value_columns_.end());    
                
            
        
//        for(size_t i=0;i< all_columns_.size();++i){
//            //don't do anything for columns on disk
//            if( all_columns_[i]->isLoadedInMainMemory()){
//                ColumnPtr tmp=copy_if_required( all_columns_[i],hype::PD_Memory_0);
//                if(tmp){
//                     all_columns_[i]=tmp;
//                }else{
//                    COGADB_FATAL_ERROR("Failed to transfer column back " << all_columns_[i]->getName() << " (" << util::getName(all_columns_[i]->getColumnType()) << ") to main memory!","");
//                }
//            }
//        }
        return true;
    }
    
    void LookupTable::print() { //const throw(){
        std::cout << toString() << std::endl;
        //cout << "printing Lookup Table: " << this->getName() << endl;

        /* 
         std::vector<ColumnPtr> columns_; 
         columns_.insert(columns_.begin(),lookup_arrays_to_real_columns_.begin(),lookup_arrays_to_real_columns_.end());
         columns_.insert(columns_.end(),appended_dense_value_columns_.begin(),appended_dense_value_columns_.end());        
        
        
         if(!CoGaDB::quiet && CoGaDB::verbose){
         cout << endl << "LookupTable '" << name_ << "':" << endl;
         cout << "Consist of " << lookup_columns_.size() << " Tables: ";
         for (unsigned int i = 0; i < lookup_columns_.size(); i++) {
             cout << "\t" << lookup_columns_[i]->getTable()->getName() << endl;
            
         }       
         }
         cout << "+------------------------------" << endl;
         cout << "|\t";
         for (unsigned int i = 0; i < columns_.size(); i++) {
             cout << columns_[i]->getName() << "\t|\t";
         }
         cout << endl;
         cout << "+------------------------------" << endl;
         if (columns_.empty()) {
             cout << "Table " << name_ << " is Empty, nothing to print" << endl;
             return;
         }
         //cout << setiosflags(ios::fixed) << std::setw(11) << std::setprecision(6);
         for (unsigned int j = 0; j < columns_[0]->size(); j++) {
             cout << "|\t";
             for (unsigned int i = 0; i < columns_.size(); i++) {
                 boost::any value = columns_[i]->get(j);
                 if (value.type() == typeid (string)) {
                     cout << boost::any_cast<string>(value) << "\t|\t";
                 } else if (value.type() == typeid (int)) {
                     cout << boost::any_cast<int>(value) << "\t|\t";
                 } else if (value.type() == typeid (float)) {
                     cout << setiosflags(ios::fixed) << boost::any_cast<float>(value) << "\t|\t";
                 } else if (value.type() == typeid (bool)) {
                     cout << boost::any_cast<bool>(value) << "\t|\t";
                 } else {

                 }



             }
             cout << endl;
         }
         //for (unsigned int i = 0; i < columns_.size(); i++)
         if(columns_.size()>0)    
                 cout << endl << columns_[0]->size() << " rows" << endl << endl;
         */
        //			cout << endl << columns_[0]->size() << " rows" << endl << endl;

        //   		ColumnVectorPtr columns = this->getLookupArrays();

        //			if(columns || columns->empty()) return;

        //			cout << endl << this->name_ << ":" << endl;
        //			cout << "|\t";
        //			for(unsigned int i=0;i<columns->size();i++){
        //				cout << (*columns)[i]->getName() << "\t|\t";
        //			}
        //			cout << endl;
        //			cout << "-------------------------------" << endl;
        //			if(lookup_columns_.empty()){
        //				cout << "Lookup Table " << this->name_ << " is Empty, nothing to print" << endl;
        //				return;
        //			}
        //			for(unsigned int j=0;j<(*columns)[0]->size();j++){
        //				cout << "|\t";
        //				for(unsigned int i=0;i<columns->size();i++){
        //					boost::any value = (*columns)[i]->get(j);
        //					if(value.type()==typeid(string)){
        //						cout << boost::any_cast<string>(value) << "\t|\t";
        //					}else if (value.type()==typeid(int)){
        //						cout << boost::any_cast<int>(value) << "\t|\t";
        //					}else if (value.type()==typeid(float)){
        //						cout << boost::any_cast<float>(value) << "\t|\t";
        //					}else if (value.type()==typeid(bool)){
        //						cout << boost::any_cast<bool>(value) << "\t|\t";
        //					}else{

        //					}


        //
        //				}
        //				cout << endl;
        //			}
        //			cout << endl << (*columns)[0]->size() << " rows" << endl << endl;
    }

    /*! tries to store table in database*/
    bool LookupTable::store(const std::string&) {
        return false; //not allowed for Lookup columns
    }

    /*! tries to load table form database*/
    bool LookupTable::load(TableLoaderMode loader_mode) {
        return false; //not allowed for Lookup columns
    }

    bool LookupTable::loadDatafromFile(std::string, bool quiet) {
        return false; //not allowed for Lookup columns
    }

    const TablePtr LookupTable::materialize() const {

        //        if (lookup_arrays_to_real_columns_.size() == 0) return TablePtr();
        //        TablePtr result_table = TablePtr(new Table(string("Materialize( ") + this->getName() + " )", this->getSchema())); //tmp_schema));
        //        ColumnPtr lookup_array_ptr = lookup_arrays_to_real_columns_[0];
        //        //fetch result tuples
        //        for (unsigned int i = 0; i < lookup_array_ptr->size(); i++) {
        //            //TID j = (*lookup_array_ptr)[i];
        //            result_table->insert(this->fetchTuple(i));
        //        }
        //this->printSchema();
        std::vector<ColumnPtr> materialized_columns;
        for (unsigned int i = 0; i < all_columns_.size(); i++) {
            ColumnPtr ptr;
            ptr = all_columns_[i]->materialize();
            //            if(!all_columns_[i]->isMaterialized() || all_columns_[i]->isCompressed()){
            //                ptr = all_columns_[i]->materialize();
            //            }else{
            //                ptr = all_columns_[i];
            //            }
            if (!ptr) {
                COGADB_FATAL_ERROR("Materialization of Table Failed! For Column: " << all_columns_[i]->getName(), "");
            }
            materialized_columns.push_back(ptr);
        }
        TablePtr result_table = TablePtr(new Table(string("Materialize( ") + this->getName() + " )", materialized_columns)); //tmp_schema));
        //result_table->printSchema();
        return result_table;
    }

    bool LookupTable::addColumn(ColumnPtr col) {
        if (!col) return false;
        //this->lookup_arrays_to_real_columns_.push_back(col);
        if (!quiet && verbose) cout << "ADD Column " << col->getName() << " to LookupTable " << this->getName() << endl;
        appended_dense_value_columns_.push_back(col);
        all_columns_.push_back(col);
        //this->columns_.push_back(col);
        this->schema_.push_back(Attribut(col->getType(), col->getName()));
        return true;
    }

    /*! \brief aggregates LookupTable wrt Lookup Column, which indexes the LookupTable. IT takes care of copying the LookupColumns and their corresponding PostitionList. Furthermore, it calls functions which createa new LookupArraylist, which represents the columns of the Table (a view, to be more precise) represented by the Lookup Table*/
    const LookupTablePtr LookupTable::aggregate(const std::string& result_lookup_table_name, const LookupTable& lookup_table, const LookupColumn& lookup_col, const ProcessorSpecification& proc_spec) {

        
        std::vector<LookupColumnPtr> new_aggregated_lookup_columns;
        //apply aggregation to each Lookup column
        for (unsigned int i = 0; i < lookup_table.lookup_columns_.size(); i++) {
            //			LookupColumnPtr tmp = shared_pointer_namespace::static_pointer_cast<LookupColumn> (lookup_table_columns[i]);
            LookupColumnPtr col = lookup_table.lookup_columns_[i]->aggregate(lookup_col, proc_spec); //returns new lookup column
            if(!col) return LookupTablePtr();
//            assert(col != NULL);
            new_aggregated_lookup_columns.push_back(col);
        }
        //get all new Lookup Arrays of all new Lookup Columns, so that they can be passed to new Lookup Table
        ColumnVectorPtr new_lookup_arrays(new ColumnVector());
        for (unsigned int i = 0; i < new_aggregated_lookup_columns.size(); i++) {
            ColumnVectorPtr tmp_lookup_arrays = new_aggregated_lookup_columns[i]->getLookupArrays();
            assert(tmp_lookup_arrays != NULL);
            //only add Lookup Arrays that are part of the schema
            TableSchema schema = lookup_table.getSchema();
            TableSchema::const_iterator it;
            for (unsigned int i = 0; i < tmp_lookup_arrays->size(); i++) {
                for (it = schema.begin(); it != schema.end(); it++) {
                    if ((*tmp_lookup_arrays)[i]->getName() == it->second) {
                        new_lookup_arrays->push_back((*tmp_lookup_arrays)[i]);
                    }
                }
            }
            //			new_lookup_arrays->insert(new_lookup_arrays->end(), //insert at the end of vector
            //													 tmp_lookup_arrays->begin(),   //insert Pointers to Lookup Arrays
            //													 tmp_lookup_arrays->end());
        }

        return LookupTablePtr(new LookupTable(result_lookup_table_name, lookup_table.getSchema(), new_aggregated_lookup_columns, *new_lookup_arrays, lookup_table.appended_dense_value_columns_));

        //
        //		ColumnVectorPtr lookup_arrays_lookup_table1 = lookup_col1->getLookupArrays();


        //		ColumnVectorPtr lookup_arrays_lookup_table2 = lookup_col2->getLookupArrays();
        //		cout << "Lookup Arrays of result table: " <<  endl;
        //		for(unsigned int i=0;i<lookup_arrays_lookup_table2->size();i++){
        //			lookup_arrays_lookup_table1->push_back((*lookup_arrays_lookup_table2)[i]);
        //		}




        //		LookupColumnPtr lookup_column_of_lookuptable = shared_pointer_namespace::static_pointer_cast<LookupColumn> (lookup_table_columns[0]);
        //		assert(lookup_column_of_lookuptable!=NULL	)
        //		const PositionListPtr lookup_table_tids=lookup_column_of_lookuptable->getPositionList();

        //		TID tid(0), translated_tid(0);
        //		for(unsigned int i=0;i<lookup_col_tids->size();i++){
        //			tid=(*lookup_col_tids)[i];
        //			translated_tid=(*lookup_table_tids)[tid];
        //			tids->push_back(translated_tid);
        //		}

        //new_lookup_columns<-deepcopy(lookup_table_columns)  //pass new PostitionPtrlist
        //new_lookup_arrays_<-deepcopy(lookup_arrays_) //pass new PostitionPtrlist
        //return LookupTablePtr(new LookupTable(lookup_table->getName(), lookup_table->getSchema(), new_lookup_columns , new_lookup_arrays_));
        //		return LookupTablePtr();
    }

    const LookupTablePtr LookupTable::concatenate(const std::string&, const LookupTable& lookup_table1, const LookupTable& lookup_table2) {

        assert(&lookup_table1 != &lookup_table2); //it makes no sense to concatenate the same Lookup Table to itself
        assert(lookup_table1.getNumberofRows() == lookup_table2.getNumberofRows());

        //concatenate Lookupcolumn vector
        std::vector<LookupColumnPtr> new_lookup_columns; //(new LookupColumn());
        new_lookup_columns.insert(new_lookup_columns.end(), lookup_table1.lookup_columns_.begin(), lookup_table1.lookup_columns_.end());
        new_lookup_columns.insert(new_lookup_columns.end(), lookup_table2.lookup_columns_.begin(), lookup_table2.lookup_columns_.end());
        //concatenate schemas
        TableSchema new_schema;
        new_schema.insert(new_schema.end(), lookup_table1.schema_.begin(), lookup_table1.schema_.end());
        new_schema.insert(new_schema.end(), lookup_table2.schema_.begin(), lookup_table2.schema_.end());
        //concatenate lookup arrays
        ColumnVectorPtr new_lookup_arrays(new ColumnVector());
        new_lookup_arrays->insert(new_lookup_arrays->end(), lookup_table1.lookup_arrays_to_real_columns_.begin(), lookup_table1.lookup_arrays_to_real_columns_.end());
        new_lookup_arrays->insert(new_lookup_arrays->end(), lookup_table2.lookup_arrays_to_real_columns_.begin(), lookup_table2.lookup_arrays_to_real_columns_.end());

        ColumnVectorPtr new_appended_dense_values_arrays(new ColumnVector());
        new_appended_dense_values_arrays->insert(new_appended_dense_values_arrays->end(), lookup_table1.appended_dense_value_columns_.begin(), lookup_table1.appended_dense_value_columns_.end());
        new_appended_dense_values_arrays->insert(new_appended_dense_values_arrays->end(), lookup_table2.appended_dense_value_columns_.begin(), lookup_table2.appended_dense_value_columns_.end());

        //		//Lookup Colums
        //		std::vector<ColumnPtr> lookup_arrays_to_real_columns_;



        return LookupTablePtr(new LookupTable(string("concat( ") + lookup_table1.getName() + string(",") + lookup_table2.getName() + " )",
                new_schema,
                new_lookup_columns,
                *new_lookup_arrays, *new_appended_dense_values_arrays));

        //concatenate Lookupcolumn vector



        //create new Lookup Table

        //		return LookupTablePtr();
    }

    /***************** status report *****************/
    //	const unsigned int getNumberofRows() const throw();

    bool LookupTable::isMaterialized() const throw () {
        return false;
    }

    /***************** relational operations *****************/
    //	const TablePtr selection(const std::string& column_name, const boost::any& value_for_comparison, const ValueComparator& comp, const ComputeDevice& comp_dev) const;// = 0;

    //	const TablePtr projection(const std::list<std::string>& columns_to_select, const ComputeDevice comp_dev) const;// = 0;

    //	const TablePtr join(TablePtr table, const std::string& join_column_table1, const std::string& join_column_table2, const ComputeDevice comp_dev) const;// = 0;

    //	const TablePtr sort(const std::string& column_name, SortOrder order, ComputeDevice comp_dev) const;// = 0;

    //	const TablePtr groupby(const std::string& grouping_column, const std::string& aggregation_column,  AggregationMethod agg_meth=SUM, ComputeDevice comp_dev=CPU) const;// = 0;

    /***************** read and write operations at table level *****************/
    const Tuple LookupTable::fetchTuple(const TID& id) const {
        Tuple t;
        for (unsigned int i = 0; i < lookup_arrays_to_real_columns_.size(); i++) {
            if (!lookup_arrays_to_real_columns_[i]->isLoadedInMainMemory()) {
                //load column in memory
                //                this->getColumnbyName(columns_[j]->getName());
                loadColumnFromDisk(lookup_arrays_to_real_columns_[i]);
            }
            t.push_back(lookup_arrays_to_real_columns_[i]->get(id));
        }
        for (unsigned int i = 0; i < appended_dense_value_columns_.size(); i++) {
            t.push_back(appended_dense_value_columns_[i]->get(id));
        }
        return t; //not allowed for Lookup columns
    }

    bool LookupTable::insert(const Tuple&) {
        return false; //not allowed for Lookup columns
    }

    bool LookupTable::update(const std::string&, const boost::any&) {
        return false; //not allowed for Lookup columns
    }

    bool LookupTable::remove(const std::string&, const boost::any&) {
        return false; //not allowed for Lookup columns
    }

    bool LookupTable::replaceColumn(const std::string& column_name, const ColumnPtr new_column){
        COGADB_FATAL_ERROR("Called unimplemented function!","");
        return false;
    }
    
    
    const ColumnPtr LookupTable::getColumnbyName(const std::string& column_name) const throw () {
        for (unsigned int i = 0; i < lookup_arrays_to_real_columns_.size(); i++) {
            if (lookup_arrays_to_real_columns_[i]->getName() == column_name) {
                if (!quiet && verbose)
                    std::cout << "Found Column: " << column_name << ":  " << lookup_arrays_to_real_columns_[i].get() << std::endl;
                if (!lookup_arrays_to_real_columns_[i]->isLoadedInMainMemory()) {
                    loadColumnFromDisk(lookup_arrays_to_real_columns_[i]);
                }
                /* update the access statistics in the stored table */
                for(size_t j=0;j<lookup_columns_.size();++j){
                    /* statistics are updated by getColumnbyName() function*/
                    ColumnPtr col = lookup_columns_[j]->getTable()->getColumnbyName(column_name);
                    if(col){
                        /* column found, statistics were updated, leave for loop*/
                        break;
                    }
                }

                return lookup_arrays_to_real_columns_[i];
            }
        }
        for (unsigned int i = 0; i < appended_dense_value_columns_.size(); i++) {
            if (appended_dense_value_columns_[i]->getName() == column_name) {
                if (!quiet && verbose)
                    std::cout << "Found Column: " << column_name << ":  " << appended_dense_value_columns_[i].get() << std::endl;
                return appended_dense_value_columns_[i];
            }
        }

        COGADB_ERROR(std::string("Error: could not find column ") + column_name + std::string(" in Table '") + name_ + std::string("'!"), "");
        return ColumnPtr(); //not found, return NULL Pointer
    }


    const std::vector<ColumnPtr>& LookupTable::getColumns() const {
        //return lookup_arrays_to_real_columns_;
        return this->all_columns_;
        //		static std::vector<ColumnPtr> v;
        //		return v; //not allowed for Lookup columns
    }

    const std::vector<LookupColumnPtr>& LookupTable::getLookupColumns() const {
        return lookup_columns_;
    }

}; //end namespace CogaDB


