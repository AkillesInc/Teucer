#pragma once

#include <core/compressed_column.hpp>
#include <sstream>
#include <string> 
#include <fstream> 
#include <boost/archive/binary_iarchive.hpp>
#include <boost/archive/binary_oarchive.hpp>
#include <boost/serialization/map.hpp>
#include <util/begin_ptr.hpp>
#include <core/data_dictionary.hpp>
#include <compression/wah_bitmap_column.hpp>

namespace CoGaDB {

    /*
     * This column compresses values by comparing them to values in another column 
     * that is ideally foreign-key referenced.
     */
    template<class T>
    class ReferenceBasedCompressedColumn : public CompressedColumn<T> {
    public:
        ReferenceBasedCompressedColumn(const std::string &name, AttributeType db_type);


        ReferenceBasedCompressedColumn(const std::string &name, AttributeType db_type,
                                       std::string lookup_value_table_name,
                                       std::string lookup_value_column_name,
                                       std::string reference_value_table_name,
                                       std::string reference_value_column_name);

        ReferenceBasedCompressedColumn(const ReferenceBasedCompressedColumn<T> &other);

        ReferenceBasedCompressedColumn<T> &operator=(const ReferenceBasedCompressedColumn<T> &other);

        ~ReferenceBasedCompressedColumn();

        bool insert(const boost::any &new_Value);

        bool insert(const T &new_value);

        bool insert(const T &new_value, const TID sb_rb_id);

        template<typename InputIterator>
        bool insert(InputIterator first, InputIterator last);

        bool update(TID tid, const boost::any &new_value);

        bool update(PositionListPtr tid, const boost::any &new_value);

        bool remove(TID tid);

        //assumes tid list is sorted ascending
        bool remove(PositionListPtr tid);

        bool clearContent();

        const PositionListPtr selection(const SelectionParam &param);

        const boost::any get(TID tid);

        void print() const throw();

        size_t size() const throw();

        size_t getSizeinBytes() const throw();

        const ColumnPtr copy() const;

        const typename ColumnBaseTyped<T>::DenseValueColumnPtr copyIntoDenseValueColumn(
                const ProcessorSpecification &proc_spec) const;

        const ColumnPtr gather(PositionListPtr tid_list, const GatherParam &);

        T &operator[](const TID index);

        void initReferenceColumnPointers() const;

    private:
        typedef boost::shared_ptr<BitPackedDictionaryCompressedColumn<T> > BitPackedDictionaryCompressedColumnPtr;

        WAHBitmapColumnPtr exception_index_;
        BitPackedDictionaryCompressedColumnPtr exception_values_;

        bool load_impl(const std::string &path, boost::archive::binary_iarchive &ia);

        bool store_impl(const std::string &path, boost::archive::binary_oarchive &oa);

        size_t number_of_rows_;
        //typedef std::map<TID, T> HashTable;
        //HashTable exception_values;

        mutable boost::shared_ptr<ColumnBaseTyped<T> > reference_value_column;
        mutable boost::shared_ptr<ColumnBaseTyped<TID> > lookup_value_column;

        std::string lookup_value_table_name;
        std::string lookup_value_column_name;
        std::string reference_value_table_name;
        std::string reference_value_column_name;
    };

    /***************** Start of Implementation Section ******************/

    template<class T>
    void ReferenceBasedCompressedColumn<T>::initReferenceColumnPointers() const {
        if (reference_value_column && lookup_value_column) return;

        TablePtr reference_table = getTablebyName(this->reference_value_table_name);
        assert(reference_table != NULL);
        reference_value_column =
                shared_pointer_namespace::dynamic_pointer_cast<ColumnBaseTyped<T> >(
                        reference_table->getColumnbyName(this->reference_value_column_name));
        assert(reference_value_column != NULL);

        TablePtr lookup_table = getTablebyName(this->lookup_value_table_name);
        assert(lookup_table != NULL);
        lookup_value_column =
                shared_pointer_namespace::dynamic_pointer_cast<ColumnBaseTyped<TID> >(
                        lookup_table->getColumnbyName(this->lookup_value_column_name));
        assert(lookup_value_column != NULL);
    }

    template<class T>
    ReferenceBasedCompressedColumn<T>::ReferenceBasedCompressedColumn(const std::string &name, AttributeType db_type) :
            CompressedColumn<T>(name, db_type, REFERENCE_BASED_COMPRESSED), number_of_rows_(0),
            //exception_values(),
            reference_value_column(),
            lookup_value_column(), lookup_value_table_name(""),
            lookup_value_column_name(""),
            reference_value_table_name(""),
            reference_value_column_name(""),
            exception_index_(new WAHBitmapColumn(this->name_ + "_EXCEPTION_INDEX")),
            exception_values_(
                    new BitPackedDictionaryCompressedColumn<T>(this->name_ + "_EXCEPTION_VALUES", VARCHAR, 3)) {


    }

    template<class T>
    ReferenceBasedCompressedColumn<T>::ReferenceBasedCompressedColumn(const std::string &name, AttributeType db_type,
                                                                      const std::string _lookup_value_table_name,
                                                                      const std::string _lookup_value_column_name,
                                                                      const std::string _reference_value_table_name,
                                                                      const std::string _reference_value_column_name) :
            CompressedColumn<T>(name, db_type, REFERENCE_BASED_COMPRESSED), number_of_rows_(0),
            //exception_values(),
            reference_value_column(),
            lookup_value_column(), lookup_value_table_name(_lookup_value_table_name),
            lookup_value_column_name(_lookup_value_column_name),
            reference_value_table_name(_reference_value_table_name),
            reference_value_column_name(_reference_value_column_name),
            exception_index_(new WAHBitmapColumn(this->name_ + "_EXCEPTION_INDEX")),
            exception_values_(
                    new BitPackedDictionaryCompressedColumn<T>(this->name_ + "_EXCEPTION_VALUES", VARCHAR, 3)) {

    }

    template<class T>
    ReferenceBasedCompressedColumn<T>::ReferenceBasedCompressedColumn(const ReferenceBasedCompressedColumn<T> &other)
            : CompressedColumn<T>(other.getName(), other.getType(), other.getColumnType()),
              number_of_rows_(other.number_of_rows_),
            //exception_values(other.exception_values),
              reference_value_column(other.reference_value_column),
              lookup_value_column(other.lookup_value_column),
              reference_value_table_name(other.reference_value_table_name),
              reference_value_column_name(other.reference_value_column_name),
              lookup_value_table_name(other.lookup_value_table_name),
              lookup_value_column_name(other.lookup_value_column_name),
              exception_index_(other.exception_index_),
              exception_values_(other.exception_values_) {

    }

    template<class T>
    ReferenceBasedCompressedColumn<T> &ReferenceBasedCompressedColumn<T>::operator=(
            const ReferenceBasedCompressedColumn<T> &other) {
        if (this != &other) // protect against invalid self-assignment
        {

            this->name_ = other.name_;
            this->db_type_ = other.db_type_;
            this->column_type_ = other.column_type_;
            this->number_of_rows_ = other.number_of_rows_;
            //this->exception_values = other.exception_values;
            this->reference_value_column = other.reference_value_column;
            this->lookup_value_column = other.lookup_value_column;
            this->reference_value_table_name = other.reference_value_table_name;
            this->reference_value_column_name = other.reference_value_column_name;
            this->lookup_value_table_name = other.lookup_value_table_name;
            this->lookup_value_column_name = other.lookup_value_column_name;
            this->exception_index_ = other.exception_index_;
            this->exception_values_ = other.exception_values_;

        }
        return *this;
    }


    template<class T>
    ReferenceBasedCompressedColumn<T>::~ReferenceBasedCompressedColumn() {
    }

    template<class T>
    bool ReferenceBasedCompressedColumn<T>::insert(const boost::any &new_value) {
        //initColumnPointers();
        if (new_value.empty()) return false;
        if (typeid(T) == new_value.type()) {
            return insert(boost::any_cast<T>(new_value));
        }
        return false;
    }

    template<typename T>
    bool ReferenceBasedCompressedColumn<T>::insert(const T &new_value) {
        COGADB_FATAL_ERROR("Not implemented for other types than STRING.", "");
    }

    template<>
    inline bool ReferenceBasedCompressedColumn<std::string>::insert(const std::string &new_value) {
        //initColumnPointers();
        // FIXME we assume that the foreign key is inserted before the value
        // number_of_rows_ stores current TID
        // check whether base value is same as reference base
        TID sb_rb_id = (*lookup_value_column)[number_of_rows_];
        std::string rb_base_value = (*reference_value_column)[sb_rb_id];
        if (new_value.compare(rb_base_value) != 0) {
            exception_index_->setNextRow();
            exception_values_->insert(new_value);
            // if not add to aux data structure
            //exception_values.insert(std::pair<int, std::string>(number_of_rows_, new_value));
        } else {
            exception_index_->unsetNextRow();
        }
        number_of_rows_++;
        return true;
    }

    template<>
    inline bool ReferenceBasedCompressedColumn<std::string>::insert(const std::string &new_value, const TID sb_rb_id) {
        //initColumnPointers();
        // FIXME we assume that the foreign key is inserted before the value
        // number_of_rows_ stores current TID
        // check whether base value is same as reference base
        // TID sb_rb_id = (*sampleBaseRBIDColumn)[number_of_rows_];
        std::string rb_base_value = (*reference_value_column)[sb_rb_id];
        if (new_value.compare(rb_base_value) != 0) {
            exception_index_->setNextRow();
            exception_values_->insert(new_value);
            // if not add to aux data structure
            //exception_values.insert(std::pair<int, std::string>(number_of_rows_, new_value));
        } else {
            exception_index_->unsetNextRow();
        }
        number_of_rows_++;
        return true;
    }

    template<typename T>
    template<typename InputIterator>
    bool ReferenceBasedCompressedColumn<T>::insert(InputIterator first, InputIterator last) {
        //initColumnPointers();
        for (InputIterator it = first; it != last; it++) {
            insert(*it);
        }
        return true;
    }

    template<class T>
    const PositionListPtr ReferenceBasedCompressedColumn<T>::selection(const SelectionParam &param) {
        COGADB_FATAL_ERROR("Called unimplemented method!", "");

        //queries that we can answer form the delta store:
        //unequal filters -> in which positions is a difference compared tp reference! (to be implemented)
        //filter for 'X'


        return PositionListPtr();
    }

    template<class T>
    const boost::any ReferenceBasedCompressedColumn<T>::get(TID tid) {
        return boost::any(operator[](tid));
    }

    template<class T>
    void ReferenceBasedCompressedColumn<T>::print() const throw() {
        //initColumnPointers();
        // TODO implement
        std::cout << "| " << this->name_ << " |" << std::endl;
        std::cout << "________________________" << std::endl;
        for (unsigned int i = 0; i < number_of_rows_; i++) {
            std::cout << "| " << i << " | " << std::endl;
        }
    }

    template<class T>
    size_t ReferenceBasedCompressedColumn<T>::size() const throw() {
        return number_of_rows_;
    }

    template<class T>
    const ColumnPtr ReferenceBasedCompressedColumn<T>::copy() const {
        //initColumnPointers();
        return ColumnPtr(new ReferenceBasedCompressedColumn<T>(*this));
    }

    template<class T>
    const typename ColumnBaseTyped<T>::DenseValueColumnPtr ReferenceBasedCompressedColumn<T>::copyIntoDenseValueColumn(
            const ProcessorSpecification &proc_spec) const {
        typedef typename ColumnBaseTyped<T>::DenseValueColumnPtr DenseValueColumnPtr;
        typedef typename ColumnBaseTyped<T>::DenseValueColumn DenseValueColumn;

        //COGADB_FATAL_ERROR("Called unimplemented method! Decompressing the sb_base column is a very bad idea anyway...",
        //                   "");
        if(proc_spec.proc_id != hype::PD0){
            COGADB_FATAL_ERROR("Function copyIntoDenseValueColumn() can only be called on CPUs!","");
        }

        DenseValueColumnPtr result(new DenseValueColumn(this->getName(), this->getType()));
        size_t num_elements = this->size();
        ReferenceBasedCompressedColumn<T>* this_col = const_cast<ReferenceBasedCompressedColumn<T>*> (this);
        for (size_t i = 0; i < num_elements; ++i) {
            result->insert((*this_col)[i]);
        }
        return result;
    }

    template<class T>
    const ColumnPtr ReferenceBasedCompressedColumn<T>::gather(PositionListPtr tid_list, const GatherParam &param) {
//        COGADB_FATAL_ERROR("Called unimplemented method!","");     
        assert(param.proc_spec.proc_id == hype::PD0);
        typedef typename ColumnBaseTyped<T>::DenseValueColumnPtr DenseValueColumnPtr;
        typedef typename ColumnBaseTyped<T>::DenseValueColumn DenseValueColumn;

        Timestamp begin = 0, end = 0;

        begin = getTimestamp();

        boost::shared_ptr<Column<T> > result(new Column<T>(this->name_, this->db_type_));
        try {
            result->resize(tid_list->size());
        } catch (std::bad_alloc &e) {
            return ColumnPtr();
        }

//        PositionListPtr copied_tids = copy_if_required(tid_list, this->mem_alloc->getMemoryID());
//        if(!copied_tids) return ColumnPtr();
//        tid_list=copied_tids;

        size_t num_elements = tid_list->size();
        TID *tid_array = tid_list->data();
        T *result_array = result->data();

        //ColumnPtr result_sb_rb_id_column = sampleBaseRBIDColumn->gather(tid_list, param);
        for (size_t i = 0; i < num_elements; ++i) {
            size_t index = tid_array[i];
            //typename HashTable::const_iterator value_iterator = exception_values.find(index);
            //is different to reference genome?
            //if (value_iterator != exception_values.end()) {
            //    result_array[i] = exception_values[index];
            //} else {
            //    //translate index to position in reference genome
            //    size_t sb_rb_id = (*lookup_value_column)[index];
            //    //get value of reference genome at position sb_rb_id
            //    result_array[i] = (*reference_value_column)[sb_rb_id];
            //}
            if (exception_index_->isRowSet(index)) {
                //HashTable::const_iterator value_iterator = exception_values.find(index);
                //if (value_iterator != exception_values.end()) {
                result_array[i] = (*exception_values_)[exception_index_->getPrefixSum(index)];
                //}
            } else {
                size_t sb_rb_id = (*lookup_value_column)[index];
                result_array[i] = (*reference_value_column)[sb_rb_id];
            }
        }
        end = getTimestamp();
        assert(end > begin);
        COGADB_WARNING("Time for Gather: " << double(end - begin) / (1024 * 1024 * 1024) << "s", "");

        return result;
    }


    template<class T>
    bool ReferenceBasedCompressedColumn<T>::update(TID tid, const boost::any &new_value) {
        // TODO implement
        return false;
    }

    template<class T>
    bool ReferenceBasedCompressedColumn<T>::update(PositionListPtr tids, const boost::any &new_value) {
        // TODO implement
        return false;
    }

    template<class T>
    bool ReferenceBasedCompressedColumn<T>::remove(TID tid) {
        // TODO implement
        return false;
    }

    template<class T>
    bool ReferenceBasedCompressedColumn<T>::remove(PositionListPtr tids) {
        // TODO implement
        if (!tids)
            return false;
        if (tids->empty())
            return false;
        return false;
    }

    template<class T>
    bool ReferenceBasedCompressedColumn<T>::clearContent() {
        number_of_rows_ = 0;
        // TODO reset bitmap and exception_values
        //exception_values.clear();
        return true;
    }

    template<class T>
    bool ReferenceBasedCompressedColumn<T>::store_impl(const std::string &path, boost::archive::binary_oarchive &oa) {
        oa << this->lookup_value_table_name;
        oa << this->lookup_value_column_name;
        oa << this->reference_value_table_name;
        oa << this->reference_value_column_name;
        oa << number_of_rows_;
        bool store_success = false;
        store_success = exception_values_->store(path);
        store_success |= exception_index_->store(path);
        return store_success;
    }

    template<class T>
    bool ReferenceBasedCompressedColumn<T>::load_impl(const std::string &path, boost::archive::binary_iarchive &ia) {
        ia >> this->lookup_value_table_name;
        ia >> this->lookup_value_column_name;
        ia >> this->reference_value_table_name;
        ia >> this->reference_value_column_name;
        ia >> number_of_rows_;
        bool load_success = false;
        load_success = exception_values_->load(path);
        load_success |= exception_index_->load(path);
        return load_success;
    }

    template<class T>
    T &ReferenceBasedCompressedColumn<T>::operator[](const TID index) {
        COGADB_FATAL_ERROR("Not implemented for any type but STRING.", "");
        //return NULL;
    }

    //typen und parallel_gpu_kernels

    template<>
    inline std::string &ReferenceBasedCompressedColumn<std::string>::operator[](const TID index) {
        if (exception_index_->isRowSet(index)) {
            //HashTable::const_iterator value_iterator = exception_values.find(index);
            //if (value_iterator != exception_values.end()) {
            return (*exception_values_)[exception_index_->getPrefixSum(index)];
            //}
        } else {
            TID sb_rb_id = (*lookup_value_column)[index];
            return (*reference_value_column)[sb_rb_id];
        }
    }

    template<class T>
    size_t ReferenceBasedCompressedColumn<T>::getSizeinBytes() const throw() {
        return sizeof(size_t)
               + this->reference_value_column_name.capacity() + this->lookup_value_column_name.capacity()
               + this->reference_value_table_name.capacity() + this->lookup_value_table_name.capacity()
               + this->fk_constr_.getNameOfForeignKeyColumn().capacity()
               + this->fk_constr_.getNameOfPrimaryKeyColumn().capacity()
               + this->fk_constr_.getNameOfForeignKeyTable().capacity()
               + this->fk_constr_.getNameOfPrimaryKeyTable().capacity()
               + this->exception_index_->getSizeInBytes() + this->exception_values_->getSizeinBytes();
    }

};

