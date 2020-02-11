
#include <util/shared_library.hpp>
#include <iostream>
#include <assert.h>
#include <dlfcn.h>

namespace CoGaDB {

    SharedLibrary::SharedLibrary(void* _shared_lib) 
    : shared_lib_(_shared_lib)
    {    
        assert(shared_lib_!=NULL);
    }
        
    SharedLibrary::~SharedLibrary(){
        dlclose(shared_lib_);
    }
    
    void* SharedLibrary::getSymbol(const std::string& mangeled_symbol_name){
        char* error = NULL;
        void* symbol = dlsym(shared_lib_, mangeled_symbol_name.c_str()); //"compiled_query");
        error = dlerror();
        if (error) {
            std::cerr << error << std::endl;
            return NULL;
        }
        return symbol;
    }
    
    const SharedLibraryPtr loadSharedLibrary(const std::string& file_path){
        char* error = NULL;
        
        void *myso = dlopen(file_path.c_str(), RTLD_NOW);
        if(!myso){
            return SharedLibraryPtr();
        }
        error = dlerror();
        if (error) {
            std::cerr << error << std::endl;
            return SharedLibraryPtr();
        }
        return SharedLibraryPtr(new SharedLibrary(myso));
    }    
    
}; //end namespace CoGaDB




