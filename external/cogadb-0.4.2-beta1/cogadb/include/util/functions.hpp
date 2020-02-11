/* 
 * File:   functions.hpp
 * Author: sebastian
 *
 * Created on 10. Januar 2015, 21:15
 */

#ifndef FUNCTIONS_HPP
#define	FUNCTIONS_HPP

#include <fstream>
#include <cmath>

namespace CoGaDB{
    
    template<class T>
    size_t getGreaterPowerOfTwo(T val){
        size_t current_power_of_two=0;
        while(pow(2,current_power_of_two)<=val){
            current_power_of_two++;
        }
        return current_power_of_two;
    }

    // count set ones in an unsigned integer: https://books.google.de/books?id=iBNKMspIlqEC&pg=PA66&redir_esc=y#v=onepage&q&f=false
    inline int pop_count(uint32_t x) {
        x = x - ((x >> 1) & 0x55555555);
        x = (x & 0x33333333) + ((x >> 2) & 0x33333333);
        x = (x + (x >> 4)) & 0x0F0F0F0F;
        x = x + (x >> 8);
        x = x + (x >> 16);
        return x & 0x0000003F;
    }

    inline size_t getUsedMainMemoryInBytes(){
        size_t dummy = 0, resident = 0;
        std::ifstream buffer("/proc/self/statm");
        buffer >> dummy >> resident;
        buffer.close();
        size_t page_size_in_bytes = sysconf(_SC_PAGE_SIZE);
        size_t resident_memory_in_byte = resident * page_size_in_bytes;
        return resident_memory_in_byte;
    }

    /* \detail result_array has to have AT LEAST (array_size+1) bytes! */
    template<class T>
    void serial_prefixsum(T* __restrict__ array, const size_t& array_size,  T* __restrict__ result_array){
                result_array[0]=0;
                for(unsigned int i=1;i<array_size+1;i++){
                    result_array[i]=result_array[i-1]+array[i-1];
                }
    }    
    
}; //end namespace CogaDB

#endif	/* FUNCTIONS_HPP */

