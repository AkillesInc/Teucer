
#all: offline_learning offline_learning_2d

#PATH_TO_CUDA :=/usr/local/cuda-5.0
PATH_TO_CUDA :=/usr/local/cuda


CXX :=g++
CC :=gcc

PROFILING_FLAGS:= -pg
OPTIMIZATION_FLAGS:= -g


ifneq ($(wildcard ../../../config.mk),) 
	include ../../../config.mk
endif

ARCH := $(shell getconf LONG_BIT)

ifeq (${ARCH},32)
PATH_TO_CUDA_LIB=${PATH_TO_CUDA}/lib
else ifeq (${ARCH},64)
PATH_TO_CUDA_LIB=${PATH_TO_CUDA}/lib64
endif


INCLUDEDIRS := -I../../../include
	
CPP_FILES := $(wildcard *.cpp)
CUDA_FILES := $(wildcard *.cu)
#HEADER_FILES := $(wildcard include/*.hpp) $(wildcard include/*/*.hpp)
OBJ_FILES := $(addprefix obj/,$(notdir $(CPP_FILES:.cpp=.o))) $(addprefix obj/,$(notdir $(CUDA_FILES:.cu=.o)))
#BIN_FILES  := $(addprefix bin/,$(notdir $(CPP_FILES:.cpp=))) $(addprefix bin/,$(notdir $(CUDA_FILES:.cu=)))
#RUN_TARGETS := $(addprefix bin/,$(notdir $(CPP_FILES:.cpp=run)))

#used libraries
#LD_FLAGS := -L../../lib -lstemod -lboost_system -lboost_filesystem -lboost_thread -lpthread -lloki -lrt
#LD_FLAGS := -L../../lib -static -lstemod -lboost_system -lboost_filesystem -lboost_thread -lpthread -lloki -lrt
LD_FLAGS := ${OPTIMIZATION_FLAGS} ${PROFILING_FLAGS} -Wl,-Bstatic -lboost_thread -pthread -Wl,-Bdynamic -L../../../lib -lstemod -lboost_system -lboost_filesystem -lboost_program_options-mt -lloki -lrt -ltbb -L${PATH_TO_CUDA_LIB} -lcudart 
#-Wl,--as-needed
#LD_FLAGS := -L../../lib -lstemod -lboost_system -lboost_filesystem -lpthread -lloki -lrt -ltbb -static -lboost_thread 
CC_FLAGS := ${OPTIMIZATION_FLAGS} ${PROFILING_FLAGS}
NVCC_FLAGS := -g -O3

#CC_FLAGS := -O3 -march=native -mtune=native
#NVCC_FLAGS := -O3

all: ../../../lib/libstemod.so ../../../lib/libstemod.a bin/benchmark


#bin/benchmark: ${OBJ_FILES}
#	 g++ ${CC_FLAGS} -Wl,-rpath,../../../lib -o $@ ${OBJ_FILES} ${INCLUDEDIRS} -L../../../lib -Wl,-Bstatic -lboost_thread -Wl,-Bdynamic -lpthread -lstemod -lboost_system -lboost_filesystem -lloki -lrt -ltbb -L/usr/local/cuda-5.0/lib -lcudart

#bin/benchmark: ${OBJ_FILES}
#	 g++ ${CC_FLAGS} -Wl,-rpath,../../../lib -o $@ ${OBJ_FILES} ${INCLUDEDIRS} -L../../../lib  -Wl,-Bdynamic -lstemod -Wl,-Bstatic -lboost_thread -Wl,-Bdynamic  -lboost_system -lboost_filesystem -lloki -lrt -ltbb -L/usr/local/cuda-5.0/lib -lcudart 

#bin/benchmark: ${OBJ_FILES} 
#	 g++ ${CC_FLAGS} -Wl,-rpath,../../../lib -o $@ ${OBJ_FILES} ${INCLUDEDIRS} -Wl,-Bstatic -lboost_thread-mt -pthread -L../../../lib -lstemod -lboost_system-mt -Wl,-Bdynamic -lboost_filesystem-mt -lboost_program_options-mt -lloki -lrt -ltbb -L${PATH_TO_CUDA}/lib -lcudart -Wl,--as-needed

bin/benchmark: ${OBJ_FILES} 
	 g++ ${CC_FLAGS} -Wl,-rpath,../../../lib -o $@ ${OBJ_FILES} ${INCLUDEDIRS} ${LD_FLAGS}


#-L../../../lib  -Wl,-Bdynamic -lstemod -Wl,-Bstatic -lboost_thread -Wl,-Bdynamic  -lboost_system -lboost_filesystem -lloki -lrt -ltbb -L/usr/local/cuda-5.0/lib -lcudart 



../../../lib/libstemod.so:
	cd ../../../; make

../../../lib/libstemod.a:
	cd ../../../; make



project_status:
	@echo "Source Files:" ${CPP_FILES} ${CUDA_FILES}
	@echo "Object Files:" ${OBJ_FILES}
	@echo "Run Targes:" ${RUN_TARGETS}

#${RUN_TARGETS}:
#	./$<

#offline_learning: ../../lib/libstemod.so ../../lib/libstemod.a
#offline_learning: offline_learning.cpp
#	g++ -g offline_learning.cpp -o offline_learning -I../../include -L../../lib -static -lstemod -lboost_system -lboost_filesystem -lboost_thread -lpthread -lloki -lrt
#	
#offline_learning_2d: ../../lib/libstemod.so ../../lib/libstemod.a offline_learning_2d.cpp
#	g++ -g offline_learning_2d.cpp -o offline_learning_2d -I../../include -L../../lib -static -lstemod -lboost_system -lboost_filesystem -lboost_thread -lpthread -lloki -lrt
	
obj/%.o: %.cpp ../../../lib/libstemod.a ${HEADER_FILES} ${CPP_FILES} Makefile                       
	g++ ${CC_FLAGS} -Wall -Werror -c -o $@ $< ${INCLUDEDIRS}
#${LD_FLAGS}

	
obj/%.o: %.cu ${HEADER_FILES} ${CUDA_FILES} Makefile                        
	nvcc ${NVCC_FLAGS} -c -o $@ $< ${INCLUDEDIRS} 




clean:
	rm -f bin/*
	rm -f obj/*.o

#run:
#	export LD_LIBRARY_PATH=$(shell pwd); ./offline_learning

#run_offline_learning_2d:
#	export LD_LIBRARY_PATH=$(shell pwd); ./offline_learning_2d

#run: ${BIN_FILES}
#	./

#debug:
#	nemiver ./offline_learning

