echo "Installing packages required by CoGaDB..."
sudo apt-get install gcc g++ clang astyle make cmake flex libtbb-dev libreadline6 libreadline6-dev doxygen doxygen-gui graphviz xsltproc libxslt1-dev libnuma1 libnuma-dev sharutils bison libsparsehash-dev netcat-openbsd libbam-dev zlib1g zlib1g-dev libboost-filesystem-dev libboost-system-dev libboost-thread-dev libboost-program-options-dev libboost-serialization-dev libboost-chrono-dev libboost-date-time-dev libboost-random-dev libboost-iostreams-dev nvidia-cuda-toolkit 
#echo "Compile and install bison-2.7.1..."
#wget http://ftp.gnu.org/gnu/bison/bison-2.7.1.tar.gz
#tar xvfz bison-2.7.1.tar.gz
#cd bison-2.7.1
#./configure && make -j 4
#sudo make install
