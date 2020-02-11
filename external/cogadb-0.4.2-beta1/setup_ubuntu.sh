#!/bin/bash
sudo apt-get install gcc g++ make cmake libboost1.54-all-dev libtbb-dev libreadline6 libreadline6-dev bison flex libsparsehash-dev
#genomics extension
sudo apt-get install libbam-dev samtools
#for documentation
sudo apt-get install doxygen doxygen-gui graphviz texlive-bibtex-extra texlive-fonts-extra texlive-fonts-extra-doc ghostscript texlive-fonts-recommended
sudo apt-get install xsltproc libxslt1-dev
#NUMA support
sudo apt-get install libnuma1 libnuma-dev
exit 0
