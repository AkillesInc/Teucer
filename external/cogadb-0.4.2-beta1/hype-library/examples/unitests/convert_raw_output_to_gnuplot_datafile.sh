cat cpu.data | awk '{print $9"\t"$13"\t"$17"\t"$20}' | sed -e 's/ns//g' | sed -e 's/(//g' | sed -e 's/)//g' | sed -e 's/%//g' > cpu_algorithm.data
