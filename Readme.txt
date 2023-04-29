Code with four Filters.


Loose, Strict, Vertex Exact Edge Approx, Edge Bucket


Usage:

Compilation:
g++ -std=c++14 filter.cpp veo.cpp graph.cpp -o filter_curr2

Code Run 

./filter_curr2  input_graph_dataset_file  choice  Sim_Threshold  graph_dataset_size  Resulting_Directory.

Resulting_Directory Will contain all the output stats file.




stuti@d3113f5ea26b:~/Filter_Veo_Label_all_Filters_int_label_other_Filter$ g++ -std=c++14 filter.cpp veo.cpp graph.cpp -o filter_curr2
stuti@d3113f5ea26b:~/Filter_Veo_Label_all_Filters_int_label_other_Filter$ 
stuti@d3113f5ea26b:~/Filter_Veo_Label_all_Filters_int_label_other_Filter$ 
stuti@d3113f5ea26b:~/Filter_Veo_Label_all_Filters_int_label_other_Filter$ ./filter_curr2 dataset/data_5500.txt 3 96 5502 stat_5502
Usage 1 
Graph Dataset parsed.
Graph Dataset sorted.
GSimJoin: VEO Similarity(filters)
Choice: 3
Similarity Score Threshold: 96
Dataset Size: 5502
ubound: 1.08333
Postfix processing Done 
Loose Filter Count: 1891175
Strict Filter Count: 1872154
 Vertex Exact Edge Approx Count : 569219
Final Similar Pair Count: 2
Memory used: 305.168 MB
Total Time Taken: 203955 milliseconds

stuti@d3113f5ea26b:~/Filter_Veo_Label_all_Filters_int_label_other_Filter$ ./filter_curr2 dataset/data_5500.txt 5 96 5502 stat_5502
Usage 1 
Graph Dataset parsed.
Graph Dataset sorted.
GSimJoin: VEO Similarity(filters)
Choice: 5
Similarity Score Threshold: 96
Dataset Size: 5502
ubound: 1.08333
Max rank is 5051
no_of_buckets is 10
buckets size 506
Postfix processing Done 
Loose Filter Count: 1891175
Strict Filter Count: 1872154
 Vertex Exact Edge Approx Count : 569219
Edge Bucket Filter Count: 117
Final Similar Pair Count: 2
Memory used: 409.816 MB
Total Time Taken: 4499 milliseconds
