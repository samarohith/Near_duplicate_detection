#include "veo.h"
#include <set>
#include <iterator>
#include<string>
#include<bits/stdc++.h>


using namespace std;

// For parsing the input graph dataset
void parseGraphDataset(ifstream &inp, vector<Graph> &graph_dataset, int &dataset_size);

// Sorts vertex and edge set of each graph in the dataset
void sortGraphDataset(vector<Graph> &graph_dataset);

// Graph comparator
bool graphComp(Graph &g1, Graph &g2);

// Returns the time from start to end in milliseconds
unsigned long long int clocksTosec(chrono::high_resolution_clock::time_point start, chrono::high_resolution_clock::time_point end);

// Displays the memory used by the program(in MB)
double memoryUsage();


// loose:   ./filter inp_file 1 simScore_threshold dataset-size res-file
// strict:  ./filter inp_file 2 simScore_threshold mismatch dataset-size res-file
//		     		  false/true : 0/1
// static:  ./filter inp_file 3 simScore_threshold mismatch noofbuckets dataset-size res-file
// dynamic: ./filter inp_file 4 simScore_threshold mismatch noofbuckets dataset-size res-file

//Taking as ordered_set. set bcoz duplicates are not allowed. ordered bcoz I need in Increasing order.
set<char> global_vrtxlbl_set;
unordered_map<char, unsigned> global_vrtxlbl_map;  //to map global vertex label to unsigned numeric.
unordered_map<string, unsigned> global_edgetype_map;





void printingAndWritingFinalStatistics(int totalTimeTaken,const string res_dir)
{

	cout << "Memory used: " << memoryUsage() << " MB" << endl;
	cout <<"Total Time Taken: "<< totalTimeTaken << " milliseconds" << endl;

  ofstream stat_file(res_dir+"/stat_file.txt");

	stat_file << "Memory used: " << memoryUsage() << " MB" << endl;
	stat_file <<"Total Time Taken: "<< totalTimeTaken << " milliseconds" << endl;
	stat_file.close();
}

int main(int argc, char const *argv[])
{
	if(argc<4)
		exit(0);

	vector<Graph> graph_dataset; // input graph dataset
	int choice = stoi(argv[2]);


	double simScore_threshold = stod(argv[3]); // similarity threshold

	const string res_dir = argv[4]; // directory in which all stat files would be stored
	mkdir(res_dir.c_str(),0777);

	int dataset_size = -1;
	ifstream dataset_file(argv[1]);
	if(!dataset_file.is_open())
	{
		cerr << "Unable to open dataset file" << endl;
		exit(0);
	}
	ifstream fin(argv[1]);
	string line;
	while(fin)
	{
		getline(fin, line);
		break;
	}
	dataset_size = stoi(line);
	fin.close();
	//cout<<dataset_size<<endl;
	// parsing input graph-dataset

	parseGraphDataset(dataset_file, graph_dataset, dataset_size);
	cout << "Graph Dataset parsed.\n";

	// Sorts vertex and edge set of each graph in the dataset
	sortGraphDataset(graph_dataset);
	// sorts the graph dataset
	sort(graph_dataset.begin(), graph_dataset.end(), graphComp);
	cout << "Graph Dataset sorted.\n";

	double simScore; // similarity score

	//printingAndWritingInitialStatistics(choice,simScore_threshold,dataset_size,res_dir,mismatch,no_of_buckets);

	VEO veo_sim = VEO(simScore_threshold);

	// Result-set for each graph as vector of other graph's gid and their similarity score as double
	unordered_map<unsigned, vector<pair<unsigned, double>>> g_res; // stores graph pair with similarity score
	// Freq of simScore with range of 1% 0-1, 1-2, 1-3, ... 99-100%
	vector<long long int> global_score_freq(102, 0); // stores sim-score frequency distribution of the dataset


 	// timestamping start time
	chrono::high_resolution_clock::time_point cl0 = chrono::high_resolution_clock::now();

	double common = 0;

	for(int g1 = 0; g1 < graph_dataset.size(); g1++)
	{

	   cout<<g1<<" ............. "<<endl;
			for(int g2 = g1-1; g2 >= 0; g2--)
			{
			// naive computation of VEO similarity
					//cout<<g1<<" "<<g2<<endl;
					simScore = veo_sim.computeSimilarity(graph_dataset[g1], graph_dataset[g2], common);
			}

		}

	chrono::high_resolution_clock::time_point cl2 = chrono::high_resolution_clock::now();
	int totalTimeTaken = (clocksTosec(cl0,cl2));

  printingAndWritingFinalStatistics(totalTimeTaken,res_dir);

	return 0;
}

// parses the input graph dataset and query graph
void parseGraphDataset(ifstream &inp, vector<Graph> &graph_dataset, int &dataset_size)
{
	int i=0,size, vlblCount;
  unsigned vlbl_unsigned =0, edge_type_unsign =0;
	inp >> size;
	//inp >>vlblCount;
	if(dataset_size == -1)
		dataset_size= size;
/*	for(int i=0; i<vlblCount; i++)
        {
                char vlbl_char;
                inp >>vlbl_char;
                cout<<vlbl_char<<" Vertex label read \n";
                global_vrtxlbl_set.insert(vlbl_char);
        }

	set<char>::iterator itr;
*/
        //for (itr = global_vrtxlbl_set.begin(); itr != global_vrtxlbl_set.end(); itr
		//cout<<"hello"<<endl;
	  for(int i = 0; i <= 100; i++)
		{
	       for(int j=i; j<=100; j++)
	       {
					 string str1 =to_string(i), str2 =to_string(j);
					 string edge_type_str =str1;
					 edge_type_str += '-';
					 edge_type_str += str2;
	         global_edgetype_map[edge_type_str] = edge_type_unsign++;
	       }

	   }


	graph_dataset.resize(dataset_size);
	//cout<<"hello"<<endl;
	for(auto g_iter = graph_dataset.begin(); g_iter != graph_dataset.end(); g_iter++)
	{
		g_iter->readGraph(inp, vlblCount, global_vrtxlbl_map, global_edgetype_map);
		//cout<<graph_dataset.size()<<endl;
	}
}

bool graphComp(Graph &g1, Graph &g2)
{
	return g1.vertexCount+g1.edgeCount < g2.vertexCount+g2.edgeCount;
}

// Sorts vertex and edge set of graph dataset
void sortGraphDataset(vector<Graph> &graph_dataset)
{
	for(int i = 0; i < graph_dataset.size(); i++)
	{
		sort(graph_dataset[i].vertices.begin(), graph_dataset[i].vertices.end()); // sort vertex-set
		sort(graph_dataset[i].edges.begin(), graph_dataset[i].edges.end()); // sort edge-set
	}
}

// Returns the time from start to end in seconds
unsigned long long int clocksTosec(chrono::high_resolution_clock::time_point start, chrono::high_resolution_clock::time_point end)
{
	return (unsigned long long int)(1e-6*chrono::duration_cast<chrono::nanoseconds>(end - start).count());
}

// Displays the memory used by the program(in MB)
double memoryUsage()
{
	struct rusage r_usage;
	getrusage(RUSAGE_SELF, &r_usage);
	return r_usage.ru_maxrss/1024.0;
}
