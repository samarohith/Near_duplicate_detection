#include "veo.h"
#include "ctpl_stl.h"
#include <set>
#include <iterator>
#include<string>
#include <iostream>
#include <algorithm>
#include<chrono>
#include<iomanip>
#include<unistd.h>
#include<vector>

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

// prints correct usage of program in case of an error
void usage();

// loose:   ./filter inp_file 1 simScore_threshold dataset-size res-file
// strict:  ./filter inp_file 2 simScore_threshold mismatch dataset-size res-file
//		     		  false/true : 0/1
// static:  ./filter inp_file 3 simScore_threshold mismatch noofbuckets dataset-size res-file
// dynamic: ./filter inp_file 4 simScore_threshold mismatch noofbuckets dataset-size res-file

//Taking as ordered_set. set bcoz duplicates are not allowed. ordered bcoz I need in Increasing order.
set<char> global_vrtxlbl_set;
unordered_map<char, unsigned> global_vrtxlbl_map;  //to map global vertex label to unsigned numeric.
unordered_map<string, unsigned> global_edgetype_map;

vector<Graph> graph_dataset;
//unsigned long looseCount = 0; // No. of graphs filtered by loose size filter
//unsigned long strictCount = 0; // No. of graphs filtered by strict size filter
//unsigned long vrtxOvrlapEdgeStrict = 0; //Vertex Overlap Edge Strict Filter
unsigned long PrefixFilterCount = 0; // No. of graphs filtered by prefix filter
//unsigned long PostfixFilterCount = 0; // No. of graphs filtered by dynamic index filter
unsigned long mismatchCount = 0; // No. of graphs filtered by mismatching filter
//unsigned long partitionCount = 0; // No. of graphs filtered by partition filter
//unsigned long simPairCount = 0; // No. of graph pairs having similarity score > threshold
//bool out = false; // a flag used to indicate whether graph is pruned or not
double simScore; // similarity score
bool isBucket;
int choice;
double simScore_threshold;
unordered_map<unsigned, vector<pair<unsigned, double>>> g_res;
int thread_work;
mutex mtx;



void printingAndWritingInitialStatistics(int choice,double simScore_threshold,int dataset_size,const string res_dir,bool mismatch,int no_of_buckets)
{
	cout << "GSimJoin: VEO Similarity(filters)" << endl;
	cout << "Choice: " << choice << endl;
	cout << "Similarity Score Threshold: " << simScore_threshold << endl;
	cout << "Dataset Size: " << dataset_size << endl;

	ofstream stat_file(res_dir+"/stat_file.txt");
	stat_file << "GSimJoin: VEO Similarity(filters)" << endl;
	stat_file << "Choice: " << choice << endl;
	stat_file << "Similarity Score Threshold: " << simScore_threshold << endl;
	stat_file << "Dataset Size: " << dataset_size << endl;
	stat_file.close();
}
void printingAndWritingFinalStatistics(int choice,unsigned long looseCount,unsigned long strictCount, unsigned long vrtxOvrlapEdgeStrict, unsigned long PrefixFilterCount, bool isBucket,unsigned long PostfixFilterCount ,bool mismatch,unsigned long mismatchCount,unsigned long simPairCount,int totalTimeTaken,const string res_dir,vector<long long int>& global_score_freq,unordered_map<unsigned, vector<pair<unsigned, double>>>& g_res)
{
    // Displaying stat file...
	/*
	if(choice >= 1)
		cout << "Loose Filter Count: " << looseCount << endl;
	if(choice >= 2 )
		cout << "Strict Filter Count: " << strictCount << endl;

	if(choice >= 3)
	{
		cout<<" Vertex Exact Edge Approx Count : "<<vrtxOvrlapEdgeStrict<<endl;
	}


	if(choice >= 4)
        cout << "Prefix Filter Count: " << PrefixFilterCount << endl;

	if(choice >= 5)
        cout << "Edge Bucket Filter Count: " << PostfixFilterCount << endl;
	*/


	//cout << "Final Similar Pair Count: " << simPairCount << endl;
	cout << "Memory used: " << memoryUsage() << " MB" << endl;
	cout <<"Total Time Taken: "<< totalTimeTaken << " milliseconds" << endl;

    ofstream stat_file(res_dir+"/stat_file.txt");
	//stat_file.open(res_dir+"/stat_file.txt", ios::app);
	// Writing counts to stat file
	if(choice >= 1)
		stat_file << "Loose Filter Count: " << looseCount << endl;
	if(choice >= 2 )
		stat_file << "Strict Filter Count: " << strictCount << endl;

	if(choice == 3)
		stat_file << "Vertex overlap Strict Edge  Count: " <<vrtxOvrlapEdgeStrict<<endl;

	if(choice >= 4)
        {
                stat_file << "Prefix Filter Count: " << PrefixFilterCount << endl;
        }

        if(choice >= 5)
        {
                stat_file << "Edge Bucket Filter Count: " << PostfixFilterCount << endl;
        }



	stat_file << "Final Similar Pair Count: " << simPairCount << endl;

	stat_file << "Memory used: " << memoryUsage() << " MB" << endl;
	stat_file <<"Total Time Taken: "<< totalTimeTaken << " milliseconds" << endl;
	stat_file.close();

	//ofstream freq_file("./"+res_dir+"/freq_distr_file.txt");
	//freq_file.close();

	ofstream all_graph_file("./"+res_dir+"/all_graph_file.txt");
	// Writing the result-set for each graph to the file for each graph
	for(auto g1 = g_res.begin(); g1 != g_res.end(); g1++)
	{
		for(auto g2 = g_res[g1->first].begin(); g2 != g_res[g1->first].end(); g2++)
		{
			all_graph_file << g1->first << " " << g2->first << " " << g2->second << endl;
		}
	}
	all_graph_file.close();

}
vector<unsigned long> helper(int id, VEO &veo_sim, const int start)
{
	//cout<<start+1<<" "<<start+thread_work<<endl;
	unsigned long looseCount = 0;
	unsigned long strictCount = 0;
	unsigned long vrtxOvrlapEdgeStrict = 0;
	unsigned long simPairCount = 0;
	unsigned long PostfixFilterCount = 0, s = 0;
	bool out = false;

	for(int g1 = start+1; g1 <= start+thread_work; g1++)
	{
		if(g1 >= graph_dataset.size()) break;
		//cout<<start+1<<" "<<start+thread_work<<endl;
		long double currSize = graph_dataset[g1].vertexCount + graph_dataset[g1].edgeCount;

		unordered_set<unsigned long>sizeFilteredGraphSet;
		//loose bound of PrevSize
        long double minPrevSize = ceil(currSize/(long double)veo_sim.ubound);

		double vIntersection, eIntersection, exact_common_vrtx;

		for(int g2 = g1-1; g2 >= 0; g2--)
		{
			double common = 0;

			out = false;
			// size of current graph g2
			long double PrevSize = graph_dataset[g2].vertexCount + graph_dataset[g2].edgeCount;

			// loose filter
			if(PrevSize >= minPrevSize)
				looseCount++;
			else
				break;

			if(choice > 1 ) // Strict Filter
			{

				vIntersection = min(graph_dataset[g1].vertexCount, graph_dataset[g2].vertexCount);
                eIntersection = min(graph_dataset[g1].edgeCount, graph_dataset[g2].edgeCount);
                double minIntersection = vIntersection + eIntersection;
				double strictBound = (double)200.0*minIntersection/(currSize+PrevSize);

				//strict filter
				if(simScore_threshold <= strictBound)
					strictCount++;
				else continue;
			}
			if(choice >= 3) // vertex overlap + strict for Edge only (Min  Edges count)
			{

				exact_common_vrtx = intersection_vertices(graph_dataset[g1], graph_dataset[g2]);  //This is Exact Vertex Overlap.
				double vrtx_ovrlap_edge_strict = (double)200.0 *((exact_common_vrtx + eIntersection)/(currSize + PrevSize));
				if(simScore_threshold <= vrtx_ovrlap_edge_strict)
				{
					vrtxOvrlapEdgeStrict++;  //sizeFilteredGraphSet.insert(g2);
				}
				else continue;
			}

			if(choice == 5) //Edge Bucket Filter.
	       {

	            chrono::high_resolution_clock::time_point cl3 = chrono::high_resolution_clock::now();
	            unsigned no_of_buckets =10;
	            if(!out) {
	            	out = veo_sim.PostfixFilter(graph_dataset[g1], graph_dataset[g2], g1, g2, simScore_threshold, isBucket, no_of_buckets, exact_common_vrtx, PostfixFilterCount);
					//out = p.first;
					//PostfixFilterCount += p.second;
	            	chrono::high_resolution_clock::time_point cl4 = chrono::high_resolution_clock::now();
	            	double postfixTimeTaken = (clocksTosec(cl3,cl4));
	            }
	       }

			if(out) continue;

			if(!out)
			{
				// naive computation of VEO similarity
				if(choice >=3)
					simScore = veo_sim.computeSimilarity(graph_dataset[g1], graph_dataset[g2], exact_common_vrtx);
				else
				{
					simScore = veo_sim.computeSimilarity(graph_dataset[g1], graph_dataset[g2], common);
				}
				if(simScore >= simScore_threshold)
				{
					mtx.lock();
					g_res[graph_dataset[g1].gid].push_back(make_pair(graph_dataset[g2].gid, simScore));
					mtx.unlock();
					simPairCount++;
				}
			}
		}
	}
	//cout<<"hey "<<start<<endl;
	return {looseCount, strictCount, vrtxOvrlapEdgeStrict, simPairCount, PostfixFilterCount};
}

int main(int argc, char const *argv[])
{
	if(argc < 6)
		usage();

	//vector<Graph> graph_dataset; // input graph dataset

	// applying mismatch filter
	bool mismatch = false;
	// no. of buckets used in dynamic filter
	int no_of_buckets = 10;
	// true if no. of buckets is greater than 0
	isBucket = false;

	choice = stoi(argv[2]);

	// Verifying args
	if(choice >= 1 && choice <= 6)
	{
		cout<<"Usage 1 \n";
		if(argc!=8)
			usage();
	}

	else
	{
		cout<<choice <<" ..\n";
		cout<<"Usage 4 \n";
		usage();
	}

	simScore_threshold = stod(argv[3]); // similarity threshold
	int dataset_size = stoi(argv[4]); // size of input dataset
	const string res_dir = argv[5]; // directory in which all stat files would be stored
	int num_threads = stoi(argv[6]);
	thread_work = stoi(argv[7]);
	mkdir(res_dir.c_str(),0777);

	ifstream dataset_file(argv[1]);
	if(!dataset_file.is_open())
	{
		cerr << "Unable to open dataset file" << endl;
		exit(0);
	}
	//int dataset_size;
	//dataset_file >> dataset_size;
	//dataset_file.seekg(0, dataset_file.beg);
	// parsing input graph-dataset
	parseGraphDataset(dataset_file, graph_dataset, dataset_size);
	cout << "Graph Dataset parsed.\n";

	// Sorts vertex and edge set of each graph in the dataset
	sortGraphDataset(graph_dataset);
	// sorts the graph dataset
	sort(graph_dataset.begin(), graph_dataset.end(), graphComp);
	cout << "Graph Dataset sorted.\n";


	printingAndWritingInitialStatistics(choice,simScore_threshold,dataset_size,res_dir,mismatch,no_of_buckets);



	VEO veo_sim = VEO(simScore_threshold);

	vector<long long int> global_score_freq(102, 0); // stores sim-score frequency distribution of the dataset

	if(choice >= 5)
                veo_sim.index(graph_dataset, choice, isBucket, no_of_buckets); // index input graphs
	if(choice == 5)
                veo_sim.Preprocess_Postfix(graph_dataset, no_of_buckets);        // preprocessing for suffix filter
    cout<<"Postfix processing Done \n";



	//int x;
    //std::cout<<"Enter num of threads : ";
    //std::cin>>x;
    //std::cout<<"Enter thread_work : ";
    //std::cin>>thread_work;
    ctpl::thread_pool p(num_threads);

 	// timestamping start time
	chrono::high_resolution_clock::time_point cl0 = chrono::high_resolution_clock::now();
	std::vector<std::future<vector<unsigned long>>> vec;
	std::vector<vector<unsigned long>> count_vector;
	//cout<<"size : "<<graph_dataset.size()<<endl;
    for(int i = 0; i < graph_dataset.size(); i = i + thread_work)
    {
	    //cout<<"\ni is "<<i<<endl;
        vec.push_back(p.push(helper, veo_sim, i));
    }
	unsigned long looseCount = 0;
	unsigned long strictCount = 0;
	unsigned long vrtxOvrlapEdgeStrict = 0;
	unsigned long PostfixFilterCount = 0;
	unsigned long simPairCount = 0;

	for(int i = 0; i < vec.size(); i++)
		count_vector.push_back(vec[i].get());

	for(int i = 0; i < count_vector.size(); i++)
	{
		looseCount += count_vector[i][0];
		strictCount += count_vector[i][1];
		vrtxOvrlapEdgeStrict += count_vector[i][2];
		simPairCount += count_vector[i][3];
		PostfixFilterCount += count_vector[i][4];
	}

	cout<<"Loose Filter Count: "<<looseCount<<endl;
	cout<<"Strict Filter Count: "<<strictCount<<endl;
	cout<<"Vertex Exact Edge Approx Count: "<<vrtxOvrlapEdgeStrict<<endl;
	if(choice >= 5)
		cout << "Edge Bucket Filter Count: " << PostfixFilterCount << endl;
	cout<<"Final Similar Pair Count: "<<simPairCount<<endl;

 	// timestamping end time
	chrono::high_resolution_clock::time_point cl2 = chrono::high_resolution_clock::now();
	int totalTimeTaken = (clocksTosec(cl0,cl2));

    printingAndWritingFinalStatistics(choice,looseCount,strictCount, vrtxOvrlapEdgeStrict, PrefixFilterCount, isBucket, PostfixFilterCount, mismatch,mismatchCount,simPairCount,totalTimeTaken,res_dir,global_score_freq,g_res);

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

        //for (itr = global_vrtxlbl_set.begin(); itr != global_vrtxlbl_set.end(); itr++)
        for(int i=0; i<=100; i++)
	{

                //char lbl = *itr;
                //cout<<lbl<<" ";
                for(int j=i; j<=100; j++)
                //for(itr2 =itr; itr2 != global_vrtxlbl_set.end(); itr2++)
                {
			string str1 =to_string(i), str2 =to_string(j);
			string edge_type_str =str1;
			edge_type_str += '-';
			edge_type_str += str2;
                        //cout<<j<<" j  "<<edge_type_str<<endl;
                        global_edgetype_map[edge_type_str] = edge_type_unsign++;
                }

        }


	graph_dataset.resize(dataset_size);

	for(auto g_iter = graph_dataset.begin(); g_iter != graph_dataset.end(); g_iter++)
	{
		g_iter->readGraph(inp, vlblCount,  global_vrtxlbl_map, global_edgetype_map);
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

// prints correct usage of program in case of an error
void usage(){
	cerr << "usage: ./filter input_file choice simScore-threshold dataset-size res-dir" <<endl;

	cerr << "Available choices: " << endl;
	cerr << endl;

	cerr << "1 loose : 1" << endl;
	cerr << endl;

	cerr << "2 loose + strict 			  : 2, mismatch=false noofbuckets=0" << endl;
	cerr << "3 loose + strict + mismatch  : 2, mismatch=true  noofbuckets=0" << endl;
	cerr << endl;

	cerr << "4 loose + strict + static 				: 3, mismatch=false noofbuckets = 0" << endl;
	cerr << "5 loose + strict + static + 2 buckets  : 3, mismatch=false noofbuckets = 2" << endl;
	cerr << "6 loose + strict + static + 5 buckets  : 3, mismatch=false noofbuckets = 5" << endl;
	cerr << "7 loose + strict + static + 10 buckets : 3, mismatch=false noofbuckets = 10" << endl;
	cerr << "8 loose + strict + static + 10 buckets : 3, mismatch=true  noofbuckets = 10" << endl;
	cerr << "9  loose + strict + dynamic 			  :  4, mismatch=false noofbuckets = 0" << endl;
	cerr << "10 loose + strict + dynamic + 2 buckets  :  4, mismatch=false noofbuckets = 2" << endl;
	cerr << "11 loose + strict + dynamic + 5 buckets  :  4, mismatch=false noofbuckets = 5" << endl;
	cerr << "12 loose + strict + dynamic + 10 buckets :  4, mismatch=false noofbuckets = 10" << endl;
	cerr << "13 loose + strict + dynamic + 10 buckets :  4, mismatch=true  noofbuckets = 10" << endl;
	cerr << endl;

	cerr << "loose:   ./filter inp_file 1 simScore_threshold dataset-size res-dir\n";
	cerr << "strict:  ./filter inp_file 2 simScore_threshold mismatch dataset-size res-dir\n";
//		     											  false/true : 0/1
	cerr << "static:  ./filter inp_file 3 simScore_threshold mismatch noofbuckets dataset-size res-dir\n";
	cerr << "dynamic: ./filter inp_file 4 simScore_threshold mismatch noofbuckets dataset-size res-dir\n";
	exit(0);
}
