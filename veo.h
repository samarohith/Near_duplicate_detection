#include "graph.h"
#include<unordered_set>
#include <map>




double intersection_vertices(Graph &g1, Graph &g2);

class VEO{
public:
        double ubound;
        map< pair<unsigned, unsigned>, unsigned long > fearture_freq;
        vector<vector< pair<unsigned long, unsigned long>> > rankList;
        vector<vector<pair< pair<unsigned long , unsigned long>, unsigned long>>> InvertedIndex;
        unordered_map<int,pair<int,int>> sparse_table;    // a array of hash_table
        vector<vector<vector<unsigned>>> bucket;
        vector<vector<unsigned>> graph_bucket;
        VEO(double threshold)
        {
                ubound = double((double)(200.0/threshold) -1.0);
                cout << "ubound: " << ubound << endl;
        }
        bool indexFilter(Graph &g2, Graph &g1, int index2, int index1, int mode, bool isBucket, int no_of_buckets, long unsigned &indexCount, long unsigned &partitionCount, double threshold);
        void ranking(vector<Graph> &graphDataset);
        void buildPrefix(vector<Graph> &graphDataset, int type, bool isBucket, int b);
        bool PrefixFilter(Graph &g2, Graph &g1, int index2, int index1, int mode, bool isBucket, int no_of_buckets, long unsigned &indexCount, double threshold);
        bool PositioningFilter(Graph &g2, Graph &g1, int index2, int index1, int mode, bool isBucket, int no_of_buckets, long unsigned &indexCount, double threshold);
        void Preprocess_Suffix(vector<Graph> &graph_dataset, int no_of_buckets);
        bool SuffixFilter(Graph &g2, Graph &g1, int index2, int index1, double threshold, bool isBucket, int no_of_buckets, long unsigned &SuffixFilterCount);
        bool mismatchingFilter(Graph &g1, Graph &g2, double &c, double threshold);
        void sortGraphDataset(vector<Graph> &graphDataset);
        double computeSimilarity(Graph &g1, Graph &g2, double &commonV);
        void sortVertexEdge(vector<Graph> &graphDataset);
      // void calculate_sparse_table(vector<Graph> &graph_dataset, int g_ind, long double minPrevSize);
	//void calculate_sparse_table(vector<Graph> &graph_dataset, int g_ind, unordered_set<unsigned long> sizeFilteredGraphSet); 
	void Preprocess_Postfix(vector<Graph> &graph_dataset, int no_of_buckets);
	bool PostfixFilter(Graph &g1, Graph &g2, int index1, int index2, double &threshold, bool isBucket, int no_of_buckets, double exact_common_vrtx, long unsigned &PostfixFilterCount );
	void calculate_sparse_table(vector<Graph> &graph_dataset, int g_ind, long double minPrevSize);
	void printlist(int i);
        void index(vector<Graph> &graphDataset, int type, bool isBucket, int b);
};

