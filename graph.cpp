#include "graph.h"
#include<string>


 // adds an edge to the graph
void Graph:: pushEdge(unsigned u,unsigned v, unsigned ec){
	if(u>v)
		edges.push_back(make_pair(u, v));
	else
		edges.push_back(make_pair(v, u));
	degrees[vid_to_ind[u]]++;
	degrees[vid_to_ind[v]]++;
}

// reads the graph from input file
void Graph:: readGraph(istream &inp, unsigned vlblCount, unordered_map<char, unsigned> global_vrtxlbl_map, unordered_map<string, unsigned> global_edgetype_map)
{
	char tag;
	// first line should be of the format "g vertexCount(unsigned int) edgeCount(unsigned int) gid(unsigned int)"
	inp >> tag; // the tag 'g'
	inp >> vertexCount; // the no. of vertices in the graph
	inp >> edgeCount; // the no. of edges in the graph
	inp >> gid; // the graph-id of the graph


	cout <<gid<<endl;
	unsigned lblcont =101;
	unsigned elblcont = (lblcont-1)*lblcont;
	elblcont =(elblcont/2) +lblcont;
	//unsigned min_edgeLbl = min(elblcont, edgeCount);
	//cout<<" elblcont "<<elblcont<<" edgeCount "<<edgeCount<<endl;
	//cout<<"min_edgeLbl "<<min_edgeLbl<<endl;
	vertices.resize(vertexCount);
	cout<<"1"<<endl;
	degrees.assign(vertexCount, 0);
	cout<<"2"<<endl;
	vrtxLbl.resize(vertexCount);
	cout<<"3"<<endl;
  edgeTypeVectr.resize(elblcont, 0);
	cout<<"4"<<endl;
	edgeType.resize(edgeCount);
	cout<<"5"<<endl;
	unsigned ind = 0;
	unsigned vid, src_vtx, dest_vtx, ec;
	unsigned vc;
	cout <<gid<<endl;
	//unsigned vc,sc,dc;  //Vertex type -- vc
	for(int vtx_ind=0; vtx_ind < vertexCount; vtx_ind++)
	{
		// each line for each vertex should be in the format like: "v vid(unsigned int)"
		inp >> tag >> vid >> vc; // the tag 'v' along with the vertex-id
		vertices[vtx_ind] = (vc);//vid;
		vid_to_ind[vid] = vtx_ind; // mapping vertex-id to its index
		vid_to_vc[vid] = vc;
		vrtxLbl[vtx_ind] = vc;
	}
	//cout <<gid<<endl;
	for(int e_ind = 0; e_ind < edgeCount; e_ind++)
	{
		// each line for each edge should be in the format like: "e vid_src(unsigned int) vid_dest(unsigned int)"
		inp >> tag >> src_vtx  >> dest_vtx >> ec; // the tag 'e' along with the source and destination vertex-ids
		// Undirected graph : adding edge source to destination and destination to source
		if(vid_to_vc[src_vtx] < vid_to_vc[dest_vtx])
                {
			string str1 =to_string(vid_to_vc[src_vtx]), str2 = to_string(vid_to_vc[dest_vtx]);
                        //string eType{str1 , '-', str2 };
			string eType =str1;
			eType += '-';
			eType += str2;
			edgeType[e_ind] = make_pair(vid_to_vc[src_vtx],  vid_to_vc[dest_vtx]);
                        unsigned unsign_eType = global_edgetype_map[eType];
                        //cout<<eType<<" is eType  "<<unsign_eType<<"    "<<edgeTypeVectr[unsign_eType]<<endl;
                        edgeTypeVectr[unsign_eType]++;
                        //cout<<edgeTypeVectr[unsign_eType]<<endl;
                }
                else
                {
			string str1 =to_string(vid_to_vc[dest_vtx]), str2 = to_string(vid_to_vc[src_vtx]);
                        //string eType{str1 , '-', str2 };
                        string eType =str1;
                        eType += '-';
                        eType += str2;
                        edgeType[e_ind] = make_pair(vid_to_vc[dest_vtx],  vid_to_vc[src_vtx]);
                        unsigned unsign_eType = global_edgetype_map[eType];
                        //cout<<eType<<" is eType "<<unsign_eType<<"    "<<edgeTypeVectr[unsign_eType]<<endl;
                        edgeTypeVectr[unsign_eType]++;

		}


		pushEdge(vid_to_vc[src_vtx], vid_to_vc[dest_vtx], ec);
		//pushEdge(sc,dc);
	}
	//cout <<gid<<endl;
	VertexLabelVectr.resize(lblcont , 0);
        for(int i=0; i<vrtxLbl.size(); i++)
        {
		//if(gid ==790 && i==0)
		//	cout<<" vrtxLbl.size() = "<<vrtxLbl.size()<<endl;
		//if(gid ==790)
		//	cout<<vrtxLbl[i]<<" \n";
                unsigned vlbl = vrtxLbl[i];
                VertexLabelVectr[vlbl]++;

        }
	/*if(gid ==790) {
	cout<<" \n ................................................................\n\n";
	for(int j=0; j< VertexLabelVectr.size(); j++)
		cout<<j<<"   "<<VertexLabelVectr[j]<<endl;
	} */
	//displayGraph();
}

// prints details of the graph
void Graph::displayGraph()
{

	cout <<"g "<< gid << ":" << endl;
	cout <<"Vertex Count: "<< vertexCount << endl;
	cout <<"Edge Count: "<< edgeCount << endl;

	for(int vtx_ind = 0; vtx_ind<vertexCount; vtx_ind++)
	{
		cout << "v" << vtx_ind << ": " << vertices[vtx_ind] << endl;
	}
	for(int edg_ind = 0; edg_ind<edgeCount; edg_ind++)
	{
		cout << "e" << edg_ind << ": " << edges[edg_ind].first << " " << edges[edg_ind].second << endl;
	}

}
