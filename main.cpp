#include "ctpl_stl.h"
#include <iostream>
#include <string>
#include <algorithm>
#include<chrono>
#include<iomanip>

int thread_work;
int N;

long long pairwise(int id, const int start) {
  //std::cout << "started " <<id<<" \n";
  long long sum = 0;
  long long counter = 0;
  for(int i = start+1; i <= start+thread_work; i++)
  {
      if(i > N) break;
      for(int j = i-1; j >= 0; j--)
      {
          sum = i + j;
    	  if(sum%2 == 0)
            counter++;
      }
  }
  //std::cout << "finished " <<id<<" \n";
  return counter;
}

int main(int argc, char **argv)
{
    int x;
    std::cout<<"Enter num of threads : ";
    std::cin>>x;
    std::cout<<"Enter thread_work : ";
    std::cin>>thread_work;
    ctpl::thread_pool p(x);

    N = 100000;
    long long res = 0;
    std::vector<std::future<long long int>> vec;
    auto start = std::chrono::high_resolution_clock::now();
    for(int i = 0; i < N; i = i + thread_work)
    {
        vec.push_back(p.push(pairwise,i));
    }
    for(int i = 0; i < vec.size(); i++) res += vec[i].get();
    std::cout<<"total is "<<res<<std::endl;
    auto end = std::chrono::high_resolution_clock::now();
    double time_taken = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
    time_taken *= 1e-9;
    std::cout << "Time taken by program is : " << std::fixed << time_taken << std::setprecision(9)<<" secs"<<std::endl;
    return 0;
}
