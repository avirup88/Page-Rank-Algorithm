# Simple Page-Rank-Algorithm

I have broken the algorithm into four map-reduce jobs in order to make it easier to perform each of the tasks and to eliminate any type of redundant activities. Please execute the file PageRankMain.java to execute the programs which have two input arguments : one for input file path and the other as the output file path.

## Mapper-0 and Reducer-0  

It calculates the initial number of nodes (N) from the adjacency graph and stores it into the num_nodes file. This step was required as Java global variables tend to reset when multiple JVMs are involved to execute a single map reduce task. This was the issue when the job was executed in AWS.

## Mapper-1 and Reducer-1  

This inserts the initial page rank value of 1/N into each of the input file rows of the adjacency graph.

## Mapper-2 and Reducer-2

It calculates overall page rank with 8 iterations. The mapper breaks down the links from the adjacency graph into keys and divides the initial page rank by the number of out-links for each title. The reducer accumulates the keys (which are the links and titles) and sums over the page rank values to create the next page rank value for the next iteration. This continues for 8 iterations as per the problem. After 1st and 8th iteration, 2 files namely iter1.out and iter8.out are also generated as per
the requirement.

## Mapper-3 and Reducer-3

It performs the filtering of the rows for the values >= 5/N and performs sorting operation in the descending order of page rank which is not the default behavior of map reduce jobs. In order to achieve this a separate sorter class is written here which helps to sort it in descending order.


## Important Note

This script doesn't handle the cases of dead ends and spider traps.
