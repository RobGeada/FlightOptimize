# FlightOptimize

Using flight data from openflights.org, FlightOpt creates a directed graph of all airports that exist in the openflights database, wherein existing flights between airports are the edges. This graph is then traversed via the A* graph search algorithm, using simple great-circle distance as the heuristic, and thus the shortest path between airports A and B can be found, a process which takes an average of 6 ms. This optimal path is referred to as a 'trip'.

By collecting data for n trips, each between two randomly selected airports, the efficiency of the sampled flight network is revealed, with the sampled network approaching the global network for large n (the global network has 9541 nodes, and thus there are theoretically ~9e7 trips total). By finding the most common airport trio in the trip dataset (an airport trio referring to three airports traversed in sequence during a trip, say LAX->JFK->Heathrow), we may find (naively) the best route to add such as to best improve the sampled network, that is, if trio A,B,C is most common, connecting A to C should have the greatest benefit to the network. With large enough n, the A to C connection is thus the connection that best improves the global flight network as a whole.  

  
