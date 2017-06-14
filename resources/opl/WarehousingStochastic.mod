/*********************************************
 * OPL 12.6.0.0 Model
 * Author: bloomj
 * Creation Date: Apr 21, 2017 at 1:53:44 PM
 *********************************************/
 
 //Stochastic Warehousing
 
 //Warehousing Application Data Model
 
 tuple Parameters {
 	int nbIterations;			// positive, starts at zero, increases
 	int nbInitialConstraints;	// negative, starts at zero, decreases
 	float periods;				// periods/yr
 }
 
 tuple Warehouse {
 	key string location;
 	float fixedCost;	// $/yr
 	float capacityCost;	// $/pallet/yr
 }
 
 tuple Store {
 	key string storeId; 
 }
 
 tuple Route {
 	key string location;
 	key string store;
 	float shippingCost;	// $/pallet
 }
 
 tuple Scenario {
 	key string id;
 	float totalDemand;
 	float periods; 	//the number of periods per year during which this scenario prevails
 					//periods = scenario probability * total periods/year
 }
 
 tuple Demand {
 	key string store;
 	key string scenarioId;
 	float amount;		// pallets/period
 }
 
 tuple Objective {
	key string problem;
 	key string dExpr;
 	key string scenarioId;
	key int iteration;
	float value;   
 }
 
 tuple Shipment {
  	key string location;
 	key string store;
 	key string scenarioId;
 	key int iteration;
 	float amount; 
 }
 
 tuple OpenWarehouse {
 	key string location;
 	key string scenarioId;
 	key int iteration;
 	int open;
 	float capacity;		// pallets
 }
 
 tuple UnservedDemand {
 	key string store;
 	key int iteration;
 	key string scenarioId;
 	float amount;		//pallets
 }
  
 //Input Data
 //Parameters parameter=...;
 
 {Warehouse} warehouses= ...;
 
 {Store} stores= ...;
 
 {Route} routes= ...;
 
 {Demand} demands= ...;
 {Scenario} scenarios= ...;
 float demand[scenarios, routes]= [t: [r: d.amount | r in routes,  d in demands: r.store==d.store && d.scenarioId==t.id] | t in scenarios]; //demand at the store at the end of route r
 
 
 //Optimization Model
 dvar boolean open[warehouses];
 dvar float+ capacity[warehouses];					//pallets
 dvar float+ ship[scenarios, routes] in 0.0..1.0;	//percentage of each store's demand shipped on each route in each scenario
 
 dexpr float capitalCost= sum(w in warehouses) (w.fixedCost*open[w] + w.capacityCost*capacity[w]);
 dexpr float operatingCost[t in scenarios]= sum(r in routes) r.shippingCost*demand[t, r]*ship[t, r];
 dexpr float totalOperatingCost= sum(t in scenarios) t.periods*operatingCost[t];
 
 constraint ctCapacity[scenarios, warehouses];
 constraint ctDemand[scenarios, stores];
 constraint ctSupply[scenarios, routes];
 
 minimize capitalCost + totalOperatingCost;	// $/yr
 subject to {
 	 
 	forall(t in scenarios, w in warehouses)
//	  Cannot ship more out of a warehouse than its capacity
 	  ctCapacity[t, w]: capacity[w] >= sum(r in routes: r.location==w.location) demand[t, r]*ship[t, r];
 	 
	forall(t in scenarios, s in stores)
//    Must ship at least 100% of each store's demand
	  ctDemand[t, s]: sum(r in routes: r.store==s.storeId) ship[t, r] >= 1.0;
   	   
	forall(t in scenarios, r in routes, w in warehouses: w.location==r.location)
//	  Can only ship along a supply route if its warehouse is open	  
	  ctSupply[t, r]: -ship[t, r] >= -open[w];	//ship[r] <= open[w]
   
 }
 
 //Output Data
 {Objective} objectives= {<"Warehousing", "capitalCost", "All", 0, capitalCost>,
 						  <"Warehousing", "operatingCost", "All", 0, totalOperatingCost>} 
 	union {<"Warehousing", "operatingCost", t.id, 0, operatingCost[t]> | t in scenarios};
 
 {Shipment} shipments= {<r.location, r.store, t.id, 0, ship[t, r]*d.amount> | t in scenarios, r in routes, d in demands: 
 	r.store==d.store && d.scenarioId==t.id && ship[t, r]>0.0};
 
 {OpenWarehouse} openWarehouses= {<w.location, "All", 0, open[w], capacity[w]> | w in warehouses};
 
 execute{
 	writeln("Objectives: ", objectives);
 	writeln();
 	writeln("Open Warehouses")
 	for(var w in openWarehouses) {
 		if(w.open>0) 
 			writeln(w.location, " ", w.capacity);	
 	}
 	writeln();
/* 	writeln("Shipments")
 	for(var t in shipments) {
 	 	if(t.amount>0.0)
 	 		writeln(t.location, " -> ", t.store, " : ", t.amount);
 	}
 	writeln();
 	
 	writeln("openWarehouses= ", openWarehouses);
*/ 
 }
 
