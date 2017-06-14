/*********************************************
 * OPL 12.6.0.0 Model
 * Author: bloomj
 * Creation Date: Jul 3, 2015 at 9:44:52 AM
 *********************************************/
 
 //Deterministic Warehousing
 //Uses the tight formulation
 
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
 	float amount;		//pallets
 }
 
 tuple CapacityDual{
//  key string model;
 	key string location;
 	key int iteration;
 	float shadowPrice;	// $/pallet/period
 }
 
 tuple DemandDual {
//  key string model;
 	key int iteration;
 	float shadowPrice;	// $/pallet/period
 }
 
  tuple SupplyDual {
//  key string model;
 	key string location;
 	key int iteration;
 	float shadowPrice;	// $/period
 }
 
 //Input Data
 //Parameters parameter=...;
 
 {Warehouse} warehouses= ...;
 
 {Store} stores= ...;
 
 {Route} routes= ...;
 
 {Demand} demands= ...;
 float demand[routes]= [r: d.amount | r in routes,  d in demands: r.store==d.store]; //demand at the store at the end of route r
 
 {Scenario} scenarios= ...;
 Scenario scenario= first(scenarios); //scenarios is a singleton set
 
 //Optimization Model
 dvar boolean open[warehouses];
 dvar float+ capacity[warehouses];		//pallets
 dvar float+ ship[routes] in 0.0..1.0;	//percentage of each store's demand shipped on each route
 
 dexpr float capitalCost=	sum(w in warehouses) (w.fixedCost*open[w] + w.capacityCost*capacity[w]);
 dexpr float operatingCost=	sum(r in routes) r.shippingCost*demand[r]*ship[r];
 dexpr float totalCost=		sum(w in warehouses) (w.fixedCost*open[w] + w.capacityCost*capacity[w]) +
 							sum(r in routes) r.shippingCost*demand[r]*ship[r];
 
 constraint ctCapacity[warehouses];
 constraint ctDemand[stores];
 constraint ctSupply[routes];
 
 minimize totalCost;					// $/yr
 subject to {
 	 
 	forall(w in warehouses)
//	  Cannot ship more out of a warehouse than its capacity
 	  ctCapacity[w]: capacity[w] >= sum(r in routes: r.location==w.location) demand[r]*ship[r];
 	 
	forall(s in stores)
//    Must ship at least 100% of each store's demand
	  ctDemand[s]: sum(r in routes: r.store==s.storeId) ship[r] >= 1.0;
   	   
	forall(r in routes, w in warehouses: w.location==r.location)
//	  Can only ship along a supply route if its warehouse is open	  
	  ctSupply[r]: -ship[r] >= -open[w];	//ship[r] <= open[w]
   
 }
 
 //Output Data
 {Objective} objectives= {
  <"Warehousing", "capitalCost", scenario.id, 0, capitalCost>,
  <"Warehousing", "operatingCost", scenario.id, 0, operatingCost>,
  <"Warehousing", "totalCost", scenario.id, 0, totalCost>};
  };
 
 {Shipment} shipments= {<r.location, r.store, scenario.id, 0, ship[r]*d.amount> | r in routes, d in demands: r.store==d.store && ship[r]>0.0};
 
 {OpenWarehouse} openWarehouses= {<w.location, scenario.id, 0, open[w], capacity[w]> | w in warehouses};
 
 execute{
 	writeln("Objectives: ", objectives);
 	writeln();
 	writeln("Open Warehouses")
 	for(var w in openWarehouses) {
 		if(w.open>0) 
 			writeln(w.location, " ", w.capacity);	
 	}
 	writeln();
 	writeln("Shipments")
 	for(var t in shipments) {
 	 	if(t.amount>0.0)
 	 		writeln(t.location, " -> ", t.store, " : ", t.amount);
 	}
 	writeln();
 	
 	writeln("openWarehouses= ", openWarehouses);
 
 }
 