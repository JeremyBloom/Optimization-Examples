// This is the Subproblem

tuple Parameter {
	int rollWidth;
	int initialPatterns; //number of patterns at the start
	int iteration;		 //current iteration number
}
{Parameter} parameters = ...; //a table with a single row
Parameter parameter= first(parameters);

tuple Order {
	key string item;
	int width;  // Not used in the Master Problem
	int amount; // Ordered amount of this item
}
{Order} orders = ...;

// From the Master Problem
tuple Dual {
	key string item;
	key int iteration;
	float price;
}
{Dual} duals = ...;

dvar int slice[orders] in 0..100000; // Number of slices of this item in the new pattern
dexpr float reducedCost = 
  1 - sum(o in orders, d in duals : o.item==d.item) d.price * slice[o];

minimize reducedCost;
subject to {
  ctFill:
    sum(o in orders) o.width * slice[o] <= parameter.rollWidth;
}

// To the Master Problem
tuple Objective {
	float value;
}
{Objective} objectives = {<reducedCost>};

// Assign an ID to the new pattern: initialPatterns+iteration number
tuple Pattern {
   key int id;
   int cost;
}
{Pattern} patterns = {<parameter.initialPatterns+parameter.iteration, 1>};

tuple Slice {
	key string item;
	key int pattern;
	int number; // Number of slices of this item in this pattern
}
{Slice} slices = {<o.item, parameter.initialPatterns+parameter.iteration, slice[o]> | o in orders};