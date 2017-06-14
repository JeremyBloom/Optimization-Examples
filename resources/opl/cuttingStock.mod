// This is the Master Problem

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

// From the Subproblem
tuple Pattern {
   key int id;
   int cost;
}
{Pattern} patterns= ...;

tuple Slice {
	key string item;
	key int pattern;
	int number; // Number of slices of this item in this pattern
}
{Slice} slices= ...;

dvar float rolls[patterns] in 0..1000000; // Number of rolls cut with each pattern
constraint ctOrder[orders];

dexpr float cost = sum( p in patterns ) p.cost * rolls[p];

minimize cost;
  
subject to {
  forall( o in orders ) 
    ctOrder[o]: // Cut enough rolls to fill the order amount for each item
      sum( p in patterns, c in slices : p.id==c.pattern && o.item==c.item )
         c.number * rolls[p] >= o.amount;
}

// The optimal solution
tuple Objective {
	float value;
}
{Objective} objectives = {<cost>};

tuple Usage {
	key int pattern;
	float rolls; // Number of rolls cut with the pattern
}
{Usage} usages = {<p.id, rolls[p]> | p in patterns};


// To the Subproblem
tuple Dual {
	key string item;
	key int iteration;
	float price;
}
{Dual} duals = {<o.item, parameter.iteration, dual(ctOrder[o])> | o in orders};

execute DISPLAY {
  writeln("usages = ", rolls);
  for(var p in patterns) 
    writeln("Use of pattern ", p, " is : ", rolls[p]);
}