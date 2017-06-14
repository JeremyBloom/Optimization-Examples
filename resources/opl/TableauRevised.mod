/*********************************************
 * OPL 12.6.0.0 Model
 * Author: bloomj
 * Creation Date: Aug 4, 2016 at 3:32:10 PM
 *********************************************/

 //Defines a column for a boolean variable
 tuple BooleanColumn {
  	key string variable;	//name = variable+index
 	int lower;				//lower bound (always 0)
 	int upper;				//upper bound (always 1)
	int value;				//optimal value (output only)
 }
 
 //Defines a column for an integer variable
 tuple IntegerColumn {
  	key string variable;	//name = variable+index
 	int lower;				//lower bound
 	int upper;				//upper bound
	int value;				//optimal value (output only)
 }
 
 //Defines a column for a continuous variable
 tuple FloatColumn {
  	key string variable;	//name = variable+index
 	float lower;			//lower bound
 	float upper;			//upper bound
	float value;			//optimal value (output only)
 }
 
 //Defines a row for a constraint or a decision expression
 tuple Row {
 	key string cnstraint;	//name = constraint+index  
 	string sense;			//GE(>=), EQ(==), LE(<=) or dexpr (must be dexpr for a decision expression)
 	float rhs;				//right-hand side term (must be zero for a decision expression)
 }
 
  
 //Defines a coefficient at a specific row and column
 tuple Entry {
 	key string cnstraint;	//name = constraint+index   (can also be used for a decision expression) 
  	key string variable;	//name = variable+index  
 	float coefficient;		//coefficent
 }
 
 //Defines a decision expression value
 tuple Objective {
    key string name;		//must correspond to the name of one of the decision expressions
    string sense;			//minimize or maximize
    float value;			//optimal decision expression value (output only)
 }
 
 //Read the input data
 {BooleanColumn} 	booleanColumns= ...;
 {IntegerColumn} 	integerColumns=  ...; 
 {FloatColumn} 		floatColumns=  ...; 
 {Row} 				rows= ...; 
 {Entry} 			entries= ...;  
 {Objective}		objectives= ...;	//a singleton tuple set designating the decison express to use as the objective function
 float				objectiveSense= (first(objectives).sense=="maximize" ? 1.0 : -1.0);
 {Row} 				rows_dexpr= {i| i in rows: i.sense=="dexpr"};

 //Optimization Model
 dvar boolean	x[booleanColumns];
 dvar int		y[j in integerColumns]	in j.lower..j.upper;
 dvar float		z[j in floatColumns]	in j.lower..j.upper;
 
 dexpr float v[i in rows_dexpr]= 			 	
 				  sum(j in booleanColumns,   t in entries: t.cnstraint==i.cnstraint && t.variable==j.variable) t.coefficient*x[j] 
 				+ sum(j in integerColumns,   t in entries: t.cnstraint==i.cnstraint && t.variable==j.variable) t.coefficient*y[j] 
			 	+ sum(j in floatColumns,     t in entries: t.cnstraint==i.cnstraint && t.variable==j.variable) t.coefficient*z[j];

 dexpr float obj= sum(i in rows_dexpr: i.cnstraint==first(objectives).name)v[i]; //selects the decision expression designated as the objective function

 constraint ct[rows];

 maximize obj*objectiveSense;
 subject to {

 forall(i in rows)
   ct[i]:	if(i.sense=="GE")   
	   			sum(j in booleanColumns,	t in entries: t.cnstraint==i.cnstraint && t.variable==j.variable)  t.coefficient*x[j] +
	   			sum(j in integerColumns,	t in entries: t.cnstraint==i.cnstraint && t.variable==j.variable)  t.coefficient*y[j] +
	   			sum(j in floatColumns,		t in entries: t.cnstraint==i.cnstraint && t.variable==j.variable)  t.coefficient*z[j]
	   			>= i.rhs;
   			else if(i.sense=="EQ")
	   			sum(j in booleanColumns,	t in entries: t.cnstraint==i.cnstraint && t.variable==j.variable)  t.coefficient*x[j] +
	   			sum(j in integerColumns,	t in entries: t.cnstraint==i.cnstraint && t.variable==j.variable)  t.coefficient*y[j] +
	   			sum(j in floatColumns, 	 	t in entries: t.cnstraint==i.cnstraint && t.variable==j.variable)  t.coefficient*z[j]
	   			== i.rhs;
   			else if(i.sense=="LE")
	   			sum(j in booleanColumns,	t in entries: t.cnstraint==i.cnstraint && t.variable==j.variable)  t.coefficient*x[j] +
	   			sum(j in integerColumns,	t in entries: t.cnstraint==i.cnstraint && t.variable==j.variable)  t.coefficient*y[j] +
	   			sum(j in floatColumns,		t in entries: t.cnstraint==i.cnstraint && t.variable==j.variable)  t.coefficient*z[j]
	   			<= i.rhs;
				
 }
 
 //Populate the output tables
 {BooleanColumn}	booleanDecisions=		{<j.variable, j.lower, j.upper, x[j]> | j in booleanColumns};
 {IntegerColumn}	integerDecisions=		{<j.variable, j.lower, j.upper, y[j]> | j in integerColumns};
 {FloatColumn}		floatDecisions=			{<j.variable, j.lower, j.upper, z[j]> | j in floatColumns};
 {Objective}		optimalObjectives=		{<i.cnstraint, "", v[i]> | i in rows_dexpr};
   