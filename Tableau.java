/**
 * 
 */
package com.ibm.optim.oaas.sample.examples;

import java.net.URL;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import com.ibm.optim.oaas.sample.optimization.Optimizer;
import com.ibm.optim.sample.oplinterface.OPLCollector;
import com.ibm.optim.sample.oplinterface.OPLGlobal;
import com.ibm.optim.sample.oplinterface.OPLTuple;

/**
 * Demonstrates equivalence between OPL and SQL using Spark and the Warehousing example.
 * The context and theory of this class are presented in an IBM DSX sample notebook entitled
 * Optimization Modeling and Relational Data (where the code is written in Python). 
 * 
 * @author bloomj
 *
 */
public class Tableau {

	/**
	 * 
	 */
	public Tableau() {
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		controller();

	}
	
	public static void controller() {
		
		OPLGlobal.setDisplayTitle("Tableau");
		OPLGlobal.showDisplay();
		
		Double infinity= 1.0E20;
		
		// Create the Warehousing data model
		Map<String, StructType> networkDataModel= (new OPLCollector.ADMBuilder())
				.addSchema("warehouses", (new OPLTuple.SchemaBuilder())
						.addField("location",		DataTypes.StringType)
						.addField("fixedCost",		DataTypes.DoubleType)
						.addField("capacityCost",	DataTypes.DoubleType)
						.buildSchema())
				.addSchema("routes", (new OPLTuple.SchemaBuilder())
						.addField("location",		DataTypes.StringType)
						.addField("store",			DataTypes.StringType)
						.addField("shippingCost",	DataTypes.DoubleType)
						.buildSchema())
				.addSchema("stores", (new OPLTuple.SchemaBuilder())
						.addField("storeId",		DataTypes.StringType)
						.buildSchema())
	            .addSchema("mapCoordinates",(new OPLTuple.SchemaBuilder())
	            		.addField("location",		DataTypes.StringType)	//Note: "location" can be either a warehouse location or a store location.
	            		.addField("lon",			DataTypes.DoubleType)
	            		.addField("lat",			DataTypes.DoubleType)					
	            		.buildSchema())
				.build();
		
		Map<String, StructType> demandDataModel= (new OPLCollector.ADMBuilder())
				.addSchema("demands", (new OPLTuple.SchemaBuilder())
						.addField("store",			DataTypes.StringType)
						.addField("scenarioId",		DataTypes.StringType)
						.addField("amount",			DataTypes.DoubleType)
						.buildSchema())
				.addSchema("scenarios", (new OPLTuple.SchemaBuilder())
						.addField("id",				DataTypes.StringType)
						.addField("totalDemand",	DataTypes.DoubleType)
						.addField("periods",		DataTypes.DoubleType)
						.buildSchema())
				.build();
		
		URL networkDataSource= Optimizer.getFileLocation(
				Tableau.class,
				"Warehousing-data.json");
		
		URL demandDataSource= Optimizer.getFileLocation(
				Tableau.class, 
				"Warehousing-sales_data-nominal_scenario.json");

		OPLCollector warehousingData= (new OPLCollector("warehousingData", networkDataModel)).setJsonSource(networkDataSource).fromJSON();
		warehousingData.addTables((new OPLCollector("demandData", demandDataModel)).setJsonSource(demandDataSource).fromJSON());

		warehousingData.show(OPLGlobal.out);

		Map<String, StructType> warehousingResultDataModel= (new OPLCollector.ADMBuilder())
				.addSchema("objectives", (new OPLTuple.SchemaBuilder())
						.addField("problem",		DataTypes.StringType)
						.addField("dExpr",			DataTypes.StringType)
					 	.addField("scenarioId",		DataTypes.StringType)
						.addField("iteration",		DataTypes.IntegerType)
						.addField("value",			DataTypes.DoubleType)
						.buildSchema())
				.addSchema("openWarehouses", (new OPLTuple.SchemaBuilder())
						.addField("location",		DataTypes.StringType)
					 	.addField("scenarioId",		DataTypes.StringType)
						.addField("iteration",		DataTypes.IntegerType)
						.addField("open",			DataTypes.IntegerType)
						.addField("capacity",		DataTypes.DoubleType)
						.buildSchema())
				.addSchema("shipments", (new OPLTuple.SchemaBuilder())
						.addField("location",		DataTypes.StringType)
						.addField("store",			DataTypes.StringType)
					 	.addField("scenarioId",		DataTypes.StringType)
						.addField("iteration",		DataTypes.IntegerType)
						.addField("amount",			DataTypes.DoubleType)
						.buildSchema())
				.build();		
//		Note: the "scenarioId" and "iteration" fields are not used in this example but are included for use in other contexts.
		
		// Create the tableau data model
		OPLCollector tableauData= new OPLCollector("tableauData");
		OPLCollector.ADMBuilder tableauADMBuilder= tableauData.buildADM();
		tableauADMBuilder.addSchema("integerColumns", (new OPLTuple.SchemaBuilder())
			    		.addField("variable", DataTypes.StringType)
			    		.addField("lower", DataTypes.IntegerType)
			    		.addField("upper", DataTypes.IntegerType)
			    		.addField("value", DataTypes.IntegerType)
						.buildSchema())
			    .addSchema("booleanColumns", (new OPLTuple.SchemaBuilder())
			    		.copyFields(tableauADMBuilder.referenceSchema("integerColumns"))
						.buildSchema())
			    .addSchema("floatColumns", (new OPLTuple.SchemaBuilder())
			    		.addField("variable", DataTypes.StringType)
			    		.addField("lower", DataTypes.DoubleType)
			    		.addField("upper", DataTypes.DoubleType)
			    		.addField("value", DataTypes.DoubleType)
						.buildSchema())
			    .addSchema("rows", (new OPLTuple.SchemaBuilder())
			    		.addField("cnstraint", DataTypes.StringType)  
			    		.addField("sense", DataTypes.StringType)  
			    		.addField("rhs", DataTypes.DoubleType)
						.buildSchema())
			    .addSchema("entries", (new OPLTuple.SchemaBuilder())
			    		.addField("cnstraint", DataTypes.StringType)  
			    		.addField("variable", DataTypes.StringType)  
			    		.addField("coefficient", DataTypes.DoubleType)
						.buildSchema())
				.addSchema("objectives", (new OPLTuple.SchemaBuilder())
			    		.addField("name", DataTypes.StringType)
			    		.addField("sense", DataTypes.StringType)
			    		.addField("value", DataTypes.DoubleType)
						.buildSchema())
			    .build();
		
		// Create the data model to transform the warehousing data into the tableau
		OPLCollector tableauTransformations= new OPLCollector("tableauTransformations");
		tableauTransformations.buildADM()
				.addSchema("columns_open", (new OPLTuple.SchemaBuilder())
			    		.copyFields(tableauData.getADM().get("booleanColumns"))
			    		.addField("location", DataTypes.StringType)
						.buildSchema())
			    .addSchema("columns_capacity", (new OPLTuple.SchemaBuilder())
			    		.copyFields(tableauData.getADM().get("floatColumns"))
			    		.addField("location", DataTypes.StringType)
						.buildSchema())
			    .addSchema("columns_ship", (new OPLTuple.SchemaBuilder())
			    		.copyFields(tableauData.getADM().get("floatColumns"))
			    		.addField("location", DataTypes.StringType)
			    		.addField("store", DataTypes.StringType)
						.buildSchema())
			    .addSchema("rows_ctCapacity", (new OPLTuple.SchemaBuilder())
			    		.copyFields(tableauData.getADM().get("rows"))
						.buildSchema())
			    .addSchema("rows_ctDemand", (new OPLTuple.SchemaBuilder())
			    		.copyFields(tableauData.getADM().get("rows"))
						.buildSchema())
			    .addSchema("rows_ctSupply", (new OPLTuple.SchemaBuilder())
			    		.copyFields(tableauData.getADM().get("rows"))
						.buildSchema())
			    .addSchema("rows_dexpr", (new OPLTuple.SchemaBuilder())
			    		.copyFields(tableauData.getADM().get("rows"))
						.buildSchema())
			    .addSchema("entries_ctCapacity_capacity", (new OPLTuple.SchemaBuilder())
			    		.copyFields(tableauData.getADM().get("entries"))
						.buildSchema())
			    .addSchema("entries_ctCapacity_ship", (new OPLTuple.SchemaBuilder())
			    		.copyFields(tableauData.getADM().get("entries"))
						.buildSchema())
				.addSchema("entries_ctDemand_ship", (new OPLTuple.SchemaBuilder())
			    		.copyFields(tableauData.getADM().get("entries"))
						.buildSchema())
				.addSchema("entries_ctSupply_open", (new OPLTuple.SchemaBuilder())
			    		.copyFields(tableauData.getADM().get("entries"))
						.buildSchema())
				.addSchema("entries_ctSupply_ship", (new OPLTuple.SchemaBuilder())
			    		.copyFields(tableauData.getADM().get("entries"))
						.buildSchema())
				.addSchema("entries_dexpr_open", (new OPLTuple.SchemaBuilder())
			    		.copyFields(tableauData.getADM().get("entries"))
						.buildSchema())
				.addSchema("entries_dexpr_capacity", (new OPLTuple.SchemaBuilder())
			    		.copyFields(tableauData.getADM().get("entries"))
						.buildSchema())
				.addSchema("entries_dexpr_ship", (new OPLTuple.SchemaBuilder())
			    		.copyFields(tableauData.getADM().get("entries"))
						.buildSchema())
			    .build();
		
		OPLCollector.DataBuilder tableauTransformer= tableauTransformations.buildData();
		
		//Create input datasets
		Dataset<Row> warehouses = warehousingData.getTable("warehouses");
		Dataset<Row> stores =     warehousingData.getTable("stores");
		Dataset<Row> routes =     warehousingData.getTable("routes");
		Dataset<Row> demands =    warehousingData.getTable("demands");
		Dataset<Row> scenarios =  warehousingData.getTable("scenarios");
		String scenarioId= scenarios.first().getString(scenarios.first().fieldIndex("id"));
		
		//Encode the columns
		tableauTransformer.addTable("columns_open", 
			warehouses.select("location")
		    	.withColumn("variable", functions.concat(functions.lit("open_"), warehouses.col("location")))
		    	.withColumn("upper", functions.lit(1))
		    	.withColumn("lower", functions.lit(0))
		    	.withColumn("value", functions.lit(0)));
		tableauTransformer.addTable("columns_capacity",
			warehouses.select("location")
			    .withColumn("variable", functions.concat(functions.lit("capacity_"), warehouses.col("location")))
			    .withColumn("upper", functions.lit(infinity))
			    .withColumn("lower", functions.lit(0.0))
			    .withColumn("value", functions.lit(0.0))); 
		tableauTransformer.addTable("columns_ship",
            routes.select("location", "store")
			    .withColumn("variable", functions.concat(functions.lit("ship_"), routes.col("location"), functions.lit("_"), routes.col("store")))
			    .withColumn("upper", functions.lit(1.0))
			    .withColumn("lower", functions.lit(0.0))
			    .withColumn("value", functions.lit(0.0)));			

		//Encode the Constraints
		tableauTransformer.addTable("rows_ctCapacity", 
				warehouses.select("location")
				    .withColumn("cnstraint", functions.concat(functions.lit("ctCapacity_"), warehouses.col("location")))
				    .withColumn("sense", functions.lit("GE"))
				    .withColumn("rhs", functions.lit(0.0)));
		tableauTransformer.addTable("rows_ctDemand", 
				stores.select("storeId")
				    .withColumn("cnstraint", functions.concat(functions.lit("ctDemand_"), stores.col("storeId")))
				    .withColumn("sense", functions.lit("GE"))
				    .withColumn("rhs", functions.lit(1.0))
				    .withColumnRenamed("storeId", "store"));
		tableauTransformer.addTable("rows_ctSupply", 
				routes.select("location", "store")
				    .withColumn("cnstraint", functions.concat(functions.lit("ctSupply_"), routes.col("location"), functions.lit("_"), routes.col("store")))
				    .withColumn("sense", functions.lit("GE"))
				    .withColumn("rhs", functions.lit(0.0)));
		tableauTransformer.addTable("rows_dexpr", 
				Arrays.asList(
					RowFactory.create("capitalCost", "dexpr", 0.0), 
					RowFactory.create("operatingCost", "dexpr", 0.0), 
					RowFactory.create("totalCost", "dexpr", 0.0)
				));
		
		//Reshape the Coefficient Data into the Tableau
		
		scala.collection.Seq<String> routesKeyColumns= scala.collection.JavaConversions.asScalaBuffer(Arrays.asList("location", "store"));
		
		tableauTransformer.addTable( 
				"entries_ctCapacity_capacity",
				tableauTransformer.referenceTable("rows_ctCapacity")
				.join(tableauTransformer.referenceTable("columns_capacity"), "location")
			    .select("cnstraint", "variable")
			    .withColumn("coefficient", functions.lit(1.0)));
		//demand at the store at the end of each route
		Dataset<Row> demandOnRoute= routes.join(
				demands.where(demands.col("scenarioId").equalTo(functions.lit(scenarioId))), "store")
				.select("location", "store", "amount").withColumnRenamed("amount", "demand");
		tableauTransformer.addTable( 
			"entries_ctCapacity_ship",
			tableauTransformer.referenceTable("rows_ctCapacity")
				.join(tableauTransformer.referenceTable("columns_ship"), "location")
			    .join(demandOnRoute, routesKeyColumns)
			    .withColumn("coefficient", functions.negate(demandOnRoute.col("demand")))
			    .select("cnstraint", "variable", "coefficient"));
		tableauTransformer.addTable( 
			"entries_ctDemand_ship",
			tableauTransformer.referenceTable("rows_ctDemand")
				.join(tableauTransformer.referenceTable("columns_ship"), "store")
			    .select("cnstraint", "variable")
			    .withColumn("coefficient", functions.lit(1.0)));
		tableauTransformer.addTable( 
			"entries_ctSupply_open",
			tableauTransformer.referenceTable("rows_ctSupply")
				.join(tableauTransformer.referenceTable("columns_open"), "location")
			    .select("cnstraint", "variable")
			    .withColumn("coefficient", functions.lit(1.0)));
		tableauTransformer.addTable( 
			"entries_ctSupply_ship",
			tableauTransformer.referenceTable("rows_ctSupply")
				.join(tableauTransformer.referenceTable("columns_ship"), routesKeyColumns)
			    .select("cnstraint", "variable")
			    .withColumn("coefficient", functions.lit(-1.0)));	
		Dataset<Row> rows_dexpr= tableauTransformer.referenceTable("rows_dexpr");
		tableauTransformer.addTable( 
			"entries_dexpr_open",
			(rows_dexpr.where(rows_dexpr.col("cnstraint").equalTo(functions.lit("capitalCost"))
							 .or(rows_dexpr.col("cnstraint").equalTo(functions.lit("totalCost")))))
			    .crossJoin(tableauTransformer.referenceTable("columns_open")
			    		.join(warehouses, "location"))
			    .select("cnstraint", "variable", "fixedCost")
			    .withColumnRenamed("fixedCost", "coefficient"));
		tableauTransformer.addTable( 
			"entries_dexpr_capacity",
			(rows_dexpr.where(rows_dexpr.col("cnstraint").equalTo(functions.lit("capitalCost"))
					 		 .or(rows_dexpr.col("cnstraint").equalTo(functions.lit("totalCost")))))
			    .crossJoin(tableauTransformer.referenceTable("columns_capacity")
			    		.join(warehouses, "location"))
			    .select("cnstraint", "variable", "capacityCost")
			    .withColumnRenamed("capacityCost", "coefficient"));
		tableauTransformer.addTable( 
			"entries_dexpr_ship",
			(rows_dexpr.where(rows_dexpr.col("cnstraint").equalTo(functions.lit("operatingCost"))
							 .or(rows_dexpr.col("cnstraint").equalTo(functions.lit("totalCost")))))
			    .crossJoin(
			    		tableauTransformer.referenceTable("columns_ship")
			    		.join(routes.join(demandOnRoute, routesKeyColumns)
		    				.withColumn("coefficient", demandOnRoute.col("demand").multiply(routes.col("shippingCost"))), 
		    				routesKeyColumns))
			    .select("cnstraint", "variable", "coefficient"));
		
		tableauTransformer.build(); 
		
		//Build the input data for the tableau optimization problem
		//Drop the instance-specific keys (location and store), which are not supported in the tableau model
		tableauData.buildData()
			.addTable(
				"booleanColumns", 
						tableauTransformations.getTable("columns_open").drop("location"))
			.addTable(
				 "floatColumns",
				 		tableauTransformations.getTable("columns_capacity").drop("location")
				.union(	tableauTransformations.getTable("columns_ship").drop("location").drop("store")))
			.addEmptyTable(
				"integerColumns")
			.addTable(
				"rows", 
						tableauTransformations.getTable("rows_ctCapacity").drop("location")
			    .union(	tableauTransformations.getTable("rows_ctDemand").drop("store"))
			    .union(	tableauTransformations.getTable("rows_ctSupply").drop("location").drop("store"))
			    .union(	tableauTransformations.getTable("rows_dexpr")))
			.addTable(
				"entries", 
						tableauTransformations.getTable("entries_ctSupply_open")
				.union(	tableauTransformations.getTable("entries_ctSupply_ship"))
				.union(	tableauTransformations.getTable("entries_ctCapacity_capacity"))
				.union(	tableauTransformations.getTable("entries_ctCapacity_ship"))
				.union(	tableauTransformations.getTable("entries_ctDemand_ship"))
				.union(	tableauTransformations.getTable("entries_dexpr_open"))
				.union(	tableauTransformations.getTable("entries_dexpr_capacity"))
				.union(	tableauTransformations.getTable("entries_dexpr_ship")))
			.addTable(
				"objectives", Arrays.asList(
						RowFactory.create("totalCost", "minimize", 0.0)
					))
			.build();
		
/*		writeProblem(tableauData);
		if(true)
			return;
*/
		URL TableauDotMod= Optimizer.getFileLocation(Tableau.class,"opl/TableauRevised.mod");
		
		Optimizer problem= new Optimizer("WarehousingTableau") 
				.setOPLModel("Tableau.mod", TableauDotMod)
				.setResultDataModel(new OPLCollector.ADMBuilder()
						.addSchema("booleanDecisions",	tableauData.getSchema("booleanColumns"))
						.addSchema("integerDecisions",	tableauData.getSchema("integerColumns"))
						.addSchema("floatDecisions",	tableauData.getSchema("floatColumns"))
						.addSchema("optimalObjectives",	tableauData.getSchema("objectives"))
						.build());
		
		OPLCollector tableauResult= problem.solve(tableauData);
			
		//Recover the solution
		OPLCollector warehousingResult= new OPLCollector("warehousingResult", warehousingResultDataModel);
		OPLCollector.DataBuilder resultsBuilder= warehousingResult.buildData();
		resultsBuilder.addTable("objectives", 
				tableauResult.getTable("optimalObjectives").select("name", "value")
					.withColumnRenamed("name", "dExpr")
					.withColumn("problem",		functions.lit("warehousing"))
				 	.withColumn("scenarioId",	functions.lit(scenarioId))
					.withColumn("iteration",	functions.lit(0)));
		resultsBuilder.addTable("openWarehouses", 
						(tableauResult.getTable("booleanDecisions").select("variable", "value").withColumnRenamed("value", "open")
						.join(tableauTransformations.getTable("columns_open"), "variable")).drop("variable")
					.join(
						tableauResult.getTable("floatDecisions").select("variable", "value").withColumnRenamed("value", "capacity")
						.join(tableauTransformations.getTable("columns_capacity"), "variable").drop("variable")
					, "location")
					.select("location",	"open",	"capacity")
				 	.withColumn("scenarioId",	functions.lit(scenarioId))
					.withColumn("iteration",	functions.lit(0)));
		Dataset<Row> floatDecisions= tableauResult.getTable("floatDecisions").select("variable", "value");
		resultsBuilder.addTable("shipments", 
				floatDecisions
					.join(tableauTransformations.getTable("columns_ship"), "variable").drop("variable")
					.join(demandOnRoute, routesKeyColumns)
    				.withColumn("amount", demandOnRoute.col("demand").multiply(floatDecisions.col("value")))
					.select("location",	"store", "amount")
				 	.withColumn("scenarioId",	functions.lit(scenarioId))
					.withColumn("iteration",	functions.lit(0)));
		resultsBuilder.build();
		
		warehousingResult.show(OPLGlobal.out);

		problem.shutdown();	
				
	}/*controller*/
	
	public static void writeProblem(OPLCollector tableauData) {
		
		Dataset<Row> booleanColumns= tableauData.getTable("booleanColumns");
		Dataset<Row> integerColumns= tableauData.getTable("integerColumns");
		Dataset<Row> floatColumns= tableauData.getTable("floatColumns");
		Dataset<Row> rows= tableauData.getTable("rows");
		Dataset<Row> entries= tableauData.getTable("entries");
		Dataset<Row> objectives= tableauData.getTable("objectives");
		
/*		Dataset<Row> constraints= entries.groupBy("cnstraint").pivot("variable").sum("coefficient").join(rows, "cnstraint");
		StructType constraintSchema= constraints.schema();
		
		Dataset<Row> ctCapacity= constraints.where(functions.substring_index(constraints.col("cnstraint"), "_", 1).equalTo(functions.lit("ctCapacity")));
		Dataset<Row> ctDemand= constraints.where(functions.substring_index(constraints.col("cnstraint"), "_", 1).equalTo(functions.lit("ctDemand")));
		Dataset<Row> ctSupply= constraints.where(functions.substring_index(constraints.col("cnstraint"), "_", 1).equalTo(functions.lit("ctSupply")));
		Dataset<Row> dexpr= constraints.where(functions.substring_index(constraints.col("cnstraint"), "_", 1).equalTo(functions.lit("dexpr")));
		
		OPLGlobal.out.println((new OPLTuple("", constraintSchema, ctSupply.first())).toString());
*/
/*		Dataset<Row> constraints= entries.groupBy("cnstraint").pivot("variable").sum("coefficient").join(rows, "cnstraint");
		OPLCollector.show("constraints", constraints, OPLGlobal.out);
*/		
		Dataset<Row> constraints= 
						   rows
							.join(entries, "cnstraint")
							.join(booleanColumns, "variable")
					.union(rows
							.join(entries, "cnstraint")
							.join(integerColumns, "variable"))
					.union(rows
							.join(entries, "cnstraint")
							.join(floatColumns, "variable"))
					.orderBy("cnstraint", "variable")
					.drop("upper").drop("lower").drop("value");
//		OPLCollector.show("constraints", constraints, OPLGlobal.out);	
		
/*		if(true)
			return;
		
*/		ForEachConstraint contraintToString= new ForEachConstraint();

		Iterator<Row> constraint= constraints.toLocalIterator();
		OPLGlobal.out.println(constraints.schema().simpleString());
		while(constraint.hasNext()) {
			OPLGlobal.out.println(constraint.next().mkString(", "));
		}
		OPLGlobal.out.println();
		
		
		constraint= constraints.toLocalIterator();
		while(constraint.hasNext())
			try {
				contraintToString.call(constraint.next());
			} catch (Exception e1) {
				e1.printStackTrace();
			}
		Map<String, String> constraintList= contraintToString.getConstraints();
		
/*		OPLGlobal.out.println(constraintList.size());
		for (String key: constraintList.keySet())
			OPLGlobal.out.println(key);
*/
		for (String e: constraintList.keySet()) {
				OPLGlobal.out.println(e + ": " + constraintList.get(e));
			}				
		
	}/*writeProblem*/
	
	public static class ForEachConstraint implements ForeachFunction<Row> {
		
		private static final long serialVersionUID = 8941973187218023710L;
		
		StringBuilder result;
		Map<String, String> constraintList= new LinkedHashMap <String, String>();
		String currentConstraint;
		String sense;
		String rhs;
		
		public ForEachConstraint() {
			
			result= new StringBuilder();
			constraintList= new LinkedHashMap <String, String>();
			currentConstraint= null;
			sense= null;
			rhs= null;
			
		}

		@Override
		public void call(Row row) throws Exception {
			String cnstraint= row.getString(row.fieldIndex("cnstraint"));
			String variable= row.getString(row.fieldIndex("variable"));
			String coefficient= Double.toString(row.getDouble(row.fieldIndex("coefficient")));
			
			if(this.currentConstraint== null || !this.currentConstraint.equals(cnstraint)) /*Begin building new constraint*/ {
				
				if(sense != null & rhs != null) { /*Complete previous constraint*/
					if (sense.equals("GE")) {
						result.append(">= " + rhs);
//						OPLGlobal.out.println(currentConstraint + ": " + result.toString());
					}
					else if (sense.equals("LE")) {
						result.append("<= " + rhs);
//						OPLGlobal.out.println(currentConstraint + ": " + result.toString());
					}
					else if(sense.equals("EQ")) {
						result.append("== " + rhs);
//						OPLGlobal.out.println(currentConstraint + ": " + result.toString());
					}
					constraintList.put(currentConstraint, result.toString());
//					OPLGlobal.out.println();					
					result.delete(0, result.length());
				}
				
				//Begin new constraint
				currentConstraint= cnstraint;				
				sense= row.getString(row.fieldIndex("sense"));
				rhs= Double.toString(row.getDouble(row.fieldIndex("rhs")));
			}
			
			if(result.length()>0 && coefficient.charAt(0)!='-') { /*not beginning of string and positive coefficient*/
				result.append("+");
//				OPLGlobal.out.println(currentConstraint + ": " + result.toString());
			}
			result.append(coefficient + "*" + variable + " ");
//			OPLGlobal.out.println(currentConstraint + ": " + result.toString());
			
		}/*call*/
		
		public Map<String, String> getConstraints() {
			return constraintList;
		}
		
	}/*class ForEachConstraint*/
	
	
}/*class Tableau*/
