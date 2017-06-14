/**
 * 
 */
package com.ibm.optim.oaas.sample.examples;

import java.net.URL;
import java.util.Iterator;
import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import com.ibm.optim.oaas.sample.optimization.Optimizer;
import com.ibm.optim.sample.oplinterface.OPLCollector;
import com.ibm.optim.sample.oplinterface.OPLGlobal;
import com.ibm.optim.sample.oplinterface.OPLTuple;

/**
 * This class solves the Warehousing stochastic mathematical program.
 * 
 * @author bloomj
 *
 */
public class WarehousingStochastic {
	
	static Map<String, StructType> networkDataModel= (new OPLCollector.ADMBuilder())
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
	
	static URL networkDataSource= Optimizer.getFileLocation(
			Warehousing.class,
			"Warehousing-data.json");
	
	static Map<String, StructType> demandDataModel= (new OPLCollector.ADMBuilder())
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
	
	static Map<String, StructType> resultDataModel= (new OPLCollector.ADMBuilder())
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


	/**
	 * 
	 */
	public WarehousingStochastic() {
		super();
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		controller2Stage();
		
//		controllerPerfectInformation();
	}
	
	/**
	 * Runs the full 2-stage stochastic optimization
	 */
	public static void controller2Stage() {
		
		OPLGlobal.setDisplayTitle("Stochastic Warehousing");
		OPLGlobal.showDisplay();
				
		URL demandDataSource= Optimizer.getFileLocation(
				Warehousing.class, 
				"Warehousing-sales_data-52_scenarios.json");

		OPLCollector warehousingData= (new OPLCollector("warehousingData", networkDataModel)).setJsonSource(networkDataSource).fromJSON();
		warehousingData.addTables((new OPLCollector("demandData", demandDataModel)).setJsonSource(demandDataSource).fromJSON());
		
		Dataset<Row> demands= warehousingData.getTable("demands");
		Dataset<Row> scenarios= warehousingData.getTable("scenarios");
		Dataset<Row> demandStatistics= demands.join(scenarios.drop("periods"), demands.col("scenarioId").equalTo(scenarios.col("id"))).drop("id")
				.groupBy("store")
				.agg(
						functions.avg("amount"), 
						functions.stddev_pop("amount"), 
						functions.corr("amount", "totalDemand")
				);

		OPLGlobal.SPARK_SESSION.sqlContext().udf().register("positive",
				  (Double number) -> (number > 0.0 ? 1: 0), DataTypes.IntegerType);
		Dataset<Row> demandSummary= demandStatistics
				.agg(
					functions.sum("avg(amount)"), 
					functions.max("corr(amount, totalDemand)"), 
					functions.min("corr(amount, totalDemand)"),
					functions.sum(functions.callUDF("positive", demandStatistics.col("corr(amount, totalDemand)")))
						.as("corr(amount, totalDemand) > 0.0")
				);

		OPLCollector.show("demandSummary", demandSummary, OPLGlobal.out);
		OPLCollector.show("demandStatistics", demandStatistics, OPLGlobal.out);
		
		//The 2-stage Stochastic Program with Recourse Optimization Model 
		URL warehousingDotMod= Optimizer.getFileLocation(Warehousing.class,"opl/WarehousingStochastic.mod");

		Optimizer problem= new Optimizer("Warehousing") 
				.setOPLModel("Warehousing.mod", warehousingDotMod)
				.setResultDataModel(resultDataModel);
		
		OPLCollector warehousingResult= problem.solve(warehousingData.copy("warehousingDataNoCoord", "warehouses", "routes", "stores", "demands", "scenarios"));
//		Note: the mapCoordinates table is not used in the optimization and so is not sent to the optimizer

		warehousingResult.show("objectives", OPLGlobal.out);
		warehousingResult.show("openWarehouses", OPLGlobal.out);

		//Show routes that are used in fewer than all scenarios, i.e. a store shifts its source warehouse location among scenarios
		Dataset<Row> shipments= warehousingResult.getTable("shipments");
		Dataset<Row> routeUtilization= shipments.groupBy("location", "store").count().where("count < 52").sort("store");

		//Show scenarios in which a store has more than one source warehouse location
		Dataset<Row> multiSource= shipments.groupBy("store", "scenarioId").count().where("count > 1").sort("store");
		
		//Show the number of scenarios in which a store has more than one source warehouse location
		Dataset<Row> multiSource2= multiSource.groupBy("store").sum("count");
		
		// Determine how frequently each route is used 
		Dataset<Row> expectedShipments= shipments.join(scenarios, scenarios.col("id").equalTo(shipments.col("scenarioId"))).drop("id", "totalDemand")
		    .withColumn("expectedAmount", shipments.col("amount").multiply(scenarios.col("periods")))
		    .groupBy("location", "store")
		    .agg(functions.sum("periods").alias("probability"), functions.sum("expectedAmount").alias("x"));
		expectedShipments= expectedShipments.withColumn("expectedShipment", expectedShipments.col("x").divide(expectedShipments.col("probability")))
				.drop("x").sort("store");
		
		//Show warehouse utilization
		Dataset<Row> openWarehouses= warehousingResult.getTable("openWarehouses").select("*").where("open == 1").drop("iteration");
		Dataset<Row> warehouseShipments= shipments.drop("iteration").groupBy("location", "scenarioId").sum("amount")
					.join(scenarios, (shipments.col("scenarioId").equalTo(scenarios.col("id"))))
					.drop("id", "totalDemand");
		Dataset<Row> warehouseUtilization= warehouseShipments.join(openWarehouses, "location").drop("iteration", "open")
					.withColumn("utilization", warehouseShipments.col("sum(amount)").multiply(warehouseShipments.col("periods")).divide(openWarehouses.col("capacity")))
					.groupBy("location").sum("utilization");
		
		OPLCollector scenarioAnalytics= new OPLCollector("scenarioAnalytics");
		scenarioAnalytics.buildADM()
				.addSchema("routeUtilization", routeUtilization.schema())
				.addSchema("multiSource", multiSource.schema())
				.addSchema("multiSource2", multiSource2.schema())
				.addSchema("warehouseUtilization", warehouseUtilization.schema())
				.addSchema("expectedShipments", expectedShipments.schema())
				.build();
		scenarioAnalytics.buildData()
			.addTable("routeUtilization", routeUtilization)
			.addTable("multiSource", multiSource)
			.addTable("multiSource2", multiSource2)
			.addTable("warehouseUtilization", warehouseUtilization)
			.addTable("expectedShipments", expectedShipments)
			.build();
		scenarioAnalytics.show(OPLGlobal.out);	

		problem.shutdown();	
					
	}/*controller2Stage*/
	
	/**
	 * Runs the optimization as if each scenario has an optimal solution
	 */
	public static void controllerPerfectInformation() {
		
		OPLGlobal.setDisplayTitle("Perfect Information");
		OPLGlobal.showDisplay();
		
		URL demandDataSource= Optimizer.getFileLocation(
				Warehousing.class, 
				"Warehousing-sales_data-52_scenarios.json");

		OPLCollector warehousingData= (new OPLCollector("warehousingData", networkDataModel)).setJsonSource(networkDataSource).fromJSON();
		warehousingData.addTables((new OPLCollector("demandData", demandDataModel)).setJsonSource(demandDataSource).fromJSON());
		
//		Dataset<Row> scenarios= warehousingData.getTable("scenarios");
			
		//The Deterministic Optimization Model 
		URL warehousingDotMod= Optimizer.getFileLocation(Warehousing.class,"opl/Warehousing.mod");

		Optimizer problem= new Optimizer("Warehousing") 
				.setOPLModel("Warehousing.mod", warehousingDotMod)
				.setResultDataModel(resultDataModel);
		
		OPLCollector scenarioData;
		OPLCollector scenarioResult;
		
		//Accumulator for the solve results from the individual scenarios
		OPLCollector warehousingResult= new OPLCollector(problem.getName()+"Result");
		
		Iterator<Row> scenario= warehousingData.getTable("scenarios").toLocalIterator();
		int idFieldIndex= warehousingData.getTable("scenarios").schema().fieldIndex("id");
		String id;	
		while(scenario.hasNext()) {
			id= scenario.next().getString(idFieldIndex);
			
			scenarioData= warehousingData.copy(warehousingData.getName()+"Scenario"+id, "warehouses", "routes", "stores", "demands", "scenarios");
//			Note: the mapCoordinates table is not used in the optimization and so is not sent to the optimizer
			scenarioData.replaceTable("demands", warehousingData.getTable("demands").select("*").where("scenarioId="+id));
			scenarioData.replaceTable("scenarios", warehousingData.getTable("scenarios").select("*").where("id="+id));
			
			scenarioResult= problem.solve(scenarioData);
	
			if(warehousingResult.getData().isEmpty()) {	//first scenario
				warehousingResult.addTables(scenarioResult);
			}
			else {
				for(String tableName: scenarioResult.getTableNames())
					warehousingResult.addData(tableName, scenarioResult.getTable(tableName), scenarioResult.getSize(tableName));
			}			
			OPLCollector.show("Scenario "+id, scenarioResult.getTable("objectives"), OPLGlobal.out);

		}/*while*/

		Dataset<Row> objectives= warehousingResult.getTable("objectives");
		Dataset<Row> scenarios= warehousingData.getTable("scenarios");
		Dataset<Row> costSummary= objectives.drop("problem", "iteration")
			.join(scenarios, objectives.col("scenarioId").equalTo(scenarios.col("id")))
			.drop("id", "totalDemand")
			.withColumn("weightedCost", objectives.col("value").multiply(scenarios.col("periods")))
			.groupBy("dexpr")
			.sum("weightedCost");
		OPLCollector.show("costSummary", costSummary, OPLGlobal.out);
		
		Dataset<Row> openWarehouses= warehousingResult.getTable("openWarehouses");
		Dataset<Row> openSummary= openWarehouses.drop("open", "iteration")
			.groupBy("location")
			.agg(functions.count("scenarioId"), functions.avg("capacity"), functions.stddev_pop("capacity"));
		OPLCollector.show("openSummary", openSummary, OPLGlobal.out);
		

		problem.shutdown();	
					
	}/*controllerPerfectInformation*/
	
	/**
	 * Runs the optimization as if each scenario uses the open warehouses from the scenario with the highest demand
	 */
	public static void controllerWorstCase() {
		
		URL demandDataSource= Optimizer.getFileLocation(
				Warehousing.class, 
				"Warehousing-sales_data-52_scenarios.json");

		OPLCollector warehousingData= (new OPLCollector("warehousingData", networkDataModel)).setJsonSource(networkDataSource).fromJSON();
		warehousingData.addTables((new OPLCollector("demandData", demandDataModel)).setJsonSource(demandDataSource).fromJSON());
		
		Dataset<Row> scenarios= warehousingData.getTable("scenarios");
			
		//The Subproblem Optimization Model 
		URL warehousingDotMod= Optimizer.getFileLocation(Warehousing.class,"opl/Warehousing.mod");

		Optimizer problem= new Optimizer("Warehousing") 
				.setOPLModel("Warehousing.mod", warehousingDotMod)
				.setResultDataModel(resultDataModel);
		
		String highestDemandScenarioId= scenarios.sort(scenarios.col("totalDemand").desc()).first().getString(scenarios.schema().fieldIndex("id"));
		OPLCollector highestDemandScenarioData= warehousingData.copy(warehousingData.getName()+"Scenario"+highestDemandScenarioId, "warehouses", "routes", "stores", "demands", "scenarios");
//		Note: the mapCoordinates table is not used in the optimization and so is not sent to the optimizer
		highestDemandScenarioData.replaceTable("demands", warehousingData.getTable("demands").select("*").where("scenarioId="+highestDemandScenarioId));
		highestDemandScenarioData.replaceTable("scenarios", warehousingData.getTable("scenarios").select("*").where("id="+highestDemandScenarioId));
		
		OPLCollector highestDemandScenarioResult= new OPLCollector(problem.getName()+"Result");
		highestDemandScenarioResult= problem.solve(highestDemandScenarioData);

		 
			
	}/*controllerWorstCase*/
		
	/**
	 * Sorts a Spark dataset to produce a cumulative distribution
	 * 
	 * @param source data set 
	 * @param groupBy names of columns to group by
	 * @param dataField name of column containing the data to be sorted
	 * @param probabilityField name of column containing the probabilities to be accumulated
	 * @return
	 */
/*	public Dataset<Row> cumulativeDistribution(Dataset<Row> source, List<String> groupBy, String dataField, String probabilityField) {
		Dataset<Row> result= null;
		Window window= Window.partitionBy(groupBy);
		result;
		
		return result;
	}
*/
	
	
}/*class WarehousingStochastic*/
