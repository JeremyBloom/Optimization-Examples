/**
 * 
 */
package com.ibm.optim.oaas.sample.examples;

import java.net.URL;
import java.util.Map;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import com.ibm.optim.oaas.sample.optimization.Optimizer;
import com.ibm.optim.sample.oplinterface.OPLCollector;
import com.ibm.optim.sample.oplinterface.OPLGlobal;
import com.ibm.optim.sample.oplinterface.OPLTuple;

/**
 * This class solves the Warehousing mathematical program.
 * 
 * @author bloomj
 *
 */
public class Warehousing {

	/**
	 * 
	 */
	public Warehousing() {
		super();
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		controller1();
	}
	
	public static void controller1() {
		
		OPLGlobal.setDisplayTitle("Warehousing");
		OPLGlobal.showDisplay();
			
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
				Warehousing.class,
				"Warehousing-data.json");
		
		URL demandDataSource= Optimizer.getFileLocation(
				Warehousing.class, 
				"Warehousing-sales_data-nominal_scenario.json");

		OPLCollector warehousingData= (new OPLCollector("warehousingData", networkDataModel)).setJsonSource(networkDataSource).fromJSON();
		warehousingData.addTables((new OPLCollector("demandData", demandDataModel)).setJsonSource(demandDataSource).fromJSON());

		warehousingData.show(OPLGlobal.out);

		Map<String, StructType> resultDataModel= (new OPLCollector.ADMBuilder())
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
		
		URL warehousingDotMod= Optimizer.getFileLocation(Warehousing.class,"opl/Warehousing.mod");

		Optimizer problem= new Optimizer("Warehousing") 
				.setOPLModel("Warehousing.mod", warehousingDotMod)
				.setResultDataModel(resultDataModel);
		
		OPLCollector warehousingResult= problem.solve(warehousingData.copy("warehousingDataNoCoord", "warehouses", "routes", "stores", "demands", "scenarios"));
//		Note: the mapCoordinates table is not used in the optimization and so is not sent to the optimizer

		warehousingResult.show(OPLGlobal.out);

		problem.shutdown();	
			
	}/*controller1*/
	

}/*class Warehousing*/
