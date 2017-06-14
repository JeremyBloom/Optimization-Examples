/**
 * 
 */
package com.ibm.optim.oaas.sample.examples;

import java.net.URL;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import com.ibm.optim.oaas.sample.optimization.ColumnGeneration;
import com.ibm.optim.oaas.sample.optimization.Optimizer;
import com.ibm.optim.sample.oplinterface.OPLCollector;
import com.ibm.optim.sample.oplinterface.OPLGlobal;
import com.ibm.optim.sample.oplinterface.OPLTuple;

/**
 * This class solves the Cutting Stock mathematical program using the column generation algorithm.
 * 
 * @author bloomj
 *
 */
public class CuttingStock {
	
	private static final int ROLL_WIDTH= 110;

	/**
	 * 
	 */
	public CuttingStock() {
		super();
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		controller();

	}
	
	public static void controller() {
		
		OPLGlobal.setDisplayTitle("Cutting Stock");
		OPLGlobal.showDisplay();
			
		Map<String, StructType> masterDataModel= (new OPLCollector.ADMBuilder())
				.addSchema("parameters", (new OPLTuple.SchemaBuilder())
					.addField("rollWidth",			DataTypes.IntegerType)
					.addField("initialPatterns",	DataTypes.IntegerType)
					.addField("iteration",			DataTypes.IntegerType)
					.buildSchema())
				.addSchema("orders", (new OPLTuple.SchemaBuilder())
					.addField("item",		DataTypes.StringType)
					.addField("width",	DataTypes.IntegerType)
					.addField("amount",	DataTypes.IntegerType)
					.buildSchema())
				.addSchema("patterns", (new OPLTuple.SchemaBuilder())
					.addField("id",		DataTypes.IntegerType)
					.addField("cost",	DataTypes.IntegerType)
					.buildSchema())
				.addSchema("slices", (new OPLTuple.SchemaBuilder())
					.addField("item",	DataTypes.StringType)
					.addField("pattern",DataTypes.IntegerType)
					.addField("number",	DataTypes.IntegerType)
					.buildSchema())
				.build();
						
		OPLCollector masterData= createSampleMasterData(masterDataModel);
		
		Map<String, StructType> subproblemDataModel= (new OPLCollector.ADMBuilder())
				.addSchema("parameters", (new OPLTuple.SchemaBuilder())
					.addField("rollWidth",			DataTypes.IntegerType)
					.addField("initialPatterns",	DataTypes.IntegerType)
					.addField("iteration",			DataTypes.IntegerType)
					.buildSchema())
				.addSchema("orders", (new OPLTuple.SchemaBuilder())
					.addField("item",			DataTypes.StringType)
					.addField("width",		DataTypes.IntegerType)
					.addField("amount",		DataTypes.IntegerType)
					.buildSchema())
				.addSchema("duals", (new OPLTuple.SchemaBuilder())
					.addField("item",		DataTypes.StringType)
					.addField("iteration", DataTypes.IntegerType)
					.addField("price",		DataTypes.DoubleType)
					.buildSchema())
				.build();
		
		OPLCollector subproblemData= createSampleSubproblemData(subproblemDataModel, masterData);
		
		Map<String, StructType> masterResultModel= (new OPLCollector.ADMBuilder())
				.addSchema("objectives", (new OPLTuple.SchemaBuilder())
					.addField("value",		DataTypes.DoubleType)
					.buildSchema())
				.addSchema("usages", (new OPLTuple.SchemaBuilder())
					.addField("pattern",	DataTypes.IntegerType)
					.addField("rolls",		DataTypes.DoubleType)
					.buildSchema())
				.addSchema("duals", (new OPLTuple.SchemaBuilder())
					.addField("item",		DataTypes.StringType)
					.addField("iteration", DataTypes.IntegerType)
					.addField("price",		DataTypes.DoubleType)
					.buildSchema())
				.build();
		
		Map<String, StructType> subproblemResultModel= (new OPLCollector.ADMBuilder())
				.addSchema("objectives", (new OPLTuple.SchemaBuilder())
					.addField("value",		DataTypes.DoubleType)
					.buildSchema())
				.addSchema("patterns", (new OPLTuple.SchemaBuilder())
					.addField("id",		DataTypes.IntegerType)
					.addField("cost",	DataTypes.IntegerType)
					.buildSchema())
				.addSchema("slices", (new OPLTuple.SchemaBuilder())
					.addField("item",	DataTypes.StringType)
					.addField("pattern",DataTypes.IntegerType)
					.addField("number",	DataTypes.IntegerType)
					.buildSchema())
				.build();
		
		
		URL masterDotMod= Optimizer.getFileLocation(CuttingStock.class, "opl/cuttingStock.mod");
		Optimizer master= new Optimizer("master") 
								.setResultDataModel(masterResultModel)
								.setOPLModel("cuttingStock.mod", masterDotMod);
						
		URL subproblemDotMod= Optimizer.getFileLocation(CuttingStock.class, "opl/cuttingStock-sub.mod");
		Optimizer subproblem= new Optimizer("subproblem") 
								.setResultDataModel(subproblemResultModel)
								.setOPLModel("cuttingStock-sub.mod", subproblemDotMod);
		
		ColumnGeneration.Linkage fromSubproblem= (new ColumnGeneration.Linkage())
														.addDataTo("patterns", "slices")
														.specifyIterationCounter("parameters", "iteration")
														.specifyBound("subproblem", "objectives", "value");
		ColumnGeneration.Linkage fromMaster=     (new ColumnGeneration.Linkage())
														.replaceDataIn("duals")
														.specifyIterationCounter("parameters", "iteration")
														.specifyBound("master", "constant", "initialMasterBound");

		ColumnGeneration.ConvergenceTest convergenceTest= (new ColumnGeneration.ConvergenceTest())
			.setTolerance(1.0e-6)
			.setInitialMasterBound(0.0)
			.setInitialSubproblemBound(Double.NEGATIVE_INFINITY);
		
		ColumnGeneration cuttingStock= (new ColumnGeneration("cuttingStock")) 
			.setConvergenceTest(convergenceTest)
			.setMaster(master, fromMaster)
			.setSubproblem(subproblem, fromSubproblem);
		
		int maxIterations= 10;
		cuttingStock.optimize(masterData, subproblemData, maxIterations);
		
		OPLGlobal.out.println("In CuttingStock.controller: Solution");
		cuttingStock.getMasterResult().show(OPLGlobal.out);
		
		//Post-processing the Solution
		Dataset<Row> slices= masterData.getTable("slices");
		Dataset<Row> orders= masterData.getTable("orders");
		
		List<String> items= new ArrayList<String>();
		Iterator<Row> i= orders.toLocalIterator();
		while(i.hasNext()) {
			items.add(i.next().getAs("item"));
		}
		
		Dataset<Row> patternWidths= slices
				.join(orders, "item").drop("amount")
				.groupBy("pattern").agg(functions.sum(orders.col("width").multiply(slices.col("number"))).alias("width"));
		patternWidths= patternWidths.withColumn("scrap", functions.lit(CuttingStock.ROLL_WIDTH).minus(patternWidths.col("width")));
		OPLCollector.show("patternWidths", patternWidths, OPLGlobal.out);
		
		Dataset<Row> usages= cuttingStock.getMasterResult().getTable("usages");
		Dataset<Row> production= usages.join(patternWidths, "pattern").filter("rolls > 0")
				.join(slices.groupBy("pattern").pivot("item").sum("number").na().fill(0), "pattern");
		OPLCollector.show("Production", production, OPLGlobal.out);
		
		//Accumulate production statistics
/*		Double rolls;
		Double scrap= 0.0;
		Row row;
		Map<String, Double> produced= new LinkedHashMap<String, Double>();
		for (String item: items)
			produced.put(item, 0.0);
		i= production.toLocalIterator();
		while(i.hasNext()) {
			row= i.next();
			rolls= (Double)row.getAs("rolls");
			scrap += (Long)row.getAs("scrap")*rolls;
			for (String item: items)
				produced.put(item, produced.get(item)+(Long)row.getAs(item)*rolls);
		}
		List<Row> productionStats= new ArrayList<Row>();
		productionStats.add(RowFactory.create(null, null, null, scrap, produced.values().toArray(new Double[]{})));
		Dataset<Row> summary= OPLGlobal.SPARK_SESSION.createDataFrame(productionStats, production.schema());
*/
				
	}/*controller*/
	
	public static OPLCollector createSampleMasterData(Map<String, StructType> dataModel) {
		OPLCollector masterData= new OPLCollector("masterData").setADM(dataModel);
		OPLCollector.DataBuilder builder= masterData.buildData();
		Map<String, List<Row>> data= new LinkedHashMap<String, List<Row>>();

		builder.addTable("parameters", (new OPLTuple("parameters", dataModel.get("parameters"), ROLL_WIDTH, 5, 0)).asSingleton(), 1L);

		data.put("patterns", new LinkedList<Row>());
		data.get("patterns").add(RowFactory.create(1, 1));
		data.get("patterns").add(RowFactory.create(2, 1));
		data.get("patterns").add(RowFactory.create(3, 1));
		data.get("patterns").add(RowFactory.create(4, 1));
		data.get("patterns").add(RowFactory.create(5, 1));
		builder.addTable("patterns", data.get("patterns"));

		data.put("orders", new LinkedList<Row>());
		data.get("orders").add(RowFactory.create("XJC001_20", 20, 48));
		data.get("orders").add(RowFactory.create("XJC001_45", 45, 35));
		data.get("orders").add(RowFactory.create("XJC001_50", 50, 24));
		data.get("orders").add(RowFactory.create("XJC001_55", 55, 10));
		data.get("orders").add(RowFactory.create("XJC001_75", 75, 8));
		builder.addTable("orders", data.get("orders"));

		data.put("slices", new LinkedList<Row>());
		data.get("slices").add(RowFactory.create("XJC001_20", 1, 1));
		data.get("slices").add(RowFactory.create("XJC001_45", 2, 1));
		data.get("slices").add(RowFactory.create("XJC001_50", 3, 1));
		data.get("slices").add(RowFactory.create("XJC001_55", 4, 1));
		data.get("slices").add(RowFactory.create("XJC001_75", 5, 1));
		builder.addTable("slices", data.get("slices"));
		
		builder.build();
		
		return masterData;
	}/*createSampleMasterData*/
	
	public static OPLCollector createSampleSubproblemData(Map<String, StructType> dataModel, OPLCollector masterData) {
		OPLCollector subproblemData= new OPLCollector("subproblemData").setADM(dataModel);
		OPLCollector.DataBuilder builder= subproblemData.buildData();
		
		builder.addTable("parameters", masterData.getTable("parameters"));
		builder.addTable("orders", masterData.getTable("orders"));
		builder.addEmptyTable("duals");
		
		builder.build();
		
		return subproblemData;
	}/*createSampleSubproblemData*/
	

}/*class CuttingStock*/
