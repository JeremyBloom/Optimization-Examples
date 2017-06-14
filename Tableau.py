__author__ = 'bloomj'

from OPLCollector import *
from Optimizer import *

try:
    from pyspark import SparkConf, SparkContext
    from pyspark.sql import SparkSession, Row, functions
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
except ImportError as e:
    print ("Error importing Spark Modules", e)
    sys.exit(1)


class Tableau:
    """
    Demonstrates equivalence between OPL and SQL using Spark and the Warehousing example.
    Demonstrates equivalence between OPL and SQL using Spark and the Warehousing example.
    The context and theory of this class are presented in an IBM DSX sample notebook entitled
    Optimization Modeling and Relational Data (which uses the code from this file). 
    """

    def controller(self):

        # Create the warehousing data model
        networkDataModel = ADMBuilder() \
            .addSchema("warehouses", buildSchema(
                ("location", StringType()),
                ("fixedCost", DoubleType()),
                ("capacityCost", DoubleType()))) \
            .addSchema("routes", buildSchema(
                ("location", StringType()),
                ("store", StringType()),
                ("shippingCost", DoubleType()))) \
            .addSchema("stores", buildSchema(
                ("storeId", StringType()))) \
            .addSchema("mapCoordinates", buildSchema(
                ("location", StringType()),
                ("lon", DoubleType()),
                ("lat", DoubleType()))) \
            .build()

        demandDataModel = ADMBuilder() \
            .addSchema("demands", buildSchema(
                ("store", StringType()),
                ("scenarioId", StringType()),
                ("amount", DoubleType()))) \
            .addSchema("scenarios", buildSchema(
                ("id", StringType()),
                ("totalDemand", DoubleType()),
                ("periods", DoubleType()))) \
            .build()

        warehousingResultDataModel = ADMBuilder() \
            .addSchema("objectives", buildSchema(
                ("problem", StringType()),
                ("dExpr", StringType()),
                ("scenarioId", StringType()),
                ("iteration", IntegerType()),
                ("value", DoubleType()))) \
            .addSchema("openWarehouses", buildSchema(
                ("location", StringType()),
                ("scenarioId", StringType()),
                ("iteration", IntegerType()),
                ("open", IntegerType()),
                ("capacity", DoubleType()))) \
            .addSchema("shipments", buildSchema(
                ("location", StringType()),
                ("store", StringType()),
                ("scenarioId", StringType()),
                ("iteration", IntegerType()),
                ("amount", DoubleType()))) \
            .build()

        # Note: the "MapCoordinates table and the "scenarioId" and "iteration" fields are not used in this notebook but are included for use in other contexts.    URL

        credentials_1= {}

        networkDataSource = getFromObjectStorage(credentials_1, filename="Warehousing-data.json")
        demandDataSource = getFromObjectStorage(credentials_1, filename="Warehousing-sales_data-nominal_scenario.json")

        warehousingData = OPLCollector("warehousingData", networkDataModel).setJsonSource(networkDataSource).fromJSON()
        warehousingData.addTables(OPLCollector("demandData", demandDataModel).setJsonSource(demandDataSource).fromJSON())

        warehousingData.displayTable("warehouses", sys.stdout)

        #Create the tableau data model
        tableauData= OPLCollector("tableauData")
        tableauADMBuilder= tableauData.buildADM()
        tableauADMBuilder.addSchema("integerColumns", buildSchema(
            ("variable", StringType()),
            ("lower", IntegerType()),
            ("upper", IntegerType()),
            ("value", IntegerType())))
        tableauADMBuilder.addSchema("booleanColumns", SchemaBuilder()\
            .copyFields(tableauADMBuilder.referenceSchema("integerColumns"))\
            .buildSchema())
        tableauADMBuilder.addSchema("floatColumns", buildSchema(
            ("variable", StringType()),
            ("lower", DoubleType()),
            ("upper", DoubleType()),
            ("value", DoubleType())))
        tableauADMBuilder.addSchema("rows", buildSchema(
            ("cnstraint", StringType()),
            ("sense", StringType()),
            ("rhs", DoubleType())))
        tableauADMBuilder.addSchema("entries", buildSchema(
            ("cnstraint", StringType()),
            ("variable", StringType()),
            ("coefficient", DoubleType())))
        tableauADMBuilder.addSchema("objectives", buildSchema(
            ("name", StringType()),
            ("sense", StringType()),
            ("value", DoubleType())))
        tableauDataModel = tableauADMBuilder.build()

        # Create the data model to transform the warehousing data into the tableau
        tableauTransformations = OPLCollector("tableauTransformations")
        tableauTransformationsADMBuilder = tableauTransformations.buildADM()
        tableauTransformationsADMBuilder.addSchema("columns_open", SchemaBuilder()\
            .copyFields(tableauData.getADM().get("booleanColumns"))\
            .addField("location", StringType())\
            .buildSchema())
        tableauTransformationsADMBuilder.addSchema("columns_capacity", SchemaBuilder()\
            .copyFields(tableauData.getADM().get("floatColumns"))\
            .addField("location", StringType())\
            .buildSchema())
        tableauTransformationsADMBuilder.addSchema("columns_ship", SchemaBuilder()\
            .copyFields(tableauData.getADM().get("floatColumns")) \
            .addField("location", StringType())\
            .addField("store", StringType())\
            .buildSchema())
        tableauTransformationsADMBuilder.addSchema("rows_ctCapacity", SchemaBuilder()\
            .copyFields(tableauData.getADM().get("rows"))\
            .buildSchema())
        tableauTransformationsADMBuilder.addSchema("rows_ctDemand", SchemaBuilder()\
            .copyFields(tableauData.getADM().get("rows"))\
            .buildSchema())
        tableauTransformationsADMBuilder.addSchema("rows_ctSupply", SchemaBuilder()\
            .copyFields(tableauData.getADM().get("rows"))\
            .buildSchema())
        tableauTransformationsADMBuilder.addSchema("rows_dexpr", SchemaBuilder()\
            .copyFields(tableauData.getADM().get("rows"))\
            .buildSchema())
        tableauTransformationsADMBuilder.addSchema("entries_ctCapacity_capacity", SchemaBuilder()\
            .copyFields(tableauData.getADM().get("entries"))\
            .buildSchema())
        tableauTransformationsADMBuilder.addSchema("entries_ctCapacity_ship", SchemaBuilder()\
            .copyFields(tableauData.getADM().get("entries"))\
            .buildSchema())
        tableauTransformationsADMBuilder.addSchema("entries_ctDemand_ship", SchemaBuilder()\
            .copyFields(tableauData.getADM().get("entries"))\
            .buildSchema())
        tableauTransformationsADMBuilder.addSchema("entries_ctSupply_open", SchemaBuilder()\
            .copyFields(tableauData.getADM().get("entries"))\
            .buildSchema())
        tableauTransformationsADMBuilder.addSchema("entries_ctSupply_ship", SchemaBuilder()\
            .copyFields(tableauData.getADM().get("entries"))\
            .buildSchema())
        tableauTransformationsADMBuilder.addSchema("entries_dexpr_open", SchemaBuilder()\
            .copyFields(tableauData.getADM().get("entries"))\
            .buildSchema())
        tableauTransformationsADMBuilder.addSchema("entries_dexpr_capacity", SchemaBuilder()\
            .copyFields(tableauData.getADM().get("entries"))\
            .buildSchema())
        tableauTransformationsADMBuilder.addSchema("entries_dexpr_ship", SchemaBuilder()\
            .copyFields(tableauData.getADM().get("entries"))\
            .buildSchema())
        tableauTransformationsADMBuilder.build()

        tableauTransformer = tableauTransformations.buildData()

        # Create input dataframes
        warehouses = warehousingData.getTable("warehouses")
        stores = warehousingData.getTable("stores")
        routes = warehousingData.getTable("routes")
        demands = warehousingData.getTable("demands")
        scenarios = warehousingData.getTable("scenarios")
        scenarioId = scenarios.first()["id"]

        # Encode the columns
        tableauTransformer.addTable("columns_open",
            warehouses.select("location")\
                .withColumn("variable", functions.concat(functions.lit("open_"), warehouses["location"]))\
                .withColumn("upper", functions.lit(1))\
                .withColumn("lower", functions.lit(0))\
                .withColumn("value", functions.lit(0)))
        tableauTransformer.addTable("columns_capacity",
            warehouses.select("location")\
                .withColumn("variable", functions.concat(functions.lit("capacity_"), warehouses["location"]))\
                .withColumn("upper", functions.lit(1.0e20))\
                .withColumn("lower", functions.lit(0.0))\
                .withColumn("value", functions.lit(0.0)))
        tableauTransformer.addTable("columns_ship",
            routes.select("location", "store")\
                .withColumn("variable", functions.concat(functions.lit("ship_"), routes["location"], functions.lit("_"),
                                                         routes["store"]))\
                .withColumn("upper", functions.lit(1.0))\
                .withColumn("lower", functions.lit(0.0))\
                .withColumn("value", functions.lit(0.0)))

        # Encode the Constraints
        tableauTransformer.addTable("rows_ctCapacity",
            warehouses.select("location")\
                .withColumn("cnstraint", functions.concat(functions.lit("ctCapacity_"), warehouses["location"]))\
                .withColumn("sense", functions.lit("GE"))\
                .withColumn("rhs", functions.lit(0.0)))
        tableauTransformer.addTable("rows_ctDemand",
            stores.select("storeId")\
                .withColumn("cnstraint", functions.concat(functions.lit("ctDemand_"), stores["storeId"]))\
                .withColumn("sense", functions.lit("GE"))\
                .withColumn("rhs", functions.lit(1.0))\
                .withColumnRenamed("storeId", "store"))
        tableauTransformer.addTable("rows_ctSupply",
            routes.select("location", "store")\
                .withColumn("cnstraint", functions.concat(functions.lit("ctSupply_"), routes["location"], functions.lit("_"),
                                                          routes["store"]))\
                .withColumn("sense", functions.lit("GE"))\
                .withColumn("rhs", functions.lit(0.0)))
        tableauTransformer.addTable("rows_dexpr",
            SPARK_SESSION.createDataFrame(
                [   Row(cnstraint= "capitalCost", sense= "dexpr", rhs= 0.0),
                    Row(cnstraint= "operatingCost", sense= "dexpr", rhs= 0.0),
                    Row(cnstraint= "totalCost", sense= "dexpr", rhs= 0.0)],
                tableauTransformations.getADM().get("rows_dexpr"))\
            .select("cnstraint", "sense", "rhs"))    #orders the columns properly

        # Reshape the Coefficient Data into the Tableau
        tableauTransformer.addTable(
            "entries_ctCapacity_capacity",
            tableauTransformer.referenceTable("rows_ctCapacity")\
                .join(tableauTransformer.referenceTable("columns_capacity"), "location")\
                .select("cnstraint", "variable")\
                .withColumn("coefficient", functions.lit(1.0)))
        # demand at the store at the end of each route
        demandOnRoute = routes.join(
            demands.where(demands["scenarioId"] == functions.lit(scenarioId)), "store")\
                .select("location", "store", "amount").withColumnRenamed("amount", "demand")
        tableauTransformer.addTable(
            "entries_ctCapacity_ship",
            tableauTransformer.referenceTable("rows_ctCapacity")\
                .join(tableauTransformer.referenceTable("columns_ship"), "location")\
                .join(demandOnRoute, ["location", "store"])\
                .withColumn("coefficient", -demandOnRoute["demand"])\
                .select("cnstraint", "variable", "coefficient"))
        tableauTransformer.addTable(
            "entries_ctDemand_ship",
            tableauTransformer.referenceTable("rows_ctDemand")\
                .join(tableauTransformer.referenceTable("columns_ship"), "store")\
                .select("cnstraint", "variable")\
                .withColumn("coefficient", functions.lit(1.0)))
        tableauTransformer.addTable(
            "entries_ctSupply_open",
            tableauTransformer.referenceTable("rows_ctSupply")\
                .join(tableauTransformer.referenceTable("columns_open"), "location")\
                .select("cnstraint", "variable")\
                .withColumn("coefficient", functions.lit(1.0)))
        tableauTransformer.addTable(
            "entries_ctSupply_ship",
            tableauTransformer.referenceTable("rows_ctSupply")\
                .join(tableauTransformer.referenceTable("columns_ship"), ["location", "store"])\
                .select("cnstraint", "variable")\
                .withColumn("coefficient", functions.lit(-1.0)))
        rows_dexpr = tableauTransformer.referenceTable("rows_dexpr")
        tableauTransformer.addTable(
            "entries_dexpr_open",
            (rows_dexpr.where((rows_dexpr["cnstraint"] == functions.lit("capitalCost"))\
                            | (rows_dexpr["cnstraint"] == functions.lit("totalCost"))))\
                .join(tableauTransformer.referenceTable("columns_open")\
                           .join(warehouses, "location"), how="cross")\
                .select("cnstraint", "variable", "fixedCost")\
                .withColumnRenamed("fixedCost", "coefficient"))
        tableauTransformer.addTable(
            "entries_dexpr_capacity",
            (rows_dexpr.where((rows_dexpr["cnstraint"] == functions.lit("capitalCost"))\
                            | (rows_dexpr["cnstraint"] == functions.lit("totalCost"))))\
                .join(tableauTransformer.referenceTable("columns_capacity")\
                           .join(warehouses, "location"), how="cross")\
                .select("cnstraint", "variable", "capacityCost")\
                .withColumnRenamed("capacityCost", "coefficient"))
        tableauTransformer.addTable(
            "entries_dexpr_ship",
            (rows_dexpr.where((rows_dexpr["cnstraint"] == functions.lit("operatingCost"))\
                            | (rows_dexpr["cnstraint"] == functions.lit("totalCost"))))\
                .join(
                    (tableauTransformer.referenceTable("columns_ship")\
                        .join((routes.join(demandOnRoute, ["location", "store"])\
                              .withColumn("coefficient", demandOnRoute["demand"] * routes["shippingCost"])),
                              ["location", "store"])), how="cross")\
                .select("cnstraint", "variable", "coefficient"))

        tableauTransformer.build()

        # Build the input data for the tableau optimization problem
        # Drop the instance-specific keys (location and store), which are not supported in the tableau model
        tableauData.buildData()\
            .addTable("booleanColumns",
                   tableauTransformations.getTable("columns_open").drop("location"))\
            .addTable("floatColumns",
                       tableauTransformations.getTable("columns_capacity").drop("location")\
                .union(tableauTransformations.getTable("columns_ship").drop("location").drop("store")))\
            .addEmptyTable("integerColumns")\
            .addTable("rows",
                       tableauTransformations.getTable("rows_ctCapacity").drop("location")\
                .union(tableauTransformations.getTable("rows_ctDemand").drop("store"))\
                .union(tableauTransformations.getTable("rows_ctSupply").drop("location").drop("store"))\
                .union(tableauTransformations.getTable("rows_dexpr")))\
            .addTable("entries",
                       tableauTransformations.getTable("entries_ctSupply_open")\
                .union(tableauTransformations.getTable("entries_ctSupply_ship"))\
                .union(tableauTransformations.getTable("entries_ctCapacity_capacity"))\
                .union(tableauTransformations.getTable("entries_ctCapacity_ship"))\
                .union(tableauTransformations.getTable("entries_ctDemand_ship"))\
                .union(tableauTransformations.getTable("entries_dexpr_open"))\
                .union(tableauTransformations.getTable("entries_dexpr_capacity"))\
                .union(tableauTransformations.getTable("entries_dexpr_ship")))\
            .addTable("objectives",
                SPARK_SESSION.createDataFrame(
                    [Row(name= "totalCost", sense= "minimize", value= 0.0)],
                    tableauData.getADM().get("objectives"))
                .select("name", "sense", "value"))\
            .build()

        # Replace with actual items
        TableauDotMod = None
        url = None
        key = None
        tableau_data_model, tableau_inputs, tableau_optimization_problem, tableau_outputs= None

        tableauProblem = Optimizer("TableauProblem", credentials={"url": url, "key": key})\
            .setOPLModel("TableauProblem.mod",
                         modelText=[tableau_data_model, tableau_inputs, tableau_optimization_problem, tableau_outputs])\
            .setResultDataModel(ADMBuilder()\
                .addSchema("booleanDecisions", tableauData.getSchema("booleanColumns"))\
                .addSchema("integerDecisions", tableauData.getSchema("integerColumns"))\
                .addSchema("floatDecisions", tableauData.getSchema("floatColumns"))\
                .addSchema("optimalObjectives", tableauData.getSchema("objectives"))\
                .build())
        tableauResult = tableauProblem.solve(tableauData)
        tableauProblem.getSolveStatus()

        # Recover the solution
        warehousingResult = OPLCollector("warehousingResult", warehousingResultDataModel)
        resultsBuilder = warehousingResult.buildData()
        resultsBuilder.addTable("objectives",
            tableauResult.getTable("optimalObjectives").select("name", "value")\
            .withColumnRenamed("name", "dExpr")\
            .withColumn("problem", functions.lit("warehousing"))\
            .withColumn("scenarioId", functions.lit(scenarioId))\
            .withColumn("iteration", functions.lit(0)))
        resultsBuilder.addTable("openWarehouses",
            (tableauResult.getTable("booleanDecisions").select("variable", "value").withColumnRenamed("value", "open")\
             .join(tableauTransformations.getTable("columns_open"), "variable")).drop("variable")\
            .join(
                tableauResult.getTable("floatDecisions").select("variable", "value").withColumnRenamed("value", "capacity")\
                    .join(tableauTransformations.getTable("columns_capacity"), "variable").drop("variable"),
                "location")
            .select("location", "open", "capacity")\
            .withColumn("scenarioId", functions.lit(scenarioId))\
            .withColumn("iteration", functions.lit(0)))
        floatDecisions = tableauResult.getTable("floatDecisions").select("variable", "value")
        resultsBuilder.addTable("shipments",
            floatDecisions\
            .join(tableauTransformations.getTable("columns_ship"), "variable").drop("variable")\
            .join(demandOnRoute, ["location", "store"])\
            .withColumn("amount", demandOnRoute["demand"]*(floatDecisions["value"]))\
            .select("location", "store", "amount")\
            .withColumn("scenarioId", functions.lit(scenarioId))\
            .withColumn("iteration", functions.lit(0)))
        resultsBuilder.build()

        warehousingResult.displayTable("objectives")
        warehousingResult.displayTable("openWarehouses")
        # to see the lengthy shipments table, uncomment the next line
        # warehousingResult.displayTable("shipments")
    # end controller

# end class Tableau

