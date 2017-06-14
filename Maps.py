__author__ = 'bloomj'

try:
    import folium
except:
    if hasattr(sys, 'real_prefix'):
        # we are in a virtual env.
        !pip install folium
    else:
        !pip install - -user folium

    import folium

from pyspark.sql.types import StringType, IntegerType, DoubleType


class Maps(object):
    """
    Displays a map showing warehouses and shipping routes for the Warehousing application.
    """

    def __init__(self, stores=None, warehouses=None, routes=None, routesToShow=None, mapCoordinates=None):
        """
        Creates a new Maps instance.
        Matches the warehouse locations and stores with their map coordinates.
        Creates a base map for adding location and shipment information.
        Note: If your data has more than one scenario, you should select the scenario when creating the dataframes used in this map, e.g.
        routes= warehousingResult.getTable("shipments").select('*').where(shipments.scenarioId == "nominal")

        @param stores: a Spark dataframe containing store data (note: must contain a column "store" containing the store locations and may contain other columns as well)
        @param warehouses: a Spark dataframe containing warehouse data (note: must contain a column "location" containing the warehouse locations and may contain other columns as well)
        @param routes: a Spark dataframe containing route data (note: must contain columns "location" and "store" containing the end-point locations and may contain other columns as well)
        @param routesToShow: a Spark dataframe containing (location, store) pairs specifying the subset of routes to show (default=None shows all routes)
        @param mapCoordinates: a Spark dataframe containing the geographic coordinates of the warehouse and store locations
        """

        # Match map coordinates with store and warehouse locations
        self.warehousesWithCoord = warehouses.join(mapCoordinates, "location")
        coord = mapCoordinates.withColumnRenamed("location", "store")
        self.storesWithCoord = stores.join(coord, "store")

        if routesToShow is not None:
            selectedRoutes = routes.join(routesToShow,
                                         [routes.location == routesToShow.location, routes.store == routesToShow.store]) \
                .select("*") \
                .drop(routesToShow.location).drop(routesToShow.store)
        else:
            selectedRoutes = routes

        routesWithCoord = selectedRoutes.join(mapCoordinates, "location") \
            .withColumnRenamed("lon", "locationLon") \
            .withColumnRenamed("lat", "locationLat")
        self.routesWithCoord = routesWithCoord.join(coord, "store") \
            .withColumnRenamed("lon", "storeLon") \
            .withColumnRenamed("lat", "storeLat")

        # Determine map center and range
        self.mapCenter = self.storesWithCoord.agg({"lat": "avg", "lon": "avg"}).first().asDict()
        self.mapCenter.update(self.storesWithCoord.agg({"lat": "min", "lon": "min"}).first().asDict())
        self.mapCenter.update(self.storesWithCoord.agg({"lat": "max", "lon": "max"}).first().asDict())

    def getBasicMap(self):
        """
        Returns a basic map with no data displayed.
        Use it to add data markers.
        """
        return folium.Map(location=[self.mapCenter["avg(lat)"], self.mapCenter["avg(lon)"]],
                          min_lat=self.mapCenter["min(lat)"], max_lat=self.mapCenter["max(lat)"],
                          min_lon=self.mapCenter["min(lon)"], max_lon=self.mapCenter["max(lon)"],
                          zoom_start=4)

    @staticmethod
    def makeLabel(label):
        """
        Used in formatCaption method

        @param label: a (possibly empty) string
        @return: a string
        """
        if len(label) > 0:
            return label + ": "
        else:
            return label

    @staticmethod
    def formatCaption(row, labelColumns, dataColumns):
        """
        Creates a caption for a popup on a map item from a Row in a Spark dataframe

        @param row: the current Row in a Spark dataframe
        @param labelColumns: the names of the columns with the identifier information to be displayed in a popup caption
            (dictionary with string keys representing the label column names and string values representing the labels to use in the caption (the data themselves are strings))
        @param dataColumns: the names of the columns with the data to be displayed in a popup caption
            (dictionary with string keys representing the data column names and string values representing the labels to use in the caption (the data themselves are numbers))
        @return an html text string
        """
        text = ""
        first = True
        if labelColumns:  # is not empty
            for col, label in labelColumns.iteritems():
                if first:
                    text = Maps.makeLabel(label) + row[col]
                    first = False
                else:
                    text = text + "<br />" + Maps.makeLabel(label) + row[col]
        if dataColumns:  # is not empty
            for col, label in dataColumns.iteritems():
                if first:
                    text = Maps.makeLabel(label) + str(row[col])
                    first = False
                else:
                    text = text + "<br />" + Maps.makeLabel(label) + str(row[col])

        return text

    def showWarehouses(self, tableMap=None, labelColumns={}, dataColumns={}):
        """
        Displays data from a table of warehouses in markers on a map. The table must include columns for the coordinates of each store.

        @param tableMap: the map to which this data is to be added (a folium.Map, which defaults to basicMap)
        @param dataColumns: the names of the columns with the data to be displayed in a popup caption
            (dictionary with string keys representing the data column names and string values representing the labels to use in the caption (the data themselves are numbers))
        @return tableMap: the map with markers added for the new data
        """
        if tableMap is None:
            tableMap = self.getBasicMap()

        table = self.warehousesWithCoord

        for r in table.collect():
            row = r.asDict()
            text = Maps.formatCaption(row, labelColumns, dataColumns)
            caption = folium.Popup(folium.element.IFrame(html=text, width=200, height=75), max_width=2650)
            folium.Marker([row["lat"], row["lon"]], popup=caption).add_to(tableMap)
        return tableMap

    def showStores(self, tableMap=None, labelColumns={}, dataColumns={}):
        """
        Displays data from a table of stores in markers on a map. The table must include columns for the coordinates of each store.

        @param tableMap: the map to which this data is to be added (a folium.Map, which defaults to basicMap)
        @param dataColumns: the names of the columns with the data to be displayed in a popup caption
            (dictionary with string keys representing the data column names and string values representing the labels to use in the caption (the data themselves are numbers))
        @return tableMap: the map with markers added for the new data
        """
        if tableMap is None:
            tableMap = self.getBasicMap()

        table = self.storesWithCoord

        for r in table.collect():
            row = r.asDict()
            text = Maps.formatCaption(row, labelColumns, dataColumns)
            caption = folium.Popup(folium.element.IFrame(html=text, width=200, height=75), max_width=2650)
            folium.CircleMarker([row["lat"], row["lon"]], popup=caption, radius=20, color='#FF0000',
                                fill_color='#FF0000') \
                .add_to(tableMap)
        return tableMap

    def showRoutes(self, tableMap=None, labelColumns={}, dataColumns={}, color='#FF0000'):
        """
        Displays data from a table of routes in markers on a map. The table must include columns for the coordinates of each end of the route.

        @param tableMap: the map to which this data is to be added (a folium.Map, which defaults to basicMap)
        @param dataColumns: the names of the columns with the data to be displayed in a popup caption
            (dictionary with string keys representing the data column names and string values representing the labels to use in the caption (the data themselves are numbers))
        @return tableMap: the map with markers added for the new data
        """
        if tableMap is None:
            tableMap = self.getBasicMap()

        table = self.routesWithCoord

        for r in table.collect():
            row = r.asDict()
            text = Maps.formatCaption(row, labelColumns, dataColumns)
            caption = folium.Popup(folium.element.IFrame(html=text, width=200, height=75), max_width=2650)
            tableMap.add_children(
                folium.PolyLine([[row["locationLat"], row["locationLon"]], [row["storeLat"], row["storeLon"]]], color,
                                popup=caption))

# end class Maps
