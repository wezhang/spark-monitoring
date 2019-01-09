package com.microsoft.pnp

//import com.datastax.spark.connector.cql.CassandraConnector
//import com.microsoft.pnp.log4j.LoggingConfiguration
import org.apache.spark.eventhubs.{EventHubsConf, EventPosition}
//import org.apache.spark.metrics.source.{AppAccumulators, AppMetrics}
import org.apache.spark.SparkConf
import org.apache.spark.listeners.{LogAnalyticsListener, LogAnalyticsStreamingQueryListener}
import org.apache.spark.sql.catalyst.expressions.{CsvToStructs, Expression}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.{Column, SparkSession}

object TaxiCabReader {
  private def withExpr(expr: Expression): Column = new Column(expr)

  def main(args: Array[String]) {


    // Configure our logging
  /*  TryWith(getClass.getResourceAsStream("/com/microsoft/pnp/azuredatabricksjob/log4j.properties")) {
      c => {
        LoggingConfiguration.configure(c)
      }
    }*/

    val conf = new JobConfiguration(args)
    val rideEventHubConnectionString = getSecret(
      conf.secretScope(), conf.taxiRideEventHubSecretName())
    val fareEventHubConnectionString = getSecret(
      conf.secretScope(), conf.taxiFareEventHubSecretName())

   // this is to run locally not in the cluster
    val sparkConfig = new SparkConf()
    sparkConfig.set("spark.logAnalytics.secret",
      "youralasecretgoeshere")

    sparkConfig.set("spark.logAnalytics.logType",
      "yourlogtypegoeshere")

    sparkConfig.set("spark.logAnalytics.workspaceId",
      "yourworkspaceidgoeshere")
     .setMaster("local[2]")
     .setAppName("test")




   // to run locall set the config
    val spark = SparkSession
      .builder
  //    .config(sparkConfig)
      .getOrCreate

    import spark.implicits._



 /*   @transient val appMetrics = new AppMetrics(spark.sparkContext)
    appMetrics.registerGauge("metrics.malformedrides", AppAccumulators.getRideInstance(spark.sparkContext))
    appMetrics.registerGauge("metrics.malformedfares", AppAccumulators.getFareInstance(spark.sparkContext))
    SparkEnv.get.metricsSystem.registerSource(appMetrics)*/

    @transient lazy val NeighborhoodFinder = GeoFinder.createGeoFinder(conf.neighborhoodFileURL())

    val neighborhoodFinder = (lon: Double, lat: Double) => {
      NeighborhoodFinder.getNeighborhood(lon, lat).get()
    }
    val to_neighborhood = spark.udf.register("neighborhoodFinder", neighborhoodFinder)

    def from_csv(e: Column, schema: StructType, options: Map[String, String]): Column = withExpr {
      CsvToStructs(schema, options, e.expr)
    }

    val sparkConf = spark.sparkContext.getConf



   spark.streams.addListener(new LogAnalyticsStreamingQueryListener(sparkConf))
   spark.sparkContext.addSparkListener(new LogAnalyticsListener(sparkConf))


    val rideEventHubOptions = EventHubsConf("yourtaxirideconnstring")
      .setMaxEventsPerTrigger(124000)
      .setConsumerGroup(conf.taxiRideConsumerGroup())
      .setStartingPosition(EventPosition.fromEndOfStream)
    val rideEvents = spark.readStream
      .format("eventhubs")
      .options(rideEventHubOptions.toMap)
      .load

    val fareEventHubOptions = EventHubsConf("yourtaxifareconnstring")
      .setMaxEventsPerTrigger(124000)
      .setConsumerGroup(conf.taxiFareConsumerGroup())
      .setStartingPosition(EventPosition.fromEndOfStream)
    val fareEvents = spark.readStream
      .format("eventhubs")
      .options(fareEventHubOptions.toMap)
      .load

    val transformedRides = rideEvents
      .select(
        $"body"
          .cast(StringType)
          .as("messageData"),
        from_json($"body".cast(StringType), RideSchema)
          .as("ride"))
      .transform(ds => {
        ds.withColumn(
          "errorMessage",
          when($"ride".isNull,
            lit("Error decoding JSON"))
            .otherwise(lit(null))
        )
      })

   // transformedRides.repartition(32)

 //   val malformedRides = AppAccumulators.getRideInstance(spark.sparkContext)

    val rides = transformedRides
      .filter(r => {
        if (r.isNullAt(r.fieldIndex("errorMessage"))) {
          true
        }
        else {
   //       malformedRides.add(1)
          false
        }
      })
      .select(
        $"ride.*",
        to_neighborhood($"ride.pickupLon", $"ride.pickupLat")
          .as("pickupNeighborhood"),
        to_neighborhood($"ride.dropoffLon", $"ride.dropoffLat")
          .as("dropoffNeighborhood")
      )
      .withWatermark("pickupTime", conf.taxiRideWatermarkInterval())

    val csvOptions = Map("header" -> "true", "multiLine" -> "true")
    val transformedFares = fareEvents
      .select(
        $"body"
          .cast(StringType)
          .as("messageData"),
        from_csv($"body".cast(StringType), FareSchema, csvOptions)
          .as("fare"))
      .transform(ds => {
        ds.withColumn(
          "errorMessage",
          when($"fare".isNull,
            lit("Error decoding CSV"))
            .when(to_timestamp($"fare.pickupTimeString", "yyyy-MM-dd HH:mm:ss").isNull,
              lit("Error parsing pickupTime"))
            .otherwise(lit(null))
        )
      })
      .transform(ds => {
        ds.withColumn(
          "pickupTime",
          when($"fare".isNull,
            lit(null))
            .otherwise(to_timestamp($"fare.pickupTimeString", "yyyy-MM-dd HH:mm:ss"))
        )
      })


  //  val malformedFares = AppAccumulators.getFareInstance(spark.sparkContext)

    val fares = transformedFares
      .filter(r => {
        if (r.isNullAt(r.fieldIndex("errorMessage"))) {
          true
        }
        else {
    //      malformedFares.add(1)
          false
        }
      })
      .select(
        $"fare.*",
        $"pickupTime"
      )
      .withWatermark("pickupTime", conf.taxiFareWatermarkInterval())

    val mergedTaxiTrip = rides.join(fares, Seq("medallion", "hackLicense", "vendorId", "pickupTime"))


    val maxAvgFarePerNeighborhood = mergedTaxiTrip.selectExpr("medallion", "hackLicense", "vendorId", "pickupTime", "rateCode", "storeAndForwardFlag", "dropoffTime", "passengerCount", "tripTimeInSeconds", "tripDistanceInMiles", "pickupLon", "pickupLat", "dropoffLon", "dropoffLat", "paymentType", "fareAmount", "surcharge", "mtaTax", "tipAmount", "tollsAmount", "totalAmount", "pickupNeighborhood", "dropoffNeighborhood")
      .groupBy(window($"pickupTime", conf.windowInterval()), $"pickupNeighborhood")
      .agg(
        count("*").as("rideCount"),
        sum($"fareAmount").as("totalFareAmount"),
        sum($"tipAmount").as("totalTipAmount")
      )
      .select($"window.start", $"window.end", $"pickupNeighborhood", $"rideCount", $"totalFareAmount", $"totalTipAmount")


    maxAvgFarePerNeighborhood
      .writeStream
      .queryName("maxAvgFarePerNeighborhood_console_insert")
      .outputMode(OutputMode.Append())
      .format("console")
      .option("truncate", true)
      .option("numRows", 5000)
      .start()
      .awaitTermination()


  }
}
