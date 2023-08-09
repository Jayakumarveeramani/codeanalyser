package com.util.elevate.competitor

import java.sql._
import java.time.{Instant, LocalDateTime}
import java.util.Properties
import com.util.elevate.SparkElevateJob
import com.util.elevate.competitor.CompetitorSyncJob.Sinks.ReportsSinks
import com.util.elevate.competitor.CompetitorSyncJob.Sources.{deleteQuery,rpoMrmConfig}
import com.util.elevate.competitor.Transformations._
import com.util.elevate.competitor.sink.{CompsetDiffSink, NewCompetitorsSink, RpoMrmCompConfigSink, Sink}
import com.util.elevate.competitor.tables.{RpoCompetitor, RpoCompset, RpoMrmCompConfig, RpoMrmCompConfigNew}
import com.util.elevate.config.{RmsSharedDSClass, RmsSharedDSDetails, WorkflowConfiguration, WorkflowInput}
import com.util.elevate.spark.framework.config.{ConnectionConfigDetails, DbDetails}
import com.util.elevate.spark.framework.constants.FrameworkConstants
import com.util.elevate.spark.framework.traits.BaseJob
import com.typesafe.scalalogging.LazyLogging

import javax.sql.DataSource
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.functions.{col, lit, _}
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{Dataset, _}

import scala.util.{Failure, Success, Try}

case class CompetitorTable(rc_cmptn_id: Int, rc_cmptn_nm: String, rc_last_upd_by: Option[String], rc_last_upd_ts: Option[Timestamp])

case class rpoMrmConfigForDeletion(RMCC_PROP_CODE: String, RMCC_CMPTN_ID: Int)
final class CompetitorSyncJob(
                               competitorSyncParameters: CompetitorSyncJob.Parameters,
                               spark: SparkSession,
                               sources: CompetitorSyncJob.Sources,
                               sinks: CompetitorSyncJob.Sinks,
                               connectionConfigDetails: ConnectionConfigDetails,
                               override val jobIdentifier: String,
                               outputJsonPath: String,
                               outputdatapath:String,
                               outputErrorFilePath: String,
                               schemaMap: Map[String, String],
                               sthreemap: Map[String, String],
                               dataDate: Date,
                               sdsks: Set[String],
                               properties: Properties,
                               dbDetails: DbDetails)
  extends BaseJob
    with LazyLogging {

  override val jobName: String = "CompetitorSync"

  override val isStreaming: Boolean = false

  override def process(args: Map[String, String]): Unit = {
    CompetitorSyncJob.run(
      competitorSyncParameters,
      spark,
      competitorSyncParameters.workflowInput.runid.toLong,
      connectionConfigDetails,
      jobIdentifier,
      outputJsonPath,
      outputdatapath,
      outputErrorFilePath,
      schemaMap,
      sthreemap,
      sdsks,
      sources,
      sinks,
      dataDate,
      properties,
      dbDetails
    )
  }
}

object CompetitorSyncJob extends LazyLogging
  with SparkElevateJob {

  case class Parameters(workflowInput: WorkflowInput,
                        masterfilePath: String,
                        // competitorRenamePath: String, //commented out for Removing competitor Rename logic which is out of scope
                        compsetSeasonRunIDFilePath: String,
                        seasonMapRunIDFilePath: String
                       )

  final class Sources(
                       val masterfile: Dataset[MasterFile],
                       val rpoProperties: Dataset[Property],
                       val rpoCompsets: Dataset[RpoCompset],
                       val rpoCompetitor: Dataset[CompetitorTable],
                       val rpoSeasonMap: Dataset[RpoSeasonMap],
                       val rpoCompsetSeasons: Dataset[RpoCompsetSeason]
                     )

  object Sources {
    def masterFile(spark: SparkSession, path: String): Dataset[Transformations.MasterFile] = {
      import spark.implicits._
      spark.read
        .schema(Schemas.TravelclickMasterFile)
        .option("header", value = true)
        .csv(path)
        .as[Transformations.MasterFile]
    }

    def runId(spark: SparkSession, path: String): Dataset[RunIdData] = {
      import spark.implicits._
      val df = spark.read
        .schema(Schemas.RunIdSchema)
        .option("header", value = true)
        .csv(path)
      df.as[RunIdData]
    }

    def deleteQuery(tableName: String,schemaName:String):String={
      s"DELETE FROM $schemaName.$tableName WHERE rmcc_prop_code=? and rmcc_cmptn_id=?"
    }

   /* def insertQuery(tableName: String,schemaName:String):String={
      s"INSERT INTO   $schemaName.$tableName ( rmcc_prop_code, rmcc_cmptn_id, rmcc_season_id, rmcc_we_ind, rmcc_cmptn_ind, rmcc_sys_weight, rmcc_usr_weight, rmcc_closed_fill_in_rt, rmcc_closed_fill_in_usr, rmcc_own_ind, rmcc_prd_profile_id, rmcc_last_upd_by, rmcc_last_upd_ts) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?,?)"
          }*/



    private def jdbcTable(spark: SparkSession, dbDetails: DbDetails, table: String, properties: Properties): DataFrame =
      spark.read.jdbc(dbDetails.jdbcurl, table, properties)

    private def jdbcTableAs[A](spark: SparkSession, dbDetails: DbDetails, table: String, properties: Properties)(
      implicit e: Encoder[A]): Dataset[A] =
      jdbcTable(spark, dbDetails, table, properties).as[A]

    def property(
                  spark: SparkSession,
                  dbDetails: DbDetails,
                  table: String,
                  properties: Properties): Dataset[Property] = {
      import spark.implicits._
      jdbcTableAs[Property](spark, dbDetails, table, properties)
    }

    def competitor(
                    spark: SparkSession,
                    dbDetails: DbDetails,
                    table: String,
                    properties: Properties): Dataset[CompetitorTable] = {
      import spark.implicits._
      jdbcTable(spark, dbDetails, table, properties)
        .select(
          col("RC_CMPTN_ID").as[Int],
          col("RC_CMPTN_NM").as[String],
          col("RC_LAST_UPD_BY").as[String],
          col("RC_LAST_UPD_TS").as[Timestamp]
        )
        .as[CompetitorTable]
    }
    def rpoMrmConfig(
                      spark: SparkSession,
                      dbDetails: DbDetails,
                      table: String,
                      properties: Properties): Dataset[RpoMrmCompConfigNew] = {
      import spark.implicits._

      jdbcTable(spark, dbDetails, table, properties)
        .select(
          col("RMCC_PROP_CODE").as[String],
          col("RMCC_CMPTN_ID").as[Int],
          col("RMCC_SEASON_ID").as[Int],
          col("RMCC_WE_IND").as[Int],
          col("RMCC_CMPTN_IND").as[String],
          col("RMCC_SYS_WEIGHT").as[BigDecimal],
          col("RMCC_USR_WEIGHT").as[BigDecimal],
          col("RMCC_CLOSED_FILL_IN_RT").as[BigDecimal],
          col("RMCC_CLOSED_FILL_IN_USR").as[BigDecimal],
          col("RMCC_OWN_IND").as[String],
          col("RMCC_PRD_PROFILE_ID").as[Int],
          col("RMCC_LAST_UPD_BY").as[String],
          col("RMCC_LAST_UPD_TS").as[Timestamp]
        )
        .as[RpoMrmCompConfigNew]
    }

    def rpoCompset(
                    spark: SparkSession,
                    dbDetails: DbDetails,
                    table: String,
                    properties: Properties): Dataset[RpoCompset] = {
      import spark.implicits._

      jdbcTable(spark, dbDetails, table, properties)
        .select(
          col("RC_PROP_CODE").as[String],
          col("RC_CMPTN_ID").as[Int],
          col("RC_OWN_IND").as[String],
          col("RC_ACTIVE_IND").as[String],
          col("RC_EFF_START_DATE").as[Date],
          col("RC_EFF_END_DATE").as[Date],
          col("RC_LAST_UPD_BY").as[String],
          col("RC_LAST_UPD_TS").as[Timestamp]
        )
        .as[RpoCompset]
    }

    def rpoCompetitortRenames(spark: SparkSession, path: String): Dataset[RpoCompetitorRename] = {
      import spark.implicits._
      val schema = Encoders.product[RpoCompetitorRename].schema
      spark.read
        .option("header", value = true)
        .schema(schema)
        .csv(path)
        .as[RpoCompetitorRename]
    }

    def rpoSeasonMap(
                      spark: SparkSession,
                      dbDetails: DbDetails,
                      table: String,
                      rpoSeasonMapFunIdDs: Dataset[RunIdData],
                      properties: Properties): Dataset[RpoSeasonMap] = {
      import spark.implicits._
      jdbcTableAs[RpoSeasonMap](spark, dbDetails, table, properties).
        join(rpoSeasonMapFunIdDs,
          rpoSeasonMapFunIdDs("propertyCode")===col("rsm_prop_code") &&
            rpoSeasonMapFunIdDs("runId")===col("rsm_run_id")).as[RpoSeasonMap]
    }



    def rpoCompsetSeasons(
                           spark: SparkSession,
                           dbDetails: DbDetails,
                           table: String,
                           compsetSeasonFunIdDs: Dataset[RunIdData],
                           properties: Properties): Dataset[RpoCompsetSeason] = {
      import spark.implicits._
      jdbcTable(spark, dbDetails, table, properties).
        join(compsetSeasonFunIdDs,
          compsetSeasonFunIdDs("propertyCode")===col("RCS_PROP_CODE") &&
            compsetSeasonFunIdDs("runId")===col("RCS_RUN_ID")).
        select(
          col("RCS_RUN_ID").as[String],
          col("RCS_DATA_DATE").as[Int],
          col("RCS_PROP_CODE").as[String],
          col("RCS_CMPTN_ID").as[String],
          col("RCS_SEASON_ID").as[Date],
          col("RCS_WE_IND").as[String],
          col("RCS_CMPTN_IND").as[Timestamp],
          col("RCS_OWN_IND").as[Timestamp],
          col("RCS_LAST_UPD_BY").as[String],
          col("RCS_LAST_UPD_TS").as[Timestamp]
        )
        .as[RpoCompsetSeason]
    }

    /** Factory method to create an instance of sources from config params */
    def from(
              spark: SparkSession,
              dbDetails: DbDetails,
              properties: Properties,
              schemaMap: Map[String, String],
              masterFilePath: String,
              // competitorRenamesPath: String,
              compsetSeasonRunIDFilePath: String,
              seasonMapRunIDFilePath: String): Sources = {
      val compsetSeasonRunIdDs = runId(spark,compsetSeasonRunIDFilePath)
      val rpoSeasonMapFunIdDs = runId(spark,seasonMapRunIDFilePath)
      new Sources(
        masterFile(spark, masterFilePath),
        property(spark, dbDetails, schemaMap("SchemaName") +"." + schemaMap("property"), properties),
        rpoCompset(spark, dbDetails, schemaMap("SchemaName") +"." + schemaMap("compset"), properties),
        competitor(spark, dbDetails, schemaMap("SchemaName") +"." + schemaMap("competitor"), properties),
        rpoSeasonMap(
          spark,
          dbDetails,
          schemaMap("SchemaName") +"." + schemaMap("seasonMap"),
          rpoSeasonMapFunIdDs,
          properties
        ),
        rpoCompsetSeasons(
          spark,
          dbDetails,
          schemaMap("SchemaName") +"." + schemaMap("compsetSeason"),
          compsetSeasonRunIdDs,
          properties)
      )
    }
  }

  final class Sinks(
                     val reportsSinks: ReportsSinks,
                     val newCompetitorsSink: Sink[RpoCompetitor],
                     val compsetDiffSink: Sink[CompsetDiff],
                     val rpoMrmCompConfigSink: Sink[RpoMrmCompConfig]
                   )

  object Sinks {

    final class ReportsSinks(
                              val malformedMarshaCodes: Sink[Row],
                              val duplicatePropertyCodes: Sink[Row],
                              val nonExistantPropertyCodes: Sink[Row],
                              val rubiconIdMismatches: Sink[Row]
                            )


    object ReportsSinks {
      def apply(outputerrorfilepath: String): ReportsSinks = {
        def forPath[T](path: String) = new Sink[T] {
          override def write(spark: SparkSession, dataset: Dataset[T]): Unit = {
            if (dataset.limit(1).count() > 0) {
              logger.info(s"writing $path ...")
              dataset.coalesce(1).write.option("header", true).mode(SaveMode.Overwrite).csv(path)
            }
          }
        }

        new ReportsSinks(
          malformedMarshaCodes = forPath(s"${outputerrorfilepath.stripSuffix("/")}/malformed-marsha-codes"),
          duplicatePropertyCodes = forPath(s"${outputerrorfilepath.stripSuffix("/")}/duplicate-property-codes"),
          nonExistantPropertyCodes = forPath(s"${outputerrorfilepath.stripSuffix("/")}/non-existent-property-codes"),
          rubiconIdMismatches = forPath(s"${outputerrorfilepath.stripSuffix("/")}/rubicon-id-mismatches")
        )
      }
    }

    def defaults(dataSource: DataSource, outputErrorFilePath: String, schemaMap : Map[String,String]): Sinks = {
      val dbSchema = schemaMap("SchemaName")
      new Sinks(
        reportsSinks = ReportsSinks(outputErrorFilePath),
        newCompetitorsSink = NewCompetitorsSink(dataSource,dbSchema),
        compsetDiffSink = CompsetDiffSink(dataSource,dbSchema),
        //        rpoCompsetSeasonSink = RpoCompsetSeasonSink(dataSource),
        rpoMrmCompConfigSink = RpoMrmCompConfigSink(dataSource,dbSchema)
      )
    }
  }

  def run(
           parameters: Parameters,
           spark: SparkSession,
           runId: Long,
           connectionConfigDetails: ConnectionConfigDetails,
           jobName: String,
           outputJsonPath: String,
           outputDataPath:String,
           outputErrorFilePath: String,
           schemaMap: Map[String, String],
           sthreemap: Map[String, String],
           sdsks: Set[String],
           sources: Sources,
           sinks: Sinks,
           dataDate: Date,
           properties: Properties,
           dbDetails: DbDetails
         ): Unit = {
    val status = Try {
      val now: Instant = Instant.now()
      val nowTs: Timestamp = Timestamp.from(now)

      // ==============================================================================
      // LLD Step 2

      /* includes

      invalidMarshaCodeRecords: Dataset[MasterFile]
        ^-- rows in Masterfile with invalid marsha codes
      nonSingleHostRecords: Dataset[MasterFileCompset]
        ^-- groupings of Masterfile rows which had too few/too many hosts
      compsets: Dataset[Compset]
        ^-- remaining, valid groupings of (host, competitors)

       */

      val cachedMasterFile = sources.masterfile.cache()
      val masterFileOutput: Transformations.MasterFileOutput =
        Transformations.processAndCleanMasterfile(spark, cachedMasterFile)
      val (validMarshaCodes, invalidMarshaCodes) = filterCanonicalizeMarshaCodes(spark, cachedMasterFile)

      // ==============================================================================
      // LLD Step 3

      // (Dataset[Compset], Dataset[Compset])
      val (withExistingPropcodes, withoutExistingPropCodes) =
        Transformations.hostsWithExistingPropCodes(spark, masterFileOutput.compsets, sources.rpoProperties)

      // ==============================================================================
      // LLD Step 4
      import spark.implicits._
      // (Dataset[Compset], Dataset[Compset])
      val (withMatchingCmptnId, withoutMatchingCmptnId, hostNotInCompset) =
        Transformations.hostsWithMatchingCmptnId(spark, withExistingPropcodes, sources.rpoCompsets)

      val newHostcompsetDsdup = Transformations.constructNewHostDs(spark, hostNotInCompset, nowTs)

      val rpoCompsetTable = readTable(spark, connectionConfigDetails.dbconfig.get, "rpo_compset", schemaMap("SchemaName"))

      val newHostCompsetDs = newHostcompsetDsdup.join(rpoCompsetTable, newHostcompsetDsdup("RC_PROP_CODE") === rpoCompsetTable("rc_prop_code")
        && newHostcompsetDsdup("RC_CMPTN_ID") === rpoCompsetTable("rc_cmptn_id"), "left_anti").as[RpoCompset]

      Transformations.persistNewHostInCompset(spark, newHostCompsetDs, connectionConfigDetails.dbconfig.get, schemaMap)

      val compIdName = sources.masterfile
        .withColumn("CMPTN_ID", col("RUBICON_PROP_ID").cast(IntegerType))
        .withColumn("CMPTN_NAME", concat(col("CHAIN_NAME"), lit(" "), col("HOTEL_NAME")))
        .select("CMPTN_ID", "CMPTN_NAME").as[RpoCompetitorRename]

      val competitorRename = Transformations.getCompetitorRenameRecords(spark, compIdName, sources.rpoCompetitor)

      // ==============================================================================
      // LLD Step 5

      val withCompetitorsRenamed: Dataset[Compset] =
        Transformations.renameCompetitors(spark, withMatchingCmptnId,
          competitorRename)

      val withHostRenamed: Dataset[RpoCompetitor] = Transformations.renameHost(spark, withMatchingCmptnId,
        competitorRename)

      val removeDuplicateCompetitorsRename = Transformations.removeDuplicateCompetitorsRename(spark, withCompetitorsRenamed, sources.rpoCompetitor)

      // ==============================================================================
      // LLD Step 6
      // no longer included, rpo_process is not a table anymore

      // ==============================================================================
      // LLD Step 7
      val rpoCompetitors: Dataset[RpoCompetitor] =
      Transformations.computeRpoCompetitors(spark, removeDuplicateCompetitorsRename, "batch", nowTs).union(withHostRenamed)
      sinks.newCompetitorsSink.write(spark, rpoCompetitors)
      // ==============================================================================
      // LLD Step 8

      val compsetDiffs: Dataset[CompsetDiff] =
        Transformations.compsetDiffs(spark, withCompetitorsRenamed, sources.rpoCompsets)

      sinks.compsetDiffSink.write(spark, compsetDiffs)
      // ==============================================================================
      // LLD Step 9

      val rpoCompsetSeasons: Dataset[RpoCompsetSeason] =
        Transformations.computeRpoCompsetSeasons(
          spark = spark,
          runId = parameters.workflowInput.runid.toLong,
          dataDate = dataDate,
          now = now,
          compsets = withCompetitorsRenamed,
          rpoSeasonMaps = sources.rpoSeasonMap
        )

      val rpoMrmCompConfigsForUpsertion =
        Transformations.computeRpoMrmCompConfigsForUpsertion(
          spark = spark,
          runId = parameters.workflowInput.runid.toLong,
          now = nowTs,
          compsetSeasonsFromFile = rpoCompsetSeasons
         // compsetSeasonsFromDb = sources.rpoCompsetSeasons
        ).distinct()

      val propCodes = validMarshaCodes.select("MARSHA").collect().map(_.getString(0)).toSet.toList
      logger.info("propCodes :: " + propCodes)

      val rpoMrmConfigTableRecords = rpoMrmConfig(spark,
        dbDetails,
        schemaMap("SchemaName") + "." + schemaMap("compConfig"),
        properties).filter(col("RMCC_PROP_CODE").isin(propCodes:_*))

      val newCompetitors = rpoMrmCompConfigsForUpsertion.join(rpoMrmConfigTableRecords, Seq("RMCC_PROP_CODE","RMCC_CMPTN_ID"),
        /*rpoMrmCompConfigsForUpsertion.col("MARSHA").equalTo(rpoMrmConfigTableRecords.col("RMCC_PROP_CODE"))
          and validMarshaCodes.col("RUBICON_PROP_ID").equalTo(rpoMrmConfigTableRecords.col("RMCC_CMPTN_ID")),*/ "leftanti").as[RpoMrmCompConfig]

      newCompetitors.show()



      val removeCompetitors = rpoMrmConfigTableRecords.join(rpoMrmCompConfigsForUpsertion, Seq("RMCC_PROP_CODE","RMCC_CMPTN_ID"),
        /*rpoMrmCompConfigsForUpsertion.col("MARSHA").equalTo(rpoMrmConfigTableRecords.col("RMCC_PROP_CODE"))
          and validMarshaCodes.col("RUBICON_PROP_ID").equalTo(rpoMrmConfigTableRecords.col("RMCC_CMPTN_ID")),*/ "leftanti")
        .select("RMCC_PROP_CODE","RMCC_CMPTN_ID").as[rpoMrmConfigForDeletion].dropDuplicates().cache()

      logger.info(s"removeCompetitors Count :: ${removeCompetitors.count()} ")


      val sqlQueryDelete = deleteQuery(schemaMap("compConfig"),schemaMap("SchemaName"))

      //val sqlQueryInsert = insertQuery(schemaMap("compConfig"),schemaMap("SchemaName"))

      removeCompetitors.coalesce(2)
        .foreachPartition(batch=> try{
          val dbDetails = connectionConfigDetails.dbconfig.get
          val dbc = DriverManager.getConnection(dbDetails.jdbcurl, dbDetails.username, dbDetails.password)
          val pst = dbc.prepareStatement(sqlQueryDelete)

          batch.grouped(100).foreach { rows =>
            rows.foreach  { x =>
              pst.setString(1, x.RMCC_PROP_CODE)
              pst.setInt(2, x.RMCC_CMPTN_ID)
              pst.addBatch()

            }
            pst.executeBatch()
          }
          pst.closeOnCompletion()
          dbc.close()
        })




      val prop = new java.util.Properties()
      prop.setProperty("user", dbDetails.username)
      prop.setProperty("password", dbDetails.password)
      prop.setProperty("driver", dbDetails.drivername)

     /* newCompetitors.write
        .mode("append")
        .jdbc(dbDetails.jdbcurl,
          schemaMap("SchemaName") + "." + schemaMap("compConfig")
          , prop)*/


      /*newCompetitors
        .map(x => {
          Try {
            val dbDetails = connectionConfigDetails.dbconfig.get
            val dbc = DriverManager.getConnection(dbDetails.jdbcurl, dbDetails.username, dbDetails.password)
            val pst = dbc.prepareStatement(sqlQueryInsert)

            pst.setString(1, x.RMCC_PROP_CODE)
            pst.setInt(2, x.RMCC_CMPTN_ID)
            if (x.RMCC_SEASON_ID == null) pst.setNull(3, Types.INTEGER)
            else pst.setInt(3, x.RMCC_SEASON_ID)
            pst.setString(4, x.RMCC_WE_IND)
            pst.setString(5, x.RMCC_CMPTN_IND)
            if (x.RMCC_SYS_WEIGHT == null) pst.setNull(6, Types.DECIMAL)
            else pst.setDouble(6, x.RMCC_SYS_WEIGHT)
            if (x.RMCC_USR_WEIGHT == null) pst.setNull(7, Types.DECIMAL)
            else pst.setDouble(7, x.RMCC_USR_WEIGHT)
            if (x.RMCC_CLOSED_FILL_IN_RT == null) pst.setNull(8, Types.DECIMAL)
            else pst.setDouble(8, x.RMCC_CLOSED_FILL_IN_RT)
            if (x.RMCC_CLOSED_FILL_IN_USR == null) pst.setNull(9, Types.DECIMAL)
            else pst.setDouble(9, x.RMCC_CLOSED_FILL_IN_USR)
            pst.setString(10, x.RMCC_OWN_IND)
            if (x.RMCC_PRD_PROFILE_ID == null) pst.setNull(11, Types.INTEGER)
            else pst.setInt(11, x.RMCC_PRD_PROFILE_ID)
            pst.setString(12, x.RMCC_LAST_UPD_BY)
            pst.setTimestamp(13, x.RMCC_LAST_UPD_TS)

            pst.executeBatch()

            dbc.close()
          }


        })*/



      // For MRM Scheduling
      val bucketName = connectionConfigDetails.bucketconfig.get(FrameworkConstants.defaultBucket)
      val readCompConfig = spark.read.jdbc(dbDetails.jdbcurl, schemaMap("SchemaName") + "." + schemaMap("compConfig"), properties)
      readCompConfig.coalesce(1).write.mode("overwrite").csv(bucketName.concat(outputDataPath).concat("MRMCOMPCONFIG"))


         removeCompetitors.union(newCompetitors.select("RMCC_PROP_CODE","RMCC_CMPTN_ID").as[rpoMrmConfigForDeletion]).withColumn("pgmcode", lit("MRC"))
        .select("rmcc_prop_code", "pgmcode").dropDuplicates()
        .coalesce(1).write.mode("overwrite").csv(bucketName.concat(outputDataPath).concat("MRMSCHEDULING"))


      /*newCompetitors.coalesce(1).write.format("csv").option("header","true").mode("Overwrite")
      .save("C:\\Compsyncinput\\newcompetitor")*/


      logger.info(s"newCompetitors Count :: ${newCompetitors.count()} ")

      sinks.rpoMrmCompConfigSink.write(spark, newCompetitors.distinct())

      logger.info(s"newCompetitors Count after write " + newCompetitors.count())




      // ==============================================================================

      sinks.reportsSinks.malformedMarshaCodes.write(spark, masterFileOutput.invalidMarshaCodeRecords.toDF())
      sinks.reportsSinks.duplicatePropertyCodes
        .write(
          spark,
          cachedMasterFile
            .as("m")
            .joinWith(
              masterFileOutput.nonSingleHostRecords.toDF().select(col("PROP_CODE")).distinct().as("error"),
              col("m.CONTROL") === "H" && col("m.MARSHA") === col("error.PROP_CODE"))
            .select(col("_1.*"))
        )

      sinks.reportsSinks.nonExistantPropertyCodes
        .write(
          spark,
          cachedMasterFile
            .as("m")
            .joinWith(
              withoutExistingPropCodes.toDF().select(col("host.PROP_CODE").as("PROP_CODE")).distinct().as("error"),
              col("m.MARSHA") === col("error.PROP_CODE"),
              "inner"
            )
            .select(col("_1.*"))
        )

      val rubiconIdMismatchesDf = cachedMasterFile
        .as("m")
        .joinWith(
          withoutMatchingCmptnId.toDF().select(col("host.PROP_CODE").as("PROP_CODE")).distinct().as("error"),
          col("m.MARSHA") === col("error.PROP_CODE"),
          "inner"
        )
        .select(col("_1.*"))
      sinks.reportsSinks.rubiconIdMismatches
        .write(spark,rubiconIdMismatchesDf)
    }

    generateOutputReport(
      status = status,
      connectionConfigDetails = connectionConfigDetails,
      spark = spark,
      workflowInput = parameters.workflowInput,
      jobName = jobName,
      outputJsonPath = outputJsonPath,
      outputDataPath = outputDataPath,
      outputErrorFilePath = outputErrorFilePath,
      schemaMap = schemaMap,
      sthreemap = sthreemap,
      sdsks = sdsks
    )

  }

  /**
   * Generates RMS Shared Dataset output report for Oozie
   */
  def generateOutputReport(
                            status: Try[Unit],
                            connectionConfigDetails: ConnectionConfigDetails,
                            spark: SparkSession,
                            workflowInput: WorkflowInput,
                            jobName: String,
                            outputJsonPath: String,
                            outputDataPath:String,
                            outputErrorFilePath: String,
                            schemaMap: Map[String, String],
                            sthreemap: Map[String, String],
                            sdsks: Set[String]) = {

    val bucketName = connectionConfigDetails.bucketconfig.get(FrameworkConstants.defaultBucket)
    val basePath = if(StringUtils.isBlank(bucketName)) bucketName else bucketName + "/"
    status match {
      case Success(_) => {
        val dataSetIdMap = Map(
          sthreemap("competitorDataSetID") -> schemaMap("competitor"),
          sthreemap("compsetDataSetID") -> schemaMap("compset"),
          sthreemap("compConfigDataSetID") -> schemaMap("compConfig"),
          sthreemap("compConfigDataDataSetID") -> bucketName.concat(outputDataPath).concat("MRMCOMPCONFIG"),
          sthreemap("mrmScheduleDataSetID") -> bucketName.concat(outputDataPath).concat("MRMSCHEDULING"))
        val featureID = sthreemap("featureID")
        val currentTime = LocalDateTime.now()
        val sdsk = sdsks.mkString(",")
        val properties = Seq(WorkflowConfiguration.ALL_PROPERTIES)
        RmsSharedDSDetails.loadPeriodRmsConfigurations(sthreemap) match {
          case Success(periods) => {
            val validityPeriod = periods._1
            val warningPeriod = periods._2
            val rmsSharedDataset = RmsSharedDSDetails.makeAll(
              featureID,
              workflowInput.runid.toString,
              currentTime,
              sdsk,
              dataSetIdMap,
              true,
              jobName,
              validityPeriod,
              warningPeriod,
              properties,
              workflowInput.datadate.toString,
              workflowInput.datadate.toString,
              workflowInput.datadate.toString,
              workflowInput.datadate.toString,
              workflowInput.asofdate.toString
            )
            val rmsData = Seq(RmsSharedDSClass(rmsSharedDataset))
            val rmsoutpath = basePath + outputJsonPath
            logger.info(s"Saving output RMS Json file $rmsoutpath}")
            import spark.implicits._
            rmsData.toDS()
              .coalesce(1).write
              .format("json")
              .mode("overwrite")
              .save(rmsoutpath)
          }
          case Failure(ex) => logger.error("Error generating RMS Data!", ex)
        }
      }
      case Failure(exception) =>
        val erroroutpath =   basePath + outputErrorFilePath
        logger.error(s"Saving error file $erroroutpath")
        logger.error(s" exception is : $exception")
        spark.sparkContext
          .parallelize(Seq(exception.getStackTrace))
          .repartition(1)
          .saveAsTextFile(erroroutpath)
        throw exception
    }
  }

}