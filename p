package dev.bigspark.caster

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.sql._
import org.yaml.snakeyaml.Yaml
import java.io.{FileNotFoundException, IOException, InputStream}
import java.util.ArrayList
import java.util.List
import java.util.Map

//remove if not needed
import scala.collection.JavaConversions._

object EDIProcessor {

  def fetchEDISchemaFromS3(pathStr: String): EDIFileSchema = {
    //Use spark to fetch yml file and construct the schema
    val spark: SparkSession = SparkSession.builder().getOrCreate
    val yml: JavaRDD[String] =
      spark.sparkContext.textFile(pathStr + "/*.yml*", 1).toJavaRDD()
    val yml_s: String = String.join("\n", yml.collect())
    getFileSchema(yml_s)
  }

  def getFileSchema(yamlString: String): EDIFileSchema = {
    val schemaAttributes: List[EDIFileSchema.SchemaAttributes] =
      new ArrayList[EDIFileSchema.SchemaAttributes]()
    var delimiter: Int = -1
    var quotechar: Int = -1
    val yaml: Yaml = new Yaml()
    val yamlMaps: Map[String, Any] =
      yaml.load(yamlString).asInstanceOf[Map[String, Any]]
    val sourceYml: Map[String, Any] =
      yamlMaps.get("source-parameters").asInstanceOf[Map[String, Any]]
    delimiter = java.lang.Integer.parseInt(sourceYml.get("delimiter").toString)
    quotechar = java.lang.Integer.parseInt(sourceYml.get("quotechar").toString)
    val attributes: ArrayList[Any] =
      sourceYml.get("attributes").asInstanceOf[ArrayList[Any]]
    for (objColDef <- attributes) {
      val colDef: Map[String, Any] = objColDef.asInstanceOf[Map[String, Any]]
      val name: String = colDef.get("name").toString
      val `type`: String = colDef.get("type").toString
      val nullable: Boolean =
        java.lang.Boolean.parseBoolean(colDef.get("nullable").toString)
      schemaAttributes.add(
        new EDIFileSchema.SchemaAttributes(name, `type`, nullable))
    }
    new EDIFileSchema(delimiter, quotechar, schemaAttributes)
  }

  private def getEDIDataframe(fileSchema: EDIFileSchema,
                              pathStr: String): Dataset[Row] = {
    val spark: SparkSession = SparkSession.builder().getOrCreate
    //Fetch datafile and return the constructed dataframe
    spark
      .read
      .format("com.databricks.spark.csv")
      .option("header", false)
      .option("delimiter",
        java.lang.Character.toString(fileSchema.getDelimiter.toChar))
      .option("quote",
        java.lang.Character.toString(fileSchema.getQuotechar.toChar))
      .option("encoding", "UTF-8")
      .schema(fileSchema.getAttributesStruct)
      .load(pathStr + "/*.dat*")
  }

  private def getJsonDataframeFromS3(
                              pathStr: String): Dataset[Row] = {
    val spark: SparkSession = SparkSession.builder().getOrCreate
    //Fetch datafile and return the constructed dataframe
    try {
      spark
        .read
        .json(pathStr + "/*.json*")
    } catch {
      case e: Exception => {
        spark
          .read
          .json(pathStr + "/*.JSON*")
      }
    }
  }

  def fetchEDIFromS3(bucketName: String,
                     databaseName: String,
                     tableName: String,
                     instanceID: String,
                     businessDate: String,
                     format: String): Dataset[Row] = {
    val pathStr: String = String.format(
      "s3a://%s/datalake-staging/%s/%s/instance=%s/business_date=%s",
      bucketName.toLowerCase,
      databaseName.toLowerCase(),
      tableName.toLowerCase(),
      instanceID.toUpperCase,
      businessDate
    )

    if (format == "text")
      fetchEDIFromS3(pathStr)
    else if (format == "json")
      getJsonDataframeFromS3(pathStr)
    else
      throw new RuntimeException ("Invalid values provided for 'format' key")
  }

  def fetchEDIFromS3(pathStr: String): Dataset[Row] = {
    val fileSchema: EDIFileSchema = fetchEDISchemaFromS3(pathStr)
    getEDIDataframe(fileSchema, pathStr)
  }

  def setProps(endpoint: String, accessKey: String, secretKey: String): Unit = {
    val spark: SparkSession = SparkSession.builder().getOrCreate
    val hadoop_conf: Configuration = spark.sparkContext.hadoopConfiguration
    hadoop_conf.set("fs.s3a.endpoint", endpoint)
    hadoop_conf.set("fs.s3a.access.key", accessKey)
    hadoop_conf.set("fs.s3a.secret.key", secretKey)
  }

  def getFileSystem(): FileSystem = {
    val spark: SparkSession = SparkSession.builder().getOrCreate
    FileSystem.get(spark.sparkContext.hadoopConfiguration)
  }

  def getFile(location: String): InputStream = {
    val fs: FileSystem = getFileSystem
    UserGroupInformation.getCurrentUser
    val p: Path = new Path(location)
    fs.open(p)
  }

  def setPriority(databaseName: String, tableName: String): Unit = {
    val spark: SparkSession = SparkSession.builder().getOrCreate
    val priorityTables: Dataset[Row] = spark
      .read
      .format("jdbc")
      .option("driver", "oracle.jdbc.OracleDriver")
      .option("url", "jdbc:oracle:thin:@192.168.1.22:1521:xe")
      .option("dbtable", "EDH_RAW_OWNER.PRIORITY_TABLES")
      .option("user", "EDH_RAW_OWNER")
      .option("password", "EDH_RAW_OWNER")
      .load()
    priorityTables.createOrReplaceTempView("PRIORITY_TABLES")
    val priority: String = spark
      .sql(
        String.format(
          "SELECT PRIORITY " + "FROM PRIORITY_TABLES " + "WHERE DATABASE_NAME='%s' " +
            "AND TABLE_NAME='%s'",
          databaseName,
          tableName))
      .toDF()
      .collectAsList()
      .get(0)
      .toString
    if (priority.equals("TRUE")) {
      spark.sparkContext.setLocalProperty("spark.scheduler.pool", "2")
    }
  }

  def createViews(spark : SparkSession
                  ,businessDate: String
                  , instanceID : String
                  , targetDbName: String
                  , targetTableName: String) : Unit = {
    if (spark.catalog.tableExists(s"${targetDbName}.${targetTableName}")) {
      spark.sql(s"CREATE VIEW IF NOT EXISTS ${targetDbName}.${targetTableName}_v AS " +
        s"SELECT * from ${targetDbName}.${targetTableName})")
      spark.sql(s"CREATE VIEW IF NOT EXISTS ${targetDbName}.${targetTableName}_vc AS " +
        s"SELECT * from ${targetDbName}.${targetTableName} a where exists " +
        s"( select 1 from ${targetDbName}.edh_control_current cntrl WHERE " +
        s"a.edi_business_day=cntrl.edi_business_day " +
        s"and a.src_sys_inst_id=cntrl.src_sys_inst_id " +
        s"and cntrl.table_name='${targetTableName}')")
    }

  }

  //https://stackoverflow.com/questions/54060596/how-to-get-the-value-of-the-location-for-a-hive-table-using-a-spark-object
  def getHiveTablePath(tableName: String, spark: SparkSession):String =
  {
    import org.apache.spark.sql.functions._
    val sql: String = String.format("desc formatted %s", tableName)
    val result: DataFrame = spark.sql(sql).filter(col("col_name") === "Location")
    //    result.show(false) // just for debug purpose
    val info: String = result.collect().mkString(",")
    val path: String = info.split(',')(1)
    path
  }

  @throws[IOException]
  def renameRelocateFiles(sh_target_partition : String, instanceID : String): Unit = {
    val fs = getFileSystem()
    UserGroupInformation.getCurrentUser
    val p = new Path(s"${sh_target_partition}/${instanceID}")
    val files = fs.listStatus(p)
    for (file <- files) {
      if (file.getPath.getName.endsWith(".parquet")) {
        val origFileName = file.getPath.getName.replace(".parquet", "")
        val newFileName = String.format("%s-%s.parquet", instanceID, origFileName)
        fs.rename(file.getPath, new Path(String.format("%s/%s", sh_target_partition, newFileName)))
      }
    }
  }

  @throws[IOException]
  def deleteExitingFiles(sh_target_partition: String, instanceID : String): Unit = {
    val fs = getFileSystem()
    UserGroupInformation.getCurrentUser
    val p = new Path(sh_target_partition)
    //Check if exists (otherwise this will error)
    if (fs.exists(p)) {
      val files = fs.listStatus(p)
      for (file <- files) {
        if (file.getPath.getName.startsWith(instanceID)) fs.delete(file.getPath, false)
      }
    }

  }

  def tacticalSHPublish(spark : SparkSession
                        ,businessDate: String
                        , instanceID : String
                        , targetDbName: String
                        , targetTableName: String) : Unit = {
    try{
    val republish = spark.sql(s"SELECT * FROM ${targetDbName}.edh_tactical_publish where table='${targetDbName}.${targetTableName}'")
    if (republish.count > 0) {
      val sh_table = republish.select("sh_table").rdd.map(x=>x.mkString).collect
      val sh_table_s = sh_table(0)
      spark.sql(s"ALTER TABLE ${sh_table_s} ADD IF NOT EXISTS PARTITION (edi_business_day='${businessDate}')")
      val df = spark.sql(s"SELECT * FROM ${targetDbName}.${targetTableName} where edi_business_day='${businessDate}' and src_sys_inst_id='${instanceID}'")
      //.withColumn("SRC_SYS_INST_ID",lit(instanceID))
      val sh_table_path = getHiveTablePath(sh_table_s, spark)
      val sh_target_partition = sh_table_path + s"/edi_business_day=${businessDate}"
      deleteExitingFiles(sh_target_partition, instanceID)
      df.write.parquet(s"${sh_target_partition}/${instanceID}")
      renameRelocateFiles(sh_target_partition, instanceID)
      val fs = getFileSystem()
      fs.delete(new Path(s"${sh_target_partition}/${instanceID}"), true)
    }
  } catch {
      //swallow exception ?! Maybe improve this when required
      case e: Exception => None
    }
  }
}
