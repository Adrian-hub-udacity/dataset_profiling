///////////////////////////////////#########################EDI PRocessor#######################///////////////////////////////////////////////////////////////////////

/////////////////EDIFileSchema////////////////////////
package dev.bigspark.caster
class EDIFileSchema(@scala.beans.BeanProperty var delimiter : scala.Int, @scala.beans.BeanProperty var quotechar : scala.Int, @scala.beans.BeanProperty var attributes : java.util.List[dev.bigspark.caster.EDIFileSchema.SchemaAttributes]) extends scala.AnyRef {
  def getDelimiter() : scala.Int = { /* compiled code */ }
  def setDelimiter(x$1 : scala.Int) : scala.Unit = { /* compiled code */ }
  def getQuotechar() : scala.Int = { /* compiled code */ }
  def setQuotechar(x$1 : scala.Int) : scala.Unit = { /* compiled code */ }
  def getAttributes() : java.util.List[dev.bigspark.caster.EDIFileSchema.SchemaAttributes] = { /* compiled code */ }
  def setAttributes(x$1 : java.util.List[dev.bigspark.caster.EDIFileSchema.SchemaAttributes]) : scala.Unit = { /* compiled code */ }
  def getAttributesStruct() : org.apache.spark.sql.types.StructType = { /* compiled code */ }
}
object EDIFileSchema extends scala.AnyRef {
  class SchemaAttributes(@scala.beans.BeanProperty var name : scala.Predef.String, @scala.beans.BeanProperty var `type` : scala.Predef.String, @scala.beans.BeanProperty var nullable : java.lang.Boolean) extends scala.AnyRef {
    def getName() : scala.Predef.String = { /* compiled code */ }
    def setName(x$1 : scala.Predef.String) : scala.Unit = { /* compiled code */ }
    def getType() : scala.Predef.String = { /* compiled code */ }
    def setType(x$1 : scala.Predef.String) : scala.Unit = { /* compiled code */ }
    def getNullable() : java.lang.Boolean = { /* compiled code */ }
    def setNullable(x$1 : java.lang.Boolean) : scala.Unit = { /* compiled code */ }
  }
  def getDataTypeForSchema(colName : scala.Predef.String, typeString : scala.Predef.String) : org.apache.spark.sql.types.DataType = { /* compiled code */ }
}


/////////////////EDIProcessor////////////////////////////////
package dev.bigspark.caster
object EDIProcessor extends scala.AnyRef {
  def fetchEDISchemaFromS3(pathStr : scala.Predef.String) : dev.bigspark.caster.EDIFileSchema = { /* compiled code */ }
  def getFileSchema(yamlString : scala.Predef.String) : dev.bigspark.caster.EDIFileSchema = { /* compiled code */ }
  def fetchEDIFromS3(bucketName : scala.Predef.String, databaseName : scala.Predef.String, tableName : scala.Predef.String, instanceID : scala.Predef.String, businessDate : scala.Predef.String, format : scala.Predef.String) : org.apache.spark.sql.Dataset[org.apache.spark.sql.Row] = { /* compiled code */ }
  def fetchEDIFromS3(pathStr : scala.Predef.String) : org.apache.spark.sql.Dataset[org.apache.spark.sql.Row] = { /* compiled code */ }
  def setProps(endpoint : scala.Predef.String, accessKey : scala.Predef.String, secretKey : scala.Predef.String) : scala.Unit = { /* compiled code */ }
  def getFileSystem() : org.apache.hadoop.fs.FileSystem = { /* compiled code */ }
  def getFile(location : scala.Predef.String) : java.io.InputStream = { /* compiled code */ }
  def setPriority(databaseName : scala.Predef.String, tableName : scala.Predef.String) : scala.Unit = { /* compiled code */ }
  def createViews(spark : org.apache.spark.sql.SparkSession, businessDate : scala.Predef.String, instanceID : scala.Predef.String, targetDbName : scala.Predef.String, targetTableName : scala.Predef.String) : scala.Unit = { /* compiled code */ }
  def getHiveTablePath(tableName : scala.Predef.String, spark : org.apache.spark.sql.SparkSession) : scala.Predef.String = { /* compiled code */ }
  @scala.throws[java.io.IOException]
  def renameRelocateFiles(sh_target_partition : scala.Predef.String, instanceID : scala.Predef.String) : scala.Unit = { /* compiled code */ }
  @scala.throws[java.io.IOException]
  def deleteExitingFiles(sh_target_partition : scala.Predef.String, instanceID : scala.Predef.String) : scala.Unit = { /* compiled code */ }
  def tacticalSHPublish(spark : org.apache.spark.sql.SparkSession, businessDate : scala.Predef.String, instanceID : scala.Predef.String, targetDbName : scala.Predef.String, targetTableName : scala.Predef.String) : scala.Unit = { /* compiled code */ }
}

//////////////////////////////////################Dequee Plugin####################////////////////////////////////////////////////////////////////////////////////////


//////////////////////////////////smoketest.sh/////////////////////////////////////////////////////////////////////////src/main/resources/bin/
#!/usr/bin/env bash

export PROJECT_DIR="$HOME/workspace/bigspark-deequ"
export PROJECT_JAR="$PROJECT_DIR/target/deequ-plugin-1.0-SNAPSHOT.jar"
export LOG4J="file://${PROJECT_DIR}/src/main/resources/log4j.properties"
export SPARK_MASTER="yarn"

#Assumes correct environment is set e.g. switch dev
#export PROJECT_JAR="$WHEREAMI/../lib/cluster-management-1.2-SNAPSHOT.jar"
echo "SPARK_HOME=$SPARK_HOME"
echo "HADOOP_CONF_DIR=$HADOOP_CONF_DIR"
echo "HIVE_CONF_DIR=$HIVE_CONF_DIR"
echo "KEYTAB=$KEYTAB"
echo "PRINCIPAL=$PRINCIPAL"
echo "KRB5_CONFIG=$KRB5_CONFIG"
echo "PROJECT_JAR=$PROJECT_JAR"
unset SPARK_CONF_DIR

$SPARK_HOME/bin/spark-submit \
            --master yarn \
            --deploy-mode cluster \
            --keytab "${KEYTAB}" \
            --principal "${PRINCIPAL}" \
            --class dev.bispark.deequ.DeequSmokeTest \
            $PROJECT_JAR "$@"

 #--conf spark.driver.extraJavaOptions="-Djava.security.krb5.conf=$KRB5_CONFIG" \
#--conf spark.driver.extraJavaOptions="-Dlog4j.configuration=$LOG4J  -Djava.security.krb5.conf=$KRB5_CONFIG" \

///////////////////////////////log4j.properties//////////////////////////////////////////////////////////////////////////src/main/resources/bin/
log4j.rootLogger=ERROR, stdout
log4j.logger.dev.bigspark=INFO
log4j.logger.org=ERROR
log4j.appender.stdout=org.apache.log4j.ConsoleAppender
#log4j.appender.stdout.layout=org.apache.log4j.PatternLayout
#og4j.appender.stdout.layout.ConversionPattern=%d [%t] %-5p %c - %m%n
log4j.appender.stdout.layout=org.apache.log4j.EnhancedPatternLayout
log4j.appender.stdout.layout.ConversionPattern=%d [%t] %-5p %c{1.} [%t]: %m%n

///////////////////////////////	DeequRunnerConfiguration.scala//////////////////////////////////////////////////////////////src\main\scala\dev\bigspark\deequ\configuration\DeequRunnerConfiguration.scala

package dev.bigspark.deequ.configuration

/**
 * Class representing the minimal Configuration required for a complete bigspark-deequ run
 *
 * @param ruleDbName             The name of the database containing the rules table
 * @param ruleTableName          The name of the rules table
 * @param targetDbName           the name of the target database being analysed
 * @param targetTableName        The name of the target table being analysed
 * @param resultFsPath           The FileSystem path of the results database
 * @param resultDbName           The name of the results database
 * @param resultTablePattern     The pattern of the results table - At runtime we will attempt to String.format in
 *                               the sub-type of table, currently "analysis" and "check".  So for example deequ_%s_results
 *                               will give us:
 *                               1. A checks table containing check results called deequ_check_results
 *                               2. A analysis table containing analyzer results called deequ_analyser_results
 *                               Failure to include a '%s" somewhere in the pattern will cause a failure on an bigspark-deequ
 *                               facilitated run!  You have been warned!
 * @param partitionSpecification The partition specification of both the target table, and thus composing part of the results table partition specification
 */
case class DeequRunnerConfiguration(
                                     ruleDbName: String
                                     , ruleTableName: String
                                     , targetDbName: String
                                     , targetTableName: String
                                     , resultFsPath: String
                                     , resultDbName: String
                                     , resultTablePattern: String
                                     , partitionSpecification: Seq[String]
                                   )

/////////////////////////////DeequProcessingException.scala///////////////////////////////////////////////////////////////////src\main\scala\dev\bigspark\deequ\exception\DeequProcessingException.scala
package dev.bigspark.deequ.exception

/**
 * Class encapsulating Generic Deequ exceptions
 *
 * @param msg
 */
class DeequProcessingException(msg: String) extends Exception(msg)

///////////////////////////DeequValidationException.scala////////////////////////////////////////////////////////////////////////////src\main\scala\dev\bigspark\deequ\exception\DeequValidationException.scala
package dev.bigspark.deequ.exception

/**
 * Class encapsulating Generic Deequ exceptions
 *
 * @param msg
 */
class DeequValidationException(msg: String) extends Exception(msg)
////////////////////////////PartitionKey.scala/////////////////////////src\main\scala\dev\bigspark\deequ\partition\PartitionKey.scala
package dev.bigspark.deequ.partition

import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.{write, writePretty}

/**
 * Class to represent a partition specification and its component values
 *
 * @param components The (sub) components of the PartitionKey
 */
case class PartitionKey(components: Seq[PartitionKeyComponent]) {
  /**
   * JSON formats
   */
  implicit val formats = DefaultFormats + DataTypeSerializer

  /**
   * Method to return a SQL string that can be applied in a df.where(<sql>) operation
   *
   * @return The SQL for the where clause
   */
  def toSparkSqlWhereString(): String = {
    val sb = new StringBuilder("1=1\n")
    components.foreach(component => {
      sb.append(s"and ${component.key}=${component.getPartitionValue()}\n")
    })
    sb.toString
  }


  /**
   * Method to return a simple partition string that can be used in a ResultKey
   *
   * @return The resultKey string
   */
  def toResultKeyString(): String = {
    val sb = new StringBuilder()
    sb.append("[")
    for (i <- components.indices) {
      if (i == components.length-1) {
        sb.append(s"{${components(i).key}:${components(i).value}}")
      } else {
        sb.append(s"{${components(i).key}:${components(i).value}},")
      }
      sb.append("]")
    }
    sb.toString
  }

  /**
   * Method to pick out business date from partition spec and return a string for use in the ResultKey map
   * @return
   */
  def getBusinessDateString(): String = {
    val dateKeys=List("edi_business_day","business_date")
    for (i <- components.indices) {
      if (dateKeys.contains(components(i).key)) {
        components(i).value
      }
    }
    //Return null if cannot find suitable key?
    ""
  }

  /**
   * Method to pick out business date from partition spec and return a string for use in the ResultKey map
   * @return
   */
  def getInstanceString(): String = {
   val  instanceKeys=List("src_sys_inst_id","instance")
    for (i <- components.indices) {
      if (instanceKeys.contains(components(i).key)) {
        return  components(i).value
      }
    }
    //Return null if cannot find suitable key?
    ""
  }

  /**
   * Method to get a JSON representation of the PartitionKey
   *
   * @return JSON String
   */
  def toJsonString(): String = {
    write(this)
  }

  /**
   * Method to get a pretty JSON representation of the PartitionKey
   *
   * @return
   */
  def toPrettyJsonString(): String = {
    writePretty(this)
  }

  /**
   * Method to get the PartitionSpecification which can be used when performing a df.SaveAsTable
   * to ensure the correct partitioning is applied
   *
   * @return
   */
  def getPartitionSpecification(): Seq[String] = {
    var spec: Seq[String] = Seq.empty
    components.foreach(component => {
      spec :+= component.key
    })
    spec
  }

  /**
   * Method to return the PartionSpecification into a map for use in the MetricsRepository ResultKey
   * @return Map[String, String]
   */
  def getPartitionSpecificationMap(): Map[String,String] = {
    val pMap: Map[String, String] = Map.empty
    components.foreach(component => {
      pMap+(component.key->component.value)
    })
    pMap
  }

}
////////////////////////////////////////PartitionKeyBuilder.scala//////////////////////////src\main\scala\dev\bigspark\deequ\partition\PartitionKeyBuilder.scala
package dev.bigspark.deequ.partition

/**
 * Class to help build a PartitionKey instance
 */
class PartitionKeyBuilder {
  /**
   * The sub-components of the PartitionKey
   */
  protected var components: Seq[PartitionKeyComponent] = Seq.empty

  /**
   * Add a Partition Component to the Parition Key Value
   *
   * @param component to add
   * @return
   */
  def addComponent(component: PartitionKeyComponent): this.type = {
    components :+= component
    this
  }

  /**
   * Get the PartitionKey once building is finished
   *
   * @return
   */
  def get(): PartitionKey = {
    PartitionKey(components)
  }

  /**
   * Constructor to create a copy of class during builder process
   *
   * @param partitionKeyBuilder
   */
  protected def this(partitionKeyBuilder: PartitionKeyBuilder) {
    this
    components = partitionKeyBuilder.components
  }
}
//////////////////////////PartitionKeyComponent.scala///////////////////////////////////////////////////src\main\scala\dev\bigspark\deequ\partition\PartitionKeyComponent.scala
package dev.bigspark.deequ.partition

import org.apache.spark.sql.types.DataType
import org.json4s.JsonAST.JString
import org.json4s.jackson.Serialization.{write, writePretty}
import org.json4s.{CustomSerializer, DefaultFormats}

/**
 * Object to allow automatic Serailization of org.apache.spark.sql.types.DataType members to JSON
 */
case object DataTypeSerializer extends CustomSerializer[DataType](format => ( {
  case JString(s) => DataType.fromJson(s)
}, {
  case dt: DataType => JString(dt.json)
}))

/**
 * Class to represent a component column of a partition key
 *
 * @param key      The name of the column
 * @param value    The value of the column
 * @param dataType The dataType of the column
 */
case class PartitionKeyComponent(key: String, value: String, dataType: DataType) {
  implicit val formats = DefaultFormats + DataTypeSerializer

  /**
   * Method to get a JSON representation of the PartitionKey
   *
   * @return JSON String
   */
  def toJsonString(): String = {
    write(this)
  }

  /**
   * Method to get a pretty JSON representation of the PartitionKey
   *
   * @return
   */
  def toPrettysonString(): String = {
    writePretty(this)
  }
  
//////////////////////////////PartitionSpecification.scala//////////////////////////////////////////src\main\scala\dev\bigspark\deequ\partition\PartitionSpecification.scala
package dev.bigspark.deequ.partition

/**
 * Singleton Object containing the definitions of known Partition Specifications
 */
object PartitionSpecification {

  /**
   * Partition Spec for EDH (Datalake) Source History table
   */
  val edhSourceHistoryKeys = Seq("edi_business_day")

  /**
   * Partition Spec for EDH (Datalake) EAS
   */
  val edhEasKeys = Seq("edi_business_day", "src_sys_id", "src_sys_inst_id")

  /**
   * Partition Spec for EDH (Datalake) EAS Raw tables
   */
  val edhEasRawKeys = Seq("edi_business_day", "src_sys_inst_id")

  /**
   * Partition Spec for EDH (Datalake) deequ rules
   */
  val deequRuleKeys = Seq("db_name", "tbl_name")
}

///////////////////////////DeequUtils.scala/////////////////////////////////////////src\main\scala\dev\bigspark\deequ\utils\DeequUtils.scala

package dev.bigspark.deequ.utils

import com.amazon.deequ.{VerificationResult, VerificationSuite}
import com.amazon.deequ.suggestions.{ConstraintSuggestionResult, ConstraintSuggestionRunner, Rules}
import dev.bigspark.deequ.configuration.DeequRunnerConfiguration
import dev.bigspark.deequ.exception.DeequValidationException
import dev.bigspark.deequ.partition.{PartitionKey, PartitionSpecification}
import dev.bigspark.deequ.validation.{DeequValidator, ValidationResult}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory
import com.amazon.deequ.repository.{MetricsRepository, ResultKey}
import com.amazon.deequ.repository.fs.FileSystemMetricsRepository
import org.apache.spark.sql.SparkSession

import scala.reflect.runtime.currentMirror
import scala.tools.reflect.ToolBox
import scala.util.{Success, Try}


/**
 * Object to interact with the AWS Labs deequ library in a standardised manner
 */
object DeequUtils {
  /**
   * Dynamically compiled type
   */
  type Verifier = (DataFrame,MetricsRepository,ResultKey) => VerificationResult
  type VerifierNoRepo = DataFrame => VerificationResult

  val logger = LoggerFactory.getLogger(getClass)

  /**
   * Method to get Deequ rules from a file system location
   *
   * @param spark           The SparkSession to use for persistance
   * @param baselocation    The root location of the rules on the filesystem
   * @param targetDbName    The database name of the target table on which rules will be applied
   * @param targetTableName The table name of the target table on which rules will be applied
   * @return DataFrame containing Deeque rules
   */
  def getDeequRules(spark: SparkSession, baselocation: String, targetDbName: String, targetTableName: String): DataFrame = {
    spark.read.parquet(s"$baselocation/$targetDbName/$targetTableName")
  }


  /**
   * Method to get deequ rules from a Hive DB table
   *
   * @param spark           The SparkSession to use for persistance
   * @param ruleDbName      The name of the database containing deequ rules table
   * @param ruleTableName   The name of the table containing the deequ rules
   * @param targetDbName    The database name of the target table on which rules will be applied
   * @param targetTableName The table name of the target table on which rules will be applied
   * @return DataFrame containing Deeque rules
   */
  def getDeequRules(spark: SparkSession, ruleDbName: String, ruleTableName: String, targetDbName: String, targetTableName: String): DataFrame = {
    spark.sql(s"select * from ${ruleDbName}.${ruleTableName} where lower(db_name) = '${targetDbName}' and lower(tbl_name) = '${targetTableName}'")
  }

  /**
   * Method to get the target table on which deequ will perform a verification run
   *
   * @param spark           The SparkSession to use for persistance
   * @param targetDbName    The database name of the target table on which rules will be applied
   * @param targetTableName The table name of the target table on which rules will be applied
   * @param partitionKey    The PartitionKey which will be used to filter the target table for a particular run
   * @return The DataFrame containing the table data on which the deequ rules will be applied
   */
  def getTargetTable(spark: SparkSession, targetDbName: String, targetTableName: String, partitionKey: PartitionKey): DataFrame = {
    spark.sql(s"select * from ${targetDbName}.${targetTableName}")
      .where(partitionKey.toSparkSqlWhereString())
  }

  /**
   * Method to construct a key for persisting in the MetricRepository
   * @param partitionKey  The current session partition
   * @return
   */
  def getResultKey(partitionKey: PartitionKey): ResultKey = {
    ResultKey(
      System.currentTimeMillis(),
      partitionKey.getPartitionSpecificationMap()
    )
  }

  /**
   * Method to construct path and provide MetricsRepository
   * @param spark  current session
   * @param config session configuration
   * @return
   */
  def getMetricsRepository(spark: SparkSession, config: DeequRunnerConfiguration): MetricsRepository = {
    val metricRepositoryFile = "%s/%s/metrics.json".format(config.resultFsPath,config.targetTableName)
   // logger.debug("MetricRepository location : "+metricRepositoryFile)
    FileSystemMetricsRepository(spark, metricRepositoryFile)
  }

  /**
   * Method to execute the given deequ rules agains the target dataframe
   *
   * @param rulesDf       DataFrame containing the rules for the targetDf
   * @param deequTargetDf The DataFrame on which to apply the given rules
   * @return VerificationResult instance containing all information related to the run
   */
  def executeRules(spark: SparkSession, rulesDf: DataFrame, deequTargetDf: DataFrame, config : DeequRunnerConfiguration, targetDfPartitionKey: PartitionKey): VerificationResult = {
    val metricsRepository : MetricsRepository =  getMetricsRepository(spark,config)
    val resultKey : ResultKey = getResultKey(targetDfPartitionKey)
    val verificationResult = getVerifier(rulesDf).get
    verificationResult(deequTargetDf, metricsRepository, resultKey)
  }

  /**
   * Method to execute the given deequ rules agains the target dataframe
   *
   * @param rulesDf       DataFrame containing the rules for the targetDf
   * @param deequTargetDf The DataFrame on which to apply the given rules
   * @return VerificationResult instance containing all information related to the run
   */
  def executeRulesNoRepo(spark: SparkSession, rulesDf: DataFrame, deequTargetDf: DataFrame): VerificationResult = {
    val verificationResult : VerifierNoRepo = getVerifierNoRepo(rulesDf).get
    verificationResult(deequTargetDf)
  }



  /**
   * Generic Method for getting deequ rules for the given check level into a
   * compilable format
   * @param rulesDf The DataFrame containing the deequ Constraints which should be used to verify a DataFrame
   * @param checkLevel the CheckLevel of the consraint rule
   * @return A list of the Check rules identified by the source rules rule_id
   */
  private def getRuleForVerifier(rulesDf: DataFrame, checkLevel: String): List[(String, Int)] = {
    val rules = rulesDf.select("rule_id", "rule_value").where(s"check_level = '${checkLevel}'").collect()
    rules.map(row => {
      (row.getString(1), row.getInt(0))
    }).toList
  }

  /**
   * Method to create and compile a Dynamically compiled Verifier - with metrics repository persistence
   *
   * @param rulesDf The DataFrame containing the deequ Constraints which should be used to verify a DataFrame
   * @return Custom Verifier Type which can be used to verify a DataFrame against the given rulesDf
   */
  def getVerifier(rulesDf: DataFrame): Try[Verifier] = {
    val constraintWarningCheckCodes = getRuleForVerifier(rulesDf, "Warning")
    val constraintErrorCheckCodes = getRuleForVerifier(rulesDf, "Error")

    def checkWarningSrcCode(checkCodeMethod: String, id: Int): String = s"""com.amazon.deequ.checks.Check(com.amazon.deequ.checks.CheckLevel.Warning, "$id")$checkCodeMethod"""
    def checkErrorSrcCode(checkCodeMethod: String, id: Int): String = s"""com.amazon.deequ.checks.Check(com.amazon.deequ.checks.CheckLevel.Error, "$id")$checkCodeMethod"""

    val verifierSrcCode =
      s"""{
         |import com.amazon.deequ.constraints.ConstrainableDataTypes
         |import com.amazon.deequ.{VerificationResult, VerificationSuite}
         |import com.amazon.deequ.repository.{MetricsRepository, ResultKey}
         |import org.apache.spark.sql.DataFrame
         |import com.amazon.deequ.analyzers.Size
         |
         |val warningChecks = Seq(
         |${
            constraintWarningCheckCodes.map {
              (checkWarningSrcCode _).tupled
            }.mkString(",\n  ")
          }
         |)
         |val errorChecks = Seq(
         |${
            constraintErrorCheckCodes.map {
              (checkErrorSrcCode _).tupled
            }.mkString(",\n  ")
          }
         |)
         |
         |(deequTargetDf: DataFrame, metricsRepository : MetricsRepository, resultKey: ResultKey) => VerificationSuite()
         |.onData(deequTargetDf)
         |.addChecks(warningChecks)
         |.addChecks(errorChecks)
         |.useRepository(metricsRepository)
         |.saveOrAppendResult(resultKey)
         |.run()
         |}
       """.stripMargin.trim
//      println(s"Verification function source code:\n${verifierSrcCode}")
      compile[Verifier](verifierSrcCode)
  }

  /**
   * Method to create and compile a Dynamically compiled Verifier - no metrics repository
   *
   * @param rulesDf The DataFrame containing the deequ Constraints which should be used to verify a DataFrame
   * @return Custom Verifier Type which can be used to verify a DataFrame against the given rulesDf
   */
  def getVerifierNoRepo(rulesDf: DataFrame): Try[VerifierNoRepo] = {
    val constraintWarningCheckCodes = getRuleForVerifier(rulesDf, "Warning")
    val constraintErrorCheckCodes = getRuleForVerifier(rulesDf, "Error")

    def checkWarningSrcCode(checkCodeMethod: String, id: Int): String = s"""com.amazon.deequ.checks.Check(com.amazon.deequ.checks.CheckLevel.Warning, "$id")$checkCodeMethod"""
    def checkErrorSrcCode(checkCodeMethod: String, id: Int): String = s"""com.amazon.deequ.checks.Check(com.amazon.deequ.checks.CheckLevel.Error, "$id")$checkCodeMethod"""

    val verifierSrcCode =
      s"""{
         |import com.amazon.deequ.constraints.ConstrainableDataTypes
         |import com.amazon.deequ.{VerificationResult, VerificationSuite}
         |import org.apache.spark.sql.DataFrame
         |import com.amazon.deequ.analyzers.Size
         |
         |val warningChecks = Seq(
         |${
        constraintWarningCheckCodes.map {
          (checkWarningSrcCode _).tupled
        }.mkString(",\n  ")
      }
         |)
         |val errorChecks = Seq(
         |${
        constraintErrorCheckCodes.map {
          (checkErrorSrcCode _).tupled
        }.mkString(",\n  ")
      }
         |)
         |
         |(deequTargetDf: DataFrame) => VerificationSuite()
         |.onData(deequTargetDf)
         |.addChecks(warningChecks)
         |.addChecks(errorChecks)
         |.run()
         |}
       """.stripMargin.trim

//    println(s"Verification function source code:\n${verifierSrcCode}")
    compile[VerifierNoRepo](verifierSrcCode)

  }


  /**
   * Method to compile a string into some scala code
   *
   * @param source The source code
   * @tparam T The Generic Type to compile
   * @return Compiled Object of Type T
   */
  def compile[T](source: String): Try[T] =
    Try {
      val toolbox = currentMirror.mkToolBox()
      val tree = toolbox.parse(source)
      val compiledCode = toolbox.compile(tree)
      compiledCode().asInstanceOf[T]
    }

  /**
   * Method to write Rules that will be executed by the bigspark-deequ plugin to a Table
   *
   * @param spark        The SparkSession to use for persistence
   * @param rulesDf      DataFrame containing the rules
   * @param baseLocation The base FileSystem location to which the rules will be written
   * @param dbName       The name of the database containing deequ rules table
   * @param tblName      The name of the table containing the deequ rules
   */
  def writeDeequRules(spark: SparkSession, rulesDf: DataFrame, baseLocation: String, dbName: String, tblName: String): Unit = {
    val result: ValidationResult = DeequValidator.validateDeequRulesForPersist(rulesDf)
    if (result.ok) {
      GeneralUtils.persistTable(spark, rulesDf, dbName, tblName, baseLocation, PartitionSpecification.deequRuleKeys)
    } else {
      throw new DeequValidationException(s"The rules DataFrame does not conform to expectation: ${result.message}")
    }
  }

  /**
   * Method to write the check results of a deequ execution to a Table
   *
   * @param spark         The SparkSession to use for persistence
   * @param resultsDf     DataFrame containing the fules
   * @param baseLocation  The base FileSystem location to which the rules will be written
   * @param dbName        The name of the database containing deequ rules table
   * @param tblPattern    The name of the table containing the deequ rules - "check" will be substituted into the pattern
   * @param partitionSpec The Partition Specification of the analysed table - This will follow the structure of the analysed table plus
   *                      the db_name and tbl_name of the analysed table
   */
  def writeDeequCheckResults(spark: SparkSession, resultsDf: DataFrame, baseLocation: String, dbName: String, tblPattern: String, partitionSpec: Seq[String]): Unit = {
    val result: ValidationResult = DeequValidator.validateDeequCheckResultsForPersist(resultsDf, partitionSpec)
    if (result.ok) {
      val tblName = tblPattern.format("check")
      GeneralUtils.persistTable(spark, resultsDf, dbName, tblName , baseLocation, partitionSpec)
    } else {
      throw new DeequValidationException(s"The result DataFrame does not conform to expectation: ${result.message}")
    }
  }

  /**
   * Method to write the analysis results of a deequ execution to a Table
   *
   * @param spark         The SparkSession to use for persistence
   * @param resultsDf     DataFrame containing the rules
   * @param baseLocation  The base FileSystem location to which the rules will be written
   * @param dbName        The name of the database containing deequ rules table
   * @param tblPattern    The name of the table containing the deequ rules - "analysis" will be substituted into the pattern
   * @param partitionSpec The Partition Specification of the analysed table - This will follow the structure of the analysed table
   */
  def writeDeequAnalysisResults(spark: SparkSession, resultsDf: DataFrame, baseLocation: String, dbName: String, tblPattern: String, partitionSpec: Seq[String]): Unit = {
    val result: ValidationResult = DeequValidator.validateDeequAnalysisResultsForPersist(resultsDf, partitionSpec)
    if (result.ok) {
      val tblName = tblPattern.format("analysis")
      GeneralUtils.persistTable(spark, resultsDf, dbName, tblName, baseLocation, partitionSpec)
    } else {
      throw new DeequValidationException(s"The result DataFrame does not conform to expectation: ${result.message}")
    }
  }

  /**
   * Method to Run a default set of Suggested Constraints on the given DataFrame
   *
   * @param data A DataFrame containing the data to be analysed
   * @return
   */
  def suggest(data: DataFrame): ConstraintSuggestionResult = {
    ConstraintSuggestionRunner()
      .onData(data)
      .addConstraintRules(Rules.DEFAULT)
      .run()
  }
}
//////////////////////////////////////GeneralUtils.scala//////////////src\main\scala\dev\bigspark\deequ\utils\GeneralUtils.scala
package dev.bigspark.deequ.utils

import java.util.Properties

import dev.bigspark.deequ.configuration.DeequRunnerConfiguration
import dev.bigspark.deequ.exception.DeequValidationException
import dev.bigspark.deequ.partition.PartitionKey
import dev.bigspark.deequ.validation.DeequValidator
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory


object GeneralUtils {
  val logger = LoggerFactory.getLogger(getClass)

  /**
   * Method to Persist a Dataframe
   *
   * @param spark         The SparkSession to be used for all Spark related actions
   * @param df            The DataFrame to persist
   * @param dbName        The DB Name of the table to be checked
   * @param tblName       The Table Name to be checked
   * @param path          The base FileSystem Path to which the table will be persisted
   * @param partitionSpec The Partition Specification of the target table
   */
  def persistTable(spark: SparkSession, df: DataFrame, dbName: String, tblName: String, path: String, partitionSpec: Seq[String]) {
    if (tableExists(spark, dbName, tblName)) {
      logger.info(s"Table exists, inserting into $dbName.$tblName")
      spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
      df.write.format("parquet").mode("overwrite").insertInto(s"$dbName.$tblName")
      spark.conf.set("spark.sql.sources.partitionOverwriteMode", "STATIC")
    } else {
      logger.info(s"Table does not exist, creating $dbName.$tblName at location '$path/$tblName'")
      df.write.format("parquet").mode("overwrite").option("path", s"$path/$tblName").partitionBy(partitionSpec: _*).saveAsTable(s"$dbName.$tblName")
    }
  }

  /**
   * Method to determin whether a table exists, or not
   *
   * @param spark   The SparkSession to be used for all Spark related actions
   * @param dbName  The DB Name of the table to be checked
   * @param tblName The Table Name to be checked
   * @return Boolean indicating whether the table exists, or not
   */
  def tableExists(spark: SparkSession, dbName: String, tblName: String): Boolean = {
    spark.catalog.tableExists(dbName, tblName)
  }

  def getArgumentsAsProperties(args: Array[String]): Properties = {
    val argsp: Properties = new Properties();
    args.foreach(f => {
      if (f.contains("=")) {
        val key = f.split("=")(0)
        val value = f.split("=")(1)
        if (!argsp.containsKey(key)) {
          argsp.setProperty(key, value)
        } else {
          //TODO: Warning
        }
      }
    })
    argsp
  }

  /**
   * Method to get deequ configuration from Java Properties
   *
   * @param args The Java Properties containing the arguments
   * @return
   */
  def getConfigurationFromJavaProperties(args: Properties): DeequRunnerConfiguration = {
    val argsValidation = DeequValidator.validateJavaArguments(args)
    if (argsValidation.ok) {
      DeequRunnerConfiguration(
        args.getProperty("ruleDbName")
        , args.getProperty("ruleTableName")
        , args.getProperty("targetDbName")
        , args.getProperty("targetTableName")
        , args.getProperty("resultFsPath")
        , args.getProperty("resultDbName")
        , args.getProperty("resultTablePattern")
        , args.getProperty("partitionSpecification").split(",").map(s => {
          s.trim
        })
      )
    } else {
      throw new DeequValidationException(s"Args Validation Failed: ${argsValidation.message}")
    }
  }

  /**
   * Method to add a PartitionKey columns (and values) to a Dataframe
   *
   * @param df           The DataFrame on which to add partition columns
   * @param partitionKey The PartitionKey from which the columns and values will be source
   * @return The input DataFrame with the PartitionKey columns added
   */
  def addPartitionKeyToDf(df: DataFrame, partitionKey: PartitionKey): DataFrame = {
    var outputDf = df
    partitionKey.components.foreach(component => {
      if (!outputDf.columns.contains(component.key)) {
        outputDf = outputDf.withColumn(component.key, lit(component.value))
      }
    })
    outputDf
  }


  /**
   * Method to generate the Deequ suggestions and optionally persist into a table for analysis
   * @param spark - SparkSession
   * @param tableName - database.table to analyse
   * @param persist - Persist table option '<table>_deequSuggestions'
   * @return DataFrame
   */
  def generateSuggestions(spark: SparkSession, tableName: String, persist:Boolean = false): DataFrame ={
    val tableDF = spark.table(tableName)
    import com.amazon.deequ.suggestions.{ConstraintSuggestionRunner, Rules}
    import spark.implicits._ // for toDS method
    // We ask deequ to compute constraint suggestions for us on the data
    val suggestionResult = { ConstraintSuggestionRunner()
      // data to suggest constraints for
      .onData(tableDF)
      // default set of rules for constraint suggestion
      .addConstraintRules(Rules.DEFAULT)
      // run data profiling and constraint suggestion
      .run()
    }
    // We can now investigate the constraints that Deequ suggested.
    val suggestionDataFrame = suggestionResult.constraintSuggestions.flatMap {
      case (column, suggestions) =>
        suggestions.map { constraint =>
          (column, constraint.description, constraint.codeForConstraint)
        }
    }.toSeq.toDF()

    if (persist){
      suggestionDataFrame.write.mode("overwrite").saveAsTable(tableName+"_deequSuggestions")
    }
    suggestionDataFrame;
  }
}




  /**
   * Method to get the value of the PartitionKeyComponent for use in spark.sql e.g.
   * StringType will be quoted
   *
   * @return
   */
  def getPartitionValue(): String = {
    if (dataType == org.apache.spark.sql.types.DataTypes.StringType) {
      s"'${value}'"
    } else {
      value
    }
  }
}

///////////////////DeequValidator.scala//////////////////////////////src\main\scala\dev\bigspark\deequ\validation\DeequValidator.scala

package dev.bigspark.deequ.validation

import java.util.Properties

import org.apache.spark.sql.DataFrame

/**
 * Class to allow a Boolean Validation Result and Message to be returned
 *
 * @param ok      Whether the validation was ok, or not
 * @param message Message detailing any validation failures
 */
case class ValidationResult(ok: Boolean, message: String)

/**
 * Object representing various validations used within the bigspark-deequ plugin
 */
object DeequValidator {
  /**
   * Java Property Keys required to create a deequ runner config
   */
  val requiredConfigurationArgs = Array(
    "ruleDbName"
    , "ruleTableName"
    , "targetDbName"
    , "targetTableName"
    , "resultFsPath"
    , "resultDbName"
    , "resultTablePattern"
    , "partitionSpecification")

  /**
   * Columns required for validating deequ rules for persistence - this is in addition to the rules required for a run
   */
  val requiredRuleDfColsForPersist = Array(
    "db_name"
    , "tbl_name")

  /**
   * Columns required for validating deequ rules for runs
   */
  val requiredRuleDfColsForRun = Array(
    "rule_column"
    , "rule_id"
    , "rule_value"
    , "check_level")

  /**
   * The columns required to persist the check constraint results of a deequ execution
   */
  val requiredCheckResultDfColsForPersist = Array(
    "check"
    , "check_level"
    , "check_status"
    , "constraint"
    , "constraint_status"
    , "constraint_message"
    , "db_name"
    , "tbl_name")

  /**
   * The columns required to persist the analysis results of a deequ execution
   */
  val requiredAnalysisResultDfColsForPersist = Array(
    "entity"
    , "instance"
    , "name"
    , "value"
    , "db_name"
    , "tbl_name")

  /**
   * Validate a dataframe containing rules for persistence
   *
   * @param rulesDf DataFrame containing the rules
   * @return The ValidationResult (ok, message)
   */
  def validateDeequRulesForPersist(rulesDf: DataFrame): ValidationResult = {
    var ok: Boolean = true
    val sb: StringBuilder = new StringBuilder()

    //enumerate standard columns and ensure columns are present
    requiredRuleDfColsForPersist.foreach(column => {
      if (!rulesDf.columns.contains(column)) {
        if (!ok) sb.append(", ")
        sb.append(s"${column} is missing")
        ok = false
      }
    })

    //Those that are required for run should also be present for persist
    val result: ValidationResult = validateDeequRulesForRun(rulesDf)
    if (!result.ok) {
      if (!ok) sb.append(", ")
      sb.append(result.message)
    }
    ValidationResult(ok, sb.toString())
  }


  /**
   * Method to determine where a rulesDf has the minimum number of columns to execute a verification run
   *
   * @param rulesDf DataFrame containing the rules for the targetDf
   * @return Boolean value indicating whether the rules are viable, or not
   */
  def validateDeequRulesForRun(rulesDf: DataFrame): ValidationResult = {
    var ok: Boolean = true
    val sb: StringBuilder = new StringBuilder()
    //enumerate standard columns and ensure columns are present
    requiredRuleDfColsForRun.foreach(column => {
      if (!rulesDf.columns.contains(column)) {
        if (!ok) sb.append(", ")
        sb.append(s"${column} is missing")
        ok = false
      }
    })

    ValidationResult(ok, sb.toString())
  }

  def validateDeequCheckResultsForPersist(resultsDf: DataFrame, partitionSpec: Seq[String]) : ValidationResult = {
    validateDeequResultsForPersist(resultsDf, partitionSpec, requiredCheckResultDfColsForPersist)
  }

  def validateDeequAnalysisResultsForPersist(resultsDf: DataFrame, partitionSpec: Seq[String]) : ValidationResult = {
    validateDeequResultsForPersist(resultsDf, partitionSpec, requiredAnalysisResultDfColsForPersist)
  }

  /**
   * Validate a dataframe containing deequ results for persistence
   *
   * @param resultsDf DataFrame containing the results
   * @return The ValidationResult (ok, message)
   */
  def validateDeequResultsForPersist(resultsDf: DataFrame, partitionSpec: Seq[String], expectedColumns: Seq[String]): ValidationResult = {
    var ok: Boolean = true
    val sb: StringBuilder = new StringBuilder()

    //enumerate standard columns and ensure columns are present
    expectedColumns.foreach(column => {
      if (!resultsDf.columns.contains(column)) {
        if (!ok) sb.append(", ")
        sb.append(s"${column} is missing")
        ok = false
      }
    })

    //enumerate target table partition spec and ensure columns are present
    partitionSpec.foreach(partitionKey => {
      if (!resultsDf.columns.contains(partitionKey)) {
        if (!ok) sb.append(", ")
        sb.append(s"${partitionKey} is missing")
        ok = false
      }
    })

    ValidationResult(ok, sb.toString())
  }

  /**
   * Method to validate a set of Java Properties to ensure a deequ runner config can be created from them
   *
   * @param args The Java Properties to validate
   * @return
   */
  def validateJavaArguments(args: Properties): ValidationResult = {
    var ok = true
    val sb = new StringBuilder()
    for (argName <- requiredConfigurationArgs) {
      val result = validateJavaArgument(ok, argName, args)
      if (!result.ok) {
        ok = result.ok
        sb.append(result.message)
      }
    }
    ValidationResult(ok, sb.toString())
  }

  /**
   * Method to validate a Java Property ot ensure it is present
   *
   * @param ok      Whether the existing validation is ok or not
   * @param argName The name of the arg to validate
   * @param args    the Java Properties
   * @return
   */
  def validateJavaArgument(ok: Boolean, argName: String, args: Properties): ValidationResult = {
    var isOk = ok
    val sb = new StringBuilder()
    if (!args.containsKey(argName)) {
      isOk = false
      if (!ok) sb.append(", ")
      sb.append(s"No argument for '${argName}")
    }
    ValidationResult(isOk, sb.toString())
  }

}
//////////////////////////////////////////DeequRunner.scala///////////src\main\scala\dev\bigspark\deequ\DeequRunner.scala
package dev.bigspark.deequ

import java.util.Calendar
import com.amazon.deequ.VerificationResult
import dev.bigspark.deequ.configuration.DeequRunnerConfiguration
import dev.bigspark.deequ.exception.DeequProcessingException
import dev.bigspark.deequ.partition.{PartitionKey, PartitionSpecification}
import dev.bigspark.deequ.utils.{DeequUtils, GeneralUtils}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

/**
 * Singleton Object to enacpsulate the end-to-end execution of a deequ verification from persited rules
 */
object DeequRunner {
  val logger = LoggerFactory.getLogger(getClass)

  /**
   * Method to execute a run
   * @param spark The current SparkSession
   * @param config The Configuration required to complete the run
   * @param targetDf The DataFrame on which the tules will be run
   * @param targetDfPartitionKey The target tables Partition Key - required to correctly persist the outcome of the deequ
   *                             Verication to a Table
   * @return Metrics related to the run
   */
  def execute(spark: SparkSession, config: DeequRunnerConfiguration, targetDf: DataFrame, targetDfPartitionKey: PartitionKey): DeequRunnerMetrics = {
    val metrics = new DeequRunnerMetrics
    try {
      metrics.runnerStartTime = Calendar.getInstance()
      logger.info("Getting deequ rules")
      metrics.getRulesStartTime = Calendar.getInstance()
      val rulesDf = DeequUtils.getDeequRules(spark, config.ruleDbName, config.ruleTableName, config.targetDbName, config.targetTableName)
      metrics.deequRules = rulesDf.count()
      if ( metrics.deequRules == 0) {
        throw new DeequProcessingException("No rules located in rules table: %s.%s for target table: %s.%s ".format(config.ruleDbName, config.ruleTableName, config.targetDbName, config.targetTableName))
      }
      logger.debug( metrics.deequRules + " rules returned for table: %s.%s ".format(config.targetDbName, config.targetTableName))
      metrics.getRulesEndTime = Calendar.getInstance()

      logger.info("Executing deequ rules")
      metrics.executeRulesStartTime = Calendar.getInstance()
      val result = DeequUtils.executeRules(spark, rulesDf, targetDf, config, targetDfPartitionKey)
      metrics.executeRulesEndTime = Calendar.getInstance()

      logger.info("Writing deequ analysis results")
      metrics.persistAnalysersStartTime = Calendar.getInstance()
      val analysisResultDf = GeneralUtils.addPartitionKeyToDf(
        VerificationResult.successMetricsAsDataFrame(spark, result)
          .withColumn("db_name", lit(config.targetDbName))
          .withColumn("tbl_name", lit(config.targetTableName)), targetDfPartitionKey)
      DeequUtils.writeDeequAnalysisResults(spark, analysisResultDf, config.resultFsPath, config.resultDbName, config.resultTablePattern, PartitionSpecification.deequRuleKeys ++ config.partitionSpecification)
      metrics.persistAnalysersEndTime = Calendar.getInstance()

      logger.info("Writing deequ check results")
      metrics.persistChecksStartTime = Calendar.getInstance()
      val checkResultsDf: DataFrame = GeneralUtils.addPartitionKeyToDf(
        VerificationResult.checkResultsAsDataFrame(spark, result)
          .withColumn("db_name", lit(config.targetDbName))
          .withColumn("tbl_name", lit(config.targetTableName)), targetDfPartitionKey)
      DeequUtils.writeDeequCheckResults(spark, checkResultsDf, config.resultFsPath, config.resultDbName, config.resultTablePattern, PartitionSpecification.deequRuleKeys ++ config.partitionSpecification)
      metrics.persistChecksEndTime = Calendar.getInstance()
      metrics.runnerEndTime = Calendar.getInstance()
      //Num check errors
      metrics.deequErrors = checkResultsDf.where("check_level = 'Error' and constraint_status = 'Failure'").count()
      //Num check warnings
      metrics.deequWarnings = checkResultsDf.where("check_level = 'Warning' and constraint_status = 'Failure'").count()
      //Num passed
      metrics.deequPasses = checkResultsDf.where("constraint_status = 'Success'").count()

      if (metrics.deequWarnings > 0) logger.warn("Deequ processing has generated %s warnings".format(metrics.deequWarnings))
      //Bomb process and stop publish?
      if (metrics.deequErrors > 0) logger.warn("Deequ processing has generated %s errors".format(metrics.deequErrors))
      metrics.processStatus = DeequRunnerProcessStatus.Success
      metrics
    }
    catch {
      case e: Exception => metrics.processStatusDescription = { //todo case out all exception scenarios
        logger.error("An unexpected error has occurred: "+e.getMessage)
        metrics.processStatusDescription = e.getMessage
        metrics.processStatusDescription
      }
        metrics.processStatus = DeequRunnerProcessStatus.Error
        logger.error(metrics.processStatusDescription)
        metrics
    }
  }
}

////////////////////DeequRunnerMetrics.scala/////////////src\main\scala\dev\bigspark\deequ\DeequRunnerMetrics.scala
package dev.bigspark.deequ

import java.sql.Timestamp
import java.util.Calendar

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Class to contain metrics to be populated by the DeequRunner object, and serialize to various formats
 */
case class DeequRunnerMetrics() {
  var runnerStartTime: Calendar = _
  var getRulesStartTime: Calendar = _
  var getRulesEndTime: Calendar = _
  var executeRulesStartTime: Calendar = _
  var executeRulesEndTime: Calendar = _
  var persistAnalysersStartTime: Calendar = _
  var persistAnalysersEndTime: Calendar = _
  var persistChecksStartTime: Calendar = _
  var persistChecksEndTime: Calendar = _
  var runnerEndTime: Calendar = _
  var processStatus = DeequRunnerProcessStatus.Unknown
  var processStatusDescription: String = _
  var deequRules: Long = _
  var deequErrors: Long = _
  var deequWarnings: Long = _
  var deequPasses: Long = _

  /**
   * Helper class to enable the conversion the DeequRunnerMetrics instance to a DataFrame instance
   */
  private case class DeequRunnerMetricsDf(runner_start_time: java.sql.Timestamp
                                  , get_rules_start_time: java.sql.Timestamp
                                  , get_rules_end_time: java.sql.Timestamp
                                  , execute_rules_start_time: java.sql.Timestamp
                                  , execute_rules_end_time: java.sql.Timestamp
                                  , persist_analysers_start_time: java.sql.Timestamp
                                  , persist_analysers_end_time: java.sql.Timestamp
                                  , persist_checks_start_time: java.sql.Timestamp
                                  , persist_checks_end_time: java.sql.Timestamp
                                  , runner_end_time: java.sql.Timestamp
                                  , deequ_status: String
                                  , deequ_status_message: String
                                  , deequ_errors: Long
                                  , deequ_warnings: Long
                                  , deequ_passes: Long
                                 )

  /**
   * Method to convert the current instance to a DataFrame
   * @param spark The current SparkSesson
   * @return DataFrame representaton of the DeequRunnerMetrics instance
   */
  def toDf(spark: SparkSession): DataFrame = {
    val obj = DeequRunnerMetricsDf(
       checkForNullOrReturnTimestamp(runnerStartTime)
      , checkForNullOrReturnTimestamp(getRulesStartTime)
      , checkForNullOrReturnTimestamp(getRulesEndTime)
      , checkForNullOrReturnTimestamp(executeRulesStartTime)
      , checkForNullOrReturnTimestamp(executeRulesEndTime)
      , checkForNullOrReturnTimestamp(persistAnalysersStartTime)
      , checkForNullOrReturnTimestamp(persistAnalysersEndTime)
      , checkForNullOrReturnTimestamp(persistChecksStartTime)
      , checkForNullOrReturnTimestamp(persistChecksEndTime)
      , checkForNullOrReturnTimestamp(runnerEndTime)
      , processStatus.toString
      , processStatusDescription
      , deequErrors
      , deequWarnings
      , deequPasses)

    val rdd: RDD[DeequRunnerMetricsDf] = spark.sparkContext.parallelize(Seq(obj))
    spark.createDataFrame(rdd)
  }

  /**
   * Method to cast calendar entry to Timestamp if not null
   *
   * @param value
   * @return
   */
  def checkForNullOrReturnTimestamp(value: Calendar) = {
    if (!value.eq(null)) {
      new Timestamp(value.getTime.getTime)
    } else null
  }


}



////////////////DeequRunnerProcessStatus.scala//////////////////////src\main\scala\dev\bigspark\deequ\DeequRunnerProcessStatus.scala


package dev.bigspark.deequ

/**
 * Enumeration object to represent the status of a deequ verification run
 */
object DeequRunnerProcessStatus extends Enumeration {
  type Main = Value
  val Error = Value("ERROR")
  val Warning = Value("WARNING")
  val Success = Value("SUCCESS")
  val Unknown = Value("UNKNOWN")
}
///////////////////SparkS.scala//////src\main\scala\dev\bigspark\deequ\SparkS.scala
package dev.bigspark.deequ

import org.apache.spark.sql.SparkSession

/**
 * Trait to encapsulate the creation of a spark session on Yarn - Use at your peril
 * as ideally the caller should pass the spark session
 */
trait SparkS {

  /**
   * The Spark Session
   */
  val spark: SparkSession = SparkSession.builder.master("yarn").enableHiveSupport().getOrCreate()

}

///////////////////TransformerDeequRunner.scala////////////////////////////src\main\scala\dev\bigspark\deequ\TransformerDeequRunner.scala
package dev.bigspark.deequ

import java.sql.Timestamp
import java.util.Calendar

import dev.bigspark.deequ.configuration.DeequRunnerConfiguration
import dev.bigspark.deequ.partition.{PartitionKey, PartitionKeyBuilder, PartitionKeyComponent, PartitionSpecification}
import org.apache.spark
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalog.Database
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

object TransformerDeequRunner {
  val logger: Logger = LoggerFactory.getLogger(getClass)
  var metrics = new DeequRunnerMetrics

  def getRunConfig ( spark : SparkSession
                       , ruleDbName: String
                       , ruleTableName: String
                       , targetDbName: String
                       , targetTableName: String ) : DeequRunnerConfiguration = {

    val database :Database = spark.catalog.getDatabase(targetDbName);

    DeequRunnerConfiguration(
      ruleDbName
      , ruleTableName
      , targetDbName
      , targetTableName
      , database.locationUri
      , targetDbName
      , "edh_control_dq_%s_results"
      , PartitionSpecification.edhEasRawKeys.mkString(",").split(",").map(s => {
        s.trim
      })
    )

  }

  def getPartitionKeyForRawTable(businessDate : String, instanceID : String): PartitionKey = {
    new PartitionKeyBuilder()
      .addComponent(PartitionKeyComponent("edi_business_day", businessDate, StringType))
      .addComponent(PartitionKeyComponent("src_sys_inst_id", instanceID, StringType))
      .get()
  }

  /**
   * Method to cast calendar entry to Timestamp if not null
   * @param value
   * @return
   */
  def checkForNullOrReturnTimestamp(value : Calendar)={
    if (!value.eq(null)){
      new Timestamp(value.getTime.getTime)
    } else null
  }

  case class _DeequRunnerMetrics(database_name: String
                                 , table_name: String
                                 , instance: String
                                 , business_date: String
                                 , runner_start_time: java.sql.Timestamp
                                 , get_rules_start_time: java.sql.Timestamp
                                 , get_rules_end_time: java.sql.Timestamp
                                 , execute_rules_start_time: java.sql.Timestamp
                                 , execute_rules_end_time: java.sql.Timestamp
                                 , persist_analysers_start_time: java.sql.Timestamp
                                 , persist_analysers_end_time: java.sql.Timestamp
                                 , persist_checks_start_time: java.sql.Timestamp
                                 , persist_checks_end_time: java.sql.Timestamp
                                 , runner_end_time: java.sql.Timestamp
                                 , deequ_status: String
                                 , deequ_status_message: String
                                 , deequ_rules: Long
                                 , deequ_errors: Long
                                 , deequ_warnings: Long
                                 , deequ_passes: Long
                                )
  /**
   * runDeequ()
   * Main entry point for StreamSets transformer to execute deequ processing and persist outputs
   *
   * @param spark : SparkSession
   * @param sourceDF : The DataFrame to operate the rules on
   * @param businessDate : Business date of the run
   * @param instanceID : Source instance
   * @param targetDbName : Target database
   * @param targetTableName : Target table
   *
   * @return Metrics Dataframe
   */
  def runDeequ(spark : SparkSession
               , sourceDF : DataFrame
               , businessDate: String
               , instanceID : String
               , targetDbName: String
               , targetTableName: String
              ): DataFrame = {
    val partitionKey: PartitionKey = getPartitionKeyForRawTable(businessDate, instanceID)
    val runConfig = getRunConfig(spark, targetDbName, "edh_control_dq_rules", targetDbName, targetTableName)
//    if (spark.catalog.tableExists(runConfig.targetDbName+"."+runConfig.targetTableName)) {
//      logger.info("Performing schema check")
//      //Remove partition cols for schema check
//      val sourceSchemaNoAuditCols = sourceDF.drop("SRC_SYS_INST_ID")
//      val sourceSchema = sourceSchemaNoAuditCols.schema
//      val targetDF = spark.table(runConfig.targetDbName + "." + runConfig.targetTableName)
//      //Remove partition cols for schema check
//      val targetDFNoAuditCols = targetDF.drop("edi_business_day", "src_sys_inst_id")
//      val targetSchema = targetDFNoAuditCols.schema
//      if (!(sourceSchema equals targetSchema)) {
//        //Stop the table publish with an unhandled exception
//        //      metrics.processStatus = DeequRunnerProcessStatus.Error
//        //      metrics.processStatusDescription="Ingested schema does not match schema of target table. [Source schema: %s] [Target schema: %s]".format(sourceSchema.fieldNames.toString, targetSchema.fieldNames.toString)
//        throw new DeequProcessingException("Ingested schema does not match schema of target table. [Source schema: %s] [Target schema: %s]"
//          .format(
//            sourceSchema.fieldNames.foreach(a => print(a + ","))
//            , targetSchema.fieldNames.foreach(a => print(a + ","))
//          )
//        )
//      }
//    }

    //Transfromer Scala step version
//    //Check if source schema matches target, if not, stop pipeline via Exception
//    if (spark.catalog.tableExists(databaseName+"."+tableName)) {
//      logger.info("Performing schema check")
//      //Remove partition cols for schema check
//      val sourceSchemaNoAuditCols = inputs(0).drop("SRC_SYS_INST_ID")
//      val sourceSchema = sourceSchemaNoAuditCols.schema
//      val targetDF = spark.table(databaseName+"."+tableName)
//      //Remove partition cols for schema check
//      val targetDFNoAuditCols = targetDF.drop("edi_business_day", "src_sys_inst_id")
//      val targetSchema = targetDFNoAuditCols.schema
//      if (!(sourceSchema equals targetSchema)) {
//        throw new Exception("Ingested schema does not match schema of target table. [Source schema: %s] [Target schema: %s]"
//          .format(
//            sourceSchema.fieldNames.foreach(a => print(a + ","))
//            , targetSchema.fieldNames.foreach(a => print(a + ","))
//          )
//        )
//      }
//    }



    //      If schema check passes, run rules
      metrics = DeequRunner.execute(spark, runConfig, sourceDF, partitionKey)

    /**
     * Method to construct encodable object for Spark and return a DataFrame
     * @return
     */
    def metricsDf(): DataFrame = {
      val obj = _DeequRunnerMetrics (
        targetDbName.toLowerCase
        , targetTableName.toLowerCase
        , instanceID
        , businessDate
        , checkForNullOrReturnTimestamp(metrics.runnerStartTime)
        , checkForNullOrReturnTimestamp(metrics.getRulesStartTime)
        , checkForNullOrReturnTimestamp(metrics.getRulesEndTime)
        , checkForNullOrReturnTimestamp(metrics.executeRulesStartTime)
        , checkForNullOrReturnTimestamp(metrics.executeRulesEndTime)
        , checkForNullOrReturnTimestamp(metrics.persistAnalysersStartTime)
        , checkForNullOrReturnTimestamp(metrics.persistAnalysersEndTime)
        , checkForNullOrReturnTimestamp(metrics.persistChecksStartTime)
        , checkForNullOrReturnTimestamp(metrics.persistChecksEndTime)
        , checkForNullOrReturnTimestamp(metrics.runnerEndTime)
        , metrics.processStatus.toString
        , metrics.processStatusDescription
        , metrics.deequRules
        , metrics.deequErrors
        , metrics.deequWarnings
        , metrics.deequPasses)

      val rdd : RDD[_DeequRunnerMetrics] = spark.sparkContext.parallelize(Seq(obj))
      spark.createDataFrame(rdd)
    }
    metricsDf()
  }
}


///////////////////////UIDeequRunner.scala/////////////////////////////////src\main\scala\dev\bigspark\deequ\UIDeequRunner.scala
package dev.bigspark.deequ

import com.amazon.deequ.VerificationResult
import dev.bigspark.deequ.utils.{DeequUtils, GeneralUtils}

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

/**
 * Singleton Object to enacpsulate the end-to-end execution of a deequ verification from persited rules
 */
object UIDeequRunner {
  val logger = LoggerFactory.getLogger(getClass)
  def execute(spark: SparkSession,rulesDf : DataFrame, targetDf: DataFrame): (DataFrame,DataFrame) = {
      val result = DeequUtils.executeRulesNoRepo(spark, rulesDf, targetDf)
      val metricsDF = VerificationResult.successMetricsAsDataFrame(spark, result)
      val checkResultsDF = VerificationResult.checkResultsAsDataFrame(spark, result)
     (metricsDF,checkResultsDF)
  }
}

/////////////////////////////////////#################TEST##########################//////////////////////////////////


///////////////////////ADB_NACLOAN_FULL//////////////////////////////////////////////////////////src\test\resources\ADB_NACLOAN_FULL

    source-parameters:
  delimiter: 001
  quotechar: 033
  attributes:
    -
      name: BRANCH_NO
      type: DOUBLE
      nullable: false
    -
      name: ACCOUNT_NO
      type: DOUBLE
      nullable: false
    -
      name: LOAN_REPAYMT_DETS_DATE
      type: DOUBLE
      nullable: false
    -
      name: HO_BORR_LIM_RDCTN_NEXT_DATE
      type: DOUBLE
      nullable: false
    -
      name: HO_BORR_LIM_RDCTN_PREV_DATE
      type: DOUBLE
      nullable: false
    -
      name: LOAN_REPAYMT_HOLIDAY_PER
      type: DOUBLE
      nullable: false
    -
      name: LOAN_REPAYMT_PER
      type: DOUBLE
      nullable: false
    -
      name: LOAN_REPAYMT_OS_CNT
      type: DOUBLE
      nullable: false
    -
      name: LOAN_REPAYMT_AMT_1
      type: DOUBLE
      nullable: false
    -
      name: LOAN_REPAYMT_DURATION
      type: DOUBLE
      nullable: false
    -
      name: LOAN_TERM_CODE
      type: STRING
      nullable: false
    -
      name: LOAN_EXP_DATE
      type: DOUBLE
      nullable: false
    -
      name: ORIGINAL_PREMIUM_AMOUNT
      type: DOUBLE
      nullable: false
    -
      name: PREMIUM_REBATE_AMOUNT
      type: DOUBLE
      nullable: false
    -
      name: ORIGINAL_LOAN_AMOUNT
      type: DOUBLE
      nullable: false
    -
      name: REPAYMENT_AMOUNT
      type: DOUBLE
      nullable: false
    -
      name: INSURANCE_SCHEME_ID
      type: DOUBLE
      nullable: false
    -
      name: INSURED_TERM
      type: DOUBLE
      nullable: false
    -
      name: ACCOUNT_OPEN_DATE
      type: DOUBLE
      nullable: false
    -
      name: CRT_DT
      type: STRING
      nullable: false
    -
      name: EDI_FD_ID
      type: DOUBLE
      nullable: false
    -
      name: EDI_FD_RUN_ID
      type: DOUBLE
      nullable: false
    -
      name: DATASET_ID
      type: DOUBLE
      nullable: false
    -
      name: SRC_SYS_ID
      type: STRING
      nullable: false
    -
      name: SRC_SYS_INST_ID
      type: STRING
      nullable: false
    -
      name: RECORD_ID
      type: DOUBLE
      nullable: false
	  
////////////////////////log4j.properties///////////////	src\test\resources\log4j.properties
log4j.rootLogger=ERROR, stdout
log4j.logger.dev.bigspark=TRACE
log4j.logger.org=ERROR
log4j.appender.stdout=org.apache.log4j.ConsoleAppender
#log4j.appender.stdout.layout=org.apache.log4j.PatternLayout
#og4j.appender.stdout.layout.ConversionPattern=%d [%t] %-5p %c - %m%n
log4j.appender.stdout.layout=org.apache.log4j.EnhancedPatternLayout
log4j.appender.stdout.layout.ConversionPattern=%d [%t] %-5p %c{1.} [%t]: %m%n



///////////////////////DeequLab.scala/////////////////////////////src\test\scala\dev\bigspark\deequ\test\manual\DeequLab.scala

package dev.bigspark.deequ.test.manual

import java.util.Calendar

import dev.bigspark.deequ.DeequRunnerMetrics
import dev.bigspark.deequ.test.SparkS
import dev.bigspark.deequ.test.utils.TestUtils
import dev.bigspark.deequ.utils.GeneralUtils

object DeequLab extends SparkS {
  val props = TestUtils.getJavaProperties()
  val config = GeneralUtils.getConfigurationFromJavaProperties(props)

  def main(args: Array[String]): Unit = {
    val metrics = new DeequRunnerMetrics
    metrics.runnerStartTime = Calendar.getInstance()
    metrics.deequPasses = 0
    metrics.deequWarnings = -1
    metrics.toDf(spark).show()
  }
}

////////////////////
package dev.bigspark.deequ.test.manual

import com.amazon.deequ.VerificationResult
import dev.bigspark.deequ.DeequRunner
import dev.bigspark.deequ.test.SparkS
import dev.bigspark.deequ.test.utils.TestUtils
import dev.bigspark.deequ.utils.{DeequUtils, GeneralUtils}
import org.slf4j.LoggerFactory

object DeequRunnerExerciser extends SparkS with App {
  val logger = LoggerFactory.getLogger(getClass)
  logger.info("Beginning Test")
  val props = TestUtils.getJavaProperties()
  val config = GeneralUtils.getConfigurationFromJavaProperties(props)
  logger.info("Clearing Down")
  TestUtils.prepareTestDataStore(config)
  logger.info("Persisting Data")
  TestUtils.createAndPersistTestData(config)

  //get partitionKey
  val partitionKey = TestUtils.getPartitionKeyForTestTable()

  logger.info("Getting Data")
  val dataDf = DeequUtils.getTargetTable(spark, config.targetDbName, config.targetTableName, partitionKey)

  if (logger.isDebugEnabled()) {
    logger.debug("Data DataFrame:")
    dataDf.show()
  }
  DeequRunner.execute(spark, config, dataDf, partitionKey)
}

//////////////////////DeequRunnerExerciser.scala////////////src\test\scala\dev\bigspark\deequ\test\manual\DeequRunnerExerciser.scala
package dev.bigspark.deequ.test.manual

import com.amazon.deequ.VerificationResult
import dev.bigspark.deequ.DeequRunner
import dev.bigspark.deequ.test.SparkS
import dev.bigspark.deequ.test.utils.TestUtils
import dev.bigspark.deequ.utils.{DeequUtils, GeneralUtils}
import org.slf4j.LoggerFactory

object DeequRunnerExerciser extends SparkS with App {
  val logger = LoggerFactory.getLogger(getClass)
  logger.info("Beginning Test")
  val props = TestUtils.getJavaProperties()
  val config = GeneralUtils.getConfigurationFromJavaProperties(props)
  logger.info("Clearing Down")
  TestUtils.prepareTestDataStore(config)
  logger.info("Persisting Data")
  TestUtils.createAndPersistTestData(config)

  //get partitionKey
  val partitionKey = TestUtils.getPartitionKeyForTestTable()

  logger.info("Getting Data")
  val dataDf = DeequUtils.getTargetTable(spark, config.targetDbName, config.targetTableName, partitionKey)

  if (logger.isDebugEnabled()) {
    logger.debug("Data DataFrame:")
    dataDf.show()
  }
  DeequRunner.execute(spark, config, dataDf, partitionKey)
}
//////////////////////TestUtils.scala///////////////////////	src\test\scala\dev\bigspark\deequ\test\utils\TestUtils.scala
package dev.bigspark.deequ.test.utils

import java.util.Properties

import dev.bigspark.deequ.configuration.DeequRunnerConfiguration
import dev.bigspark.deequ.partition.{PartitionKey, PartitionKeyBuilder, PartitionKeyComponent, PartitionSpecification}
import dev.bigspark.deequ.test.SparkS
import dev.bigspark.deequ.utils.GeneralUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.StringType
import org.slf4j.LoggerFactory

/**
 * Lots of friendly helper methods in here for you
 */
object TestUtils extends SparkS {
  val logger = LoggerFactory.getLogger(getClass)

  def createDb(dbName : String) : Unit = {
    if (!spark.catalog.databaseExists(dbName)) {
      spark.sql(s"create database ${dbName}")
    }
  }

/////////////////////BaseSpec.scala//////////////////////src\test\scala\dev\bigspark\deequ\test\BaseSpec.scala
package dev.bigspark.deequ.test
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec

abstract class BaseSpec extends AnyFlatSpec with TestData

//////////////////SparkS.scala///////////////////////////src\test\scala\dev\bigspark\deequ\test\SparkS.scala
package dev.bigspark.deequ.test

import org.apache.spark.sql.SparkSession

trait SparkS {

  val spark: SparkSession = SparkSession.builder.master("local[2]").enableHiveSupport().getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

}

///////////////TestData.scala//////////////////////////rc\test\scala\dev\bigspark\deequ\test\TestData.scala

package dev.bigspark.deequ.test

import dev.bigspark.deequ.test.utils.TestUtils

trait TestData extends SparkS {
  val rulesDf = TestUtils.getTestRules()
  val dataDf =  TestUtils.getTestData()
}

////////////////////ValidationSpec.scala//////////////////////////src\test\scala\dev\bigspark\deequ\test\ValidationSpec.scala
import dev.bigspark.deequ.configuration.DeequRunnerConfiguration
import dev.bigspark.deequ.exception.DeequValidationException
import dev.bigspark.deequ.test.utils.TestUtils
import dev.bigspark.deequ.test.{BaseSpec, SparkS}
import dev.bigspark.deequ.utils.{DeequUtils, GeneralUtils}
import dev.bigspark.deequ.validation.DeequValidator
import org.apache.spark.sql.DataFrame
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers.{be, noException}


class ValidationSpec extends BaseSpec with BeforeAndAfter with SparkS {

  val testDbName = "automated_test_db"
  val testRuleTableName = "test_rule_table"
  var rulesDfBroken: DataFrame = _
  var config: DeequRunnerConfiguration = _

   before {
     config = GeneralUtils.getConfigurationFromJavaProperties(TestUtils.getJavaProperties())
     spark.sql(s"drop table if exists ${testDbName}.${testRuleTableName}")
    // spark.sql(s"create database if not exists ${testDbName}")
   }

    "A deequ rules DataFrame" should "should have a specific set of columns for a run " in {
      val result = DeequValidator.validateDeequRulesForRun(rulesDf)
      assert(result.ok)
    }

  it should "throw DeequValidationException if we try to persist without the correct columns" in {
    assertThrows[DeequValidationException] {
      DeequUtils.writeDeequRules(spark
        , rulesDf
        , s"${config.resultFsPath}"
        , "test_db"
        , "test_table"
      )
    }
  }

  //it should "not throw DeequValidationException if we try to persist with the correct columns" in {
//  noException should be thrownBy {
//      DeequUtils.writeDeequRules(spark
//        , rulesDf.withColumn("db_name",lit("test_db")).withColumn("tbl_name",lit("test_table"))
//        ,s"${config.resultFsPath}"
//        , "test_db"
//        , "test_table"
//      )
//    }



}

  def getTmpDir(): String = {
    System.getProperty("java.io.tmpdir")
  }

  def getJavaProperties(): Properties = {
    val props = new Properties()
    props.setProperty("ruleDbName", "test_db")
    props.setProperty("ruleTableName", "deequ_rules")
    props.setProperty("targetDbName", "test_db")
    props.setProperty("targetTableName", "deequ_data")
    props.setProperty("resultFsPath", s"${TestUtils.getHiveTableLocation()}")
    props.setProperty("resultDbName", "test_db")
    props.setProperty("resultTablePattern", "deequ_%s_results")
    props.setProperty("partitionSpecification", PartitionSpecification.edhEasKeys.mkString(","))
    props
  }

  def prepareTestDataStore(config: DeequRunnerConfiguration): Unit = {
    //Tidy Up file System
    val targetPath = s"${TestUtils.getHiveTableLocation()}"
    val targetDfsPath = new Path(targetPath)
    val dfs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    if (dfs.exists(targetDfsPath)) dfs.delete(targetDfsPath, true)

    //Tidy Up DB
    spark.sql("drop  table if exists  test_db.test_table")
    spark.sql("drop  table if exists  test_db.deequ_rules")
    spark.sql("drop  table if exists  test_db.deequ_data")
    spark.sql("drop  table if exists  test_db.deequ_check_results")
    spark.sql("drop  table if exists  test_db.deequ_analysis_results")

    spark.sql("drop database if exists test_db")
    spark.sql("create database  test_db")
  }

  def getHiveTableLocation(): String = {
    s"${getHomeArea()}/test/hive/warehouse"
  }

  def getHomeArea(): String = {
    System.getProperty("user.home")
  }

  def createAndPersistTestData(config: DeequRunnerConfiguration) = {
    //Get DataFrames
    val rulesDf = TestUtils.getTestRules()
      .withColumn("db_name", lit(s"${config.targetDbName}"))
      .withColumn("tbl_name", lit(s"${config.targetTableName}"))
    val dataDf = TestUtils.getTestData()
      .withColumn("edi_business_day", lit("2020-01-01"))
      .withColumn("src_sys_id", lit("TST"))
      .withColumn("src_sys_inst_id", lit("ALL"))

    GeneralUtils.persistTable(spark
      , rulesDf
      , config.ruleDbName
      , config.ruleTableName
      , config.resultFsPath
      , PartitionSpecification.deequRuleKeys)

    GeneralUtils.persistTable(spark
      , dataDf
      , config.targetDbName
      , config.targetTableName
      , config.resultFsPath
      , PartitionSpecification.edhEasKeys)
  }

    def getTestRules(): DataFrame = {
    val rows = spark.sparkContext.parallelize(Seq(
      TestRule("productName", 1, ".isComplete(\"productName\")", "Error"),
      TestRule("productName", 2, ".isComplete(\"valuable\")", "Warning")
    ))
    val rawData = spark.createDataFrame(rows)
    rawData
  }

  def getTestData(): DataFrame = {
    val rows = spark.sparkContext.parallelize(Seq(
      TestData("thingA", "13.0", "IN_TRANSIT", "true"),
      TestData("thingA", "5", "DELAYED", "false"),
      TestData("thingB", null, "DELAYED", null),
      TestData("thingC", null, "IN_TRANSIT", "false"),
      TestData("thingD", "1.0", "DELAYED", "true"),
      TestData("thingC", "7.0", "UNKNOWN", null),
      TestData("thingC", "20", "UNKNOWN", null),
      TestData("thingE", "20", "DELAYED", "false")
    ))

    val rawData = spark.createDataFrame(rows)
    rawData

  }

  def getPartitionKeyForTestTable(): PartitionKey = {
    new PartitionKeyBuilder()
      .addComponent(new PartitionKeyComponent("edi_business_day", s"2020-01-01", StringType))
      .addComponent(new PartitionKeyComponent("src_sys_id", "TST", StringType))
      .addComponent(new PartitionKeyComponent("src_sys_inst_id", "ALL", StringType))
      .get()
  }

  case class TestRule(rule_column: String, rule_id: Int, rule_value: String, check_level: String)

  case class TestData(productName: String, totalNumber: String, status: String, valuable: String)

}

/////////VerificationSpec.scala///////////////////src\test\scala\dev\bigspark\deequ\test\VerificationSpec.scala

package dev.bigspark.deequ.test

import dev.bigspark.deequ.configuration.DeequRunnerConfiguration
import dev.bigspark.deequ.utils.{DeequUtils, GeneralUtils}
import org.scalatest.BeforeAndAfter
import java.lang.IllegalArgumentException

import dev.bigspark.deequ.test.utils.TestUtils
import dev.bigspark.deequ.test.utils.TestUtils.getPartitionKeyForTestTable

class VerificationSpec extends BaseSpec with BeforeAndAfter with SparkS {

  val testDbName = "automated_test_db"
  val testRuleTableName = "test_rule_table"
  var config: DeequRunnerConfiguration = _

  before {
    config = GeneralUtils.getConfigurationFromJavaProperties(TestUtils.getJavaProperties())
    spark.sql(s"drop table if exists ${testDbName}.${testRuleTableName}")
    TestUtils.createDb(testDbName)
    //spark.sql(s"create database if not exists ${testDbName}")
  }


  "A deequ Verifier" should "should successfully be compiled from a rules DataFrame" in {
    DeequUtils.getVerifier(rulesDf)
  }

  it should "successfully execute all checks and Analyzers" in {
    DeequUtils.executeRules(spark, rulesDf, dataDf, config, getPartitionKeyForTestTable())
  }

  it should "throw an InvalidArgumentException if there are duplicate Analyzers" in {
    assertThrows[IllegalArgumentException] {
      DeequUtils.executeRules(spark, rulesDf.union(rulesDf), dataDf, config, getPartitionKeyForTestTable())
    }
  }
}
///////////////////////////////////////gitnore/
target/
.idea/
.directory/
*.iml
.attach_*
derby.log
metastore_db



////////////////////deequ-plugin-develop@c0a3280c003\pom.xml/////////////////scala\scala_test\deequ-plugin-develop@c0a3280c003\pom.xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <groupId>dev.bigspark</groupId>
    <artifactId>deequ-plugin</artifactId>
    <version>1.1-SNAPSHOT</version>
    <packaging>jar</packaging>
    <name>bigspark deequ Plugin</name>
    <description>bigspark concrete implementation of AWS Labs deequ</description>
    <url>https://github.com/itsbigspark/deequ-plugin</url>

    <licenses>
        <license>
            <name>Apache License, Version 2.0</name>
            <url>http://www.apache.org/licenses/LICENSE-2.0/</url>
            <distribution>repo</distribution>
        </license>
    </licenses>

    <developers>
        <developer>
            <id>zbacjxx</id>
            <name>Richard Hay</name>
            <url>https://github.com/zbacjxx</url>
        </developer>
    </developers>

    <scm>
        <url>https://github.com/itsbigspark/deequ-plugin</url>
    </scm>


    <properties>
        <maven.compiler.source>1.8</maven.compiler.source>
        <maven.compiler.target>1.8</maven.compiler.target>
        <encoding>UTF-8</encoding>

        <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
        <typesafe.config.version>1.2.1</typesafe.config.version>

        <scala.major.version>2.11</scala.major.version>
        <scala.version>${scala.major.version}.12</scala.version>
        <java.major.version>1.8</java.major.version>
        <spark.version>2.4.6</spark.version>


        <maven.project.info.reports.plugin.version>2.8</maven.project.info.reports.plugin.version>
        <dependency.locations.enabled>false</dependency.locations.enabled>

        <scalatest.version>3.2.1</scalatest.version>
        <scalamock.version>4.1.0</scalamock.version>
        <junit.version>5.6.2</junit.version>

        <maven.scala.plugin.version>3.3.2</maven.scala.plugin.version>
        <maven.assembly.plugin.version>3.1.0</maven.assembly.plugin.version>
        <maven.compiler.plugin>3.8.1</maven.compiler.plugin>
        <maven.surefire.report.plugin.version>2.21.0</maven.surefire.report.plugin.version>
        <maven.scalatest.plugin.version>2.0.0</maven.scalatest.plugin.version>
        <maven.jar.plugin.version>3.0.0</maven.jar.plugin.version>
    </properties>

    <dependencies>
        <dependency>
            <groupId>org.scala-lang</groupId>
            <artifactId>scala-library</artifactId>
            <version>${scala.version}</version>
            <scope>compile</scope>
        </dependency>
        <dependency>
            <groupId>org.scala-lang</groupId>
            <artifactId>scala-compiler</artifactId>
            <version>${scala.version}</version>
        </dependency>
        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-core_2.11</artifactId>
            <version>${spark.version}</version>
            <scope>provided</scope>
            <exclusions>
                <exclusion>
                    <artifactId>scala-xml_2.11</artifactId>
                    <groupId>org.scala-lang.modules</groupId>
                </exclusion>
            </exclusions>
        </dependency>
        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-sql_2.11</artifactId>
            <version>${spark.version}</version>
            <scope>provided</scope>
        </dependency>
        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-yarn_2.11</artifactId>
            <version>${spark.version}</version>
            <scope>provided</scope>
        </dependency>
        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-hive_${scala.major.version}</artifactId>
            <version>${spark.version}</version>
            <scope>provided</scope>
        </dependency>

        <!-- https://mvnrepository.com/artifact/com.amazon.deequ/deequ -->
        <dependency>
            <groupId>com.amazon.deequ</groupId>
            <artifactId>deequ</artifactId>
            <version>1.0.4</version>
        </dependency>

        <!-- https://mvnrepository.com/artifact/org.slf4j/slf4j-api -->
        <dependency>
            <groupId>org.slf4j</groupId>
            <artifactId>slf4j-api</artifactId>
            <version>1.7.30</version>
            <scope>provided</scope>
        </dependency>

        <!-- https://mvnrepository.com/artifact/org.slf4j/slf4j-log4j12 -->
        <dependency>
            <groupId>org.slf4j</groupId>
            <artifactId>slf4j-log4j12</artifactId>
            <version>1.7.30</version>
            <scope>provided</scope>
        </dependency>

        <dependency>
            <groupId>org.scalatest</groupId>
            <artifactId>scalatest_${scala.major.version}</artifactId>
            <version>${scalatest.version}</version>
            <scope>test</scope>
        </dependency>

      <!--  <dependency>
            <groupId>org.scalamock</groupId>
            <artifactId>scalamock_${scala.major.version}</artifactId>
            <version>${scalamock.version}</version>
            <scope>test</scope>
        </dependency>-->
    </dependencies>

    <profiles>
        <profile>
            <id>test</id>
             <build>
                <plugins>
                    <plugin>
                        <groupId>net.alchim31.maven</groupId>
                        <artifactId>scala-maven-plugin</artifactId>
                        <version>${maven.scala.plugin.version}</version>
                        <executions>
                            <execution>
                                <id>scala-compile-first</id>
                                <phase>process-resources</phase>
                                <goals>
                                    <goal>add-source</goal>
                                    <goal>compile</goal>
                                </goals>
                            </execution>
                            <execution>
                                <id>scala-test-compile</id>
                                <phase>process-test-resources</phase>
                                <goals>
                                    <goal>testCompile</goal>
                                </goals>
                            </execution>
                        </executions>
                    </plugin>
                    <!-- disable surefire -->
                    <plugin>
                        <groupId>org.apache.maven.plugins</groupId>
                        <artifactId>maven-surefire-plugin</artifactId>
                        <version>3.0.0-M5</version>
                        <configuration>
                            <skipTests>true</skipTests>
                        </configuration>
                    </plugin>
                    <!-- enable scalatest -->
                    <plugin>
                        <groupId>org.scalatest</groupId>
                        <artifactId>scalatest-maven-plugin</artifactId>
                        <version>${maven.scalatest.plugin.version}</version>
                        <executions>
                            <execution>
                                <id>test</id>
                                <goals>
                                    <goal>test</goal>
                                </goals>
                            </execution>
                        </executions>
                        <configuration>
                            <reportsDirectory>${project.build.directory}/surefire-reports</reportsDirectory>
                            <junitxml>.</junitxml>
                            <filereports>WDF TestSuite.txt</filereports>
                        </configuration>
                    </plugin>
                </plugins>
            </build>
        </profile>
        <profile>
            <id>standard</id>
            <activation>
                <activeByDefault>true</activeByDefault>
            </activation>
            <build>
            <!--    <sourceDirectory>src/main/scala</sourceDirectory-->
                <plugins>
                    <plugin>
                        <groupId>net.alchim31.maven</groupId>
                        <artifactId>scala-maven-plugin</artifactId>
                        <version>${maven.scala.plugin.version}</version>
                        <executions>
                            <execution>
                                <id>scala-compile-first</id>
                                <phase>process-resources</phase>
                                <goals>
                                    <goal>add-source</goal>
                                    <goal>compile</goal>
                                </goals>
                            </execution>
                            <execution>
                                <id>scala-test-compile</id>
                                <phase>process-test-resources</phase>
                                <goals>
                                    <goal>testCompile</goal>
                                </goals>
                            </execution>
                        </executions>
                    </plugin>
                    <plugin>
                        <groupId>org.apache.maven.plugins</groupId>
                        <artifactId>maven-compiler-plugin</artifactId>
                        <version>${maven.compiler.plugin}</version>
                        <executions>
                            <execution>
                                <phase>compile</phase>
                                <goals>
                                    <goal>compile</goal>
                                </goals>
                            </execution>
                        </executions>
                        <configuration>
                            <source>${java.major.version}</source>
                            <target>${java.major.version}</target>
                        </configuration>
                    </plugin>
                    <plugin>
                        <groupId>org.apache.maven.plugins</groupId>
                        <artifactId>maven-jar-plugin</artifactId>
                        <version>${maven.jar.plugin.version}</version>
                        <configuration>
                            <archive>
                                <manifest>
                                    <addDefaultImplementationEntries>true</addDefaultImplementationEntries>
                                </manifest>
                            </archive>
                        </configuration>
                    </plugin>
                    <!--<plugin>
                        <groupId>org.apache.maven.plugins</groupId>
                        <artifactId>maven-assembly-plugin</artifactId>
                        <version>${maven.assembly.plugin.version}</version>
                        <configuration>
                            <descriptorRefs>
                                <descriptorRef>jar-with-dependencies</descriptorRef>
                            </descriptorRefs>
                        </configuration>
                        <executions>
                            <execution>
                                <phase>compile</phase>
                                <goals>
                                    <goal>single</goal>
                                </goals>
                            </execution>
                        </executions>
                    </plugin>-->
                </plugins>
            </build>
        </profile>
    </profiles>


</project>



