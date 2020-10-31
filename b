package dev.bigspark.caster

import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.ArrayList
import java.util.List
import EDIFileSchema._
import scala.beans.{BeanProperty, BooleanBeanProperty}

object EDIFileSchema {

  private var logger: Logger = LoggerFactory.getLogger(classOf[EDIFileSchema])

  class SchemaAttributes(@BeanProperty var name: String,
                         @BeanProperty var `type`: String,
                         @BeanProperty var nullable: java.lang.Boolean) {

  }

  def getDataTypeForSchema(colName: String, typeString: String): DataType = {
    var returnDataType: DataType = DataTypes.StringType
    typeString.toUpperCase() match {
      case "TINYINT" =>
        auditConversion(colName, typeString, "DataTypes.ByteType")
        returnDataType = DataTypes.ByteType
      case "SMALLINT" =>
        auditConversion(colName, typeString, "DataTypes.ShortType")
        returnDataType = DataTypes.ShortType
      case "INTEGER" =>
        auditConversion(colName, typeString, "DataTypes.IntegerType")
        returnDataType = DataTypes.IntegerType
      case "BIGINT" =>
        auditConversion(colName, typeString, "DataTypes.LongType")
        returnDataType = DataTypes.LongType
      case "FLOAT" =>
        auditConversion(colName, typeString, "DataTypes.FloatType")
        returnDataType = DataTypes.FloatType
      case "DOUBLE" =>
        auditConversion(colName, typeString, "DataTypes.DoubleType")
        returnDataType = DataTypes.DoubleType
      case "DECIMAL" =>
        auditConversion(colName, typeString, "DataTypes.DoubleType")
        returnDataType = DataTypes.createDecimalType()
      case "STRING" =>
        auditConversion(colName, typeString, "DataTypes.StringType")
        returnDataType = DataTypes.StringType
      case "VARCHAR" =>
        auditConversion(colName, typeString, "DataTypes.StringType")
        returnDataType = DataTypes.StringType
      case "BINARY" =>
        auditConversion(colName, typeString, "DataTypes.StringType")
        returnDataType = DataTypes.BinaryType
      case "BOOLEAN" =>
        auditConversion(colName, typeString, "DataTypes.BooleanType")
        returnDataType = DataTypes.BooleanType
      case "DATE" =>
        auditConversion(colName, typeString, "DataTypes.DateType")
        returnDataType = DataTypes.DateType
      case "DATETIME" =>
        auditConversion(colName, typeString, "DataTypes.DateType")
        returnDataType = DataTypes.DateType
      case "TIMESTAMP" =>
        auditConversion(colName, typeString, "DataTypes.TimestampType")
        returnDataType = DataTypes.TimestampType
      case "ARRAY" =>
        auditConversion(colName, typeString, "DataTypes.ArrayType")
        returnDataType = DataTypes.createArrayType(DataTypes.StringType, true)
      case "NULL" =>
        auditConversion(colName, typeString, "DataTypes.NullType")
        returnDataType = DataTypes.NullType
      case _ =>
        logger.warn(
          "INVALID Schema file DataType found! >>>>>" + typeString +
            "<<<<<<")
        logger.warn("INVALID Schema file DataType for column: " + colName)
        logger.warn(
          "Interpreting as DataTypes.StringType for Spark, continuing...")
        returnDataType = DataTypes.StringType

    }
    returnDataType
  }

  private def auditConversion(colName: String,
                              typeString: String,
                              sparkType: String): Unit = {
    logger.debug(
      "Interpreting Schema file datatype: " + typeString + " to Spark DataType: " +
        sparkType +
        " for column: " +
        colName)
  }
}

class EDIFileSchema(@BeanProperty var delimiter: Int,
                    @BeanProperty var quotechar: Int,
                    @BeanProperty var attributes: List[SchemaAttributes]) {


  def getAttributesStruct(): StructType = {
    val fields: List[StructField] = new ArrayList[StructField]()
    for (i <- 0 until getAttributes.size) {
      val fieldName: String = getAttributes.get(i).getName
      val fieldType: String = getAttributes.get(i).getType
      val fieldNullable: java.lang.Boolean = getAttributes.get(i).getNullable
      fields.add(
        DataTypes.createStructField(fieldName,
          getDataTypeForSchema(fieldName, fieldType),
          fieldNullable))
    }
    val schema: StructType = DataTypes.createStructType(fields)
    schema
  }

}
