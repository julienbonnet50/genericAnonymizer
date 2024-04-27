package anonymizer

import java.security.Timestamp

object Anonymizer extends Serializable {

    import org.apache.commons.codec.digest.DigestUtils
    import org.apache.spark.sql._
    import org.apache.spark.sql.types._

    import scala.math.ceil

    var byteKeyFactor: Double = 1.0
    var shortKeyFactor: Double = 1.0
    var integerKeyFactor: Double = 1.0
    var longKeyFactor: Double = 1.0
    var floatKeyFactor: Double = 1.0
    var doubleKeyFactor: Double = 1.0
    var decimalKeyFactor: Double = 1.0
    var sha256KeepSourceLength: Boolean = true
    var sha256MinLength: Int = 8
    var excludeColNames: Seq[String] = Seq()
    var excludeTypesNames: Seq[String] = Seq()

    def generateKeyFactor(): Double = {
        val r = new scala.util.Random()
        ((r.nextInt().abs % 3 + 1 + r.nextDouble()) * 1000).round / 1000.toDouble
    }

    def sha256(value: String, keepSourceLength: Boolean = sha256KeepSourceLength, minLength: Int = sha256MinLength): String = {
        if (value == null) return null
        if (value.isEmpty) return ""

        val sha256 = new DigestUtils("SHA-256").digestAsHex(value)

        if(!keepSourceLength) {
            return sha256
        }

        val len = if (minLength > 0 && value.length() < minLength) minLength else value.length()

        if (len < sha256.length) {
            sha256.substring(0, len)
        } else {
            sha256.concat("=" * (len - sha256.length))
        }
    }

    private def anonymizeByteType(value: Byte): Byte = {
        ceil(value.toDouble / byteKeyFactor).toByte
    }

    private def anonymizeShortType(value: Short): Short = {
        ceil(value.toDouble / shortKeyFactor).toShort 
    }

    private def anonymizeIntegerType(value: Int): Int = {
        ceil(value.toDouble / integerKeyFactor).toInt
    }

    private def anonymizeLongType(value: Long): Long = {
        ceil(value.toDouble / longKeyFactor).toLong
    }

    private def anonymizeFloatType(value: Float): Float = {
        value / floatKeyFactor.toFloat
    }

    private def anonymizeDoubleType(value: Double): Double = {
        value / doubleKeyFactor
    }

    private def anonymizeDecimalType(value: Decimal): Decimal = {
        value / Decimal(decimalKeyFactor)
    }

    private def anonymizeStringType(value: String): String = {
        sha256(value)
    } 

    private def anonymizeVarcharType(value: String): String = {
        sha256(value, keepSourceLength = true, minLength = 0)
    }

    private def anonymizeCharType(value: String): String = {
        sha256(value, keepSourceLength = true, 0)
    }

    private def anonymizeBinaryType(value: Array[Byte]): Array[Byte] = {
        new DigestUtils("SHA-256").digest(value)
    }

    private def anonymizeBooleanType(): Boolean = {
        new scala.util.Random().nextBoolean()
    }

    private def anonymizeArrayType(values: Seq[Any], elementType: DataType): Seq[Any] = {
        values.map(value => anonymizeDataType(value, elementType))
    }

    private def anonymizeMapType(values: Map[Any, Any], keyType: DataType, valueType: DataType): Map[Any, Any] = {
        values.map { case(key, value) => anonymizeDataType(key, keyType) -> anonymizeDataType(value, valueType)}
    }

    private def anonymizeStructType(value: Row, schema: StructType): Row = {
        Row.fromSeq(schema
        .fields
        .zipWithIndex
        .map { case (field, index) =>
            if(!excludeColNames.contains(field.name)) {
                anonymizeDataType(value.get(index), field.dataType)
            } else {
                value.get(index)
            }
        })
    }

    private def anonymizeDataType(value: Any, dataType: DataType): Any = {
        if (value == null) return null
        if (excludeTypesNames.contains(dataType.typeName)) return value
        dataType match {
            /* Numeric types */
            case ByteType => anonymizeByteType(value.asInstanceOf[Byte])
            case ShortType => anonymizeShortType(value.asInstanceOf[Short])
            case IntegerType => anonymizeIntegerType(value.asInstanceOf[Int])
            case LongType => anonymizeLongType(value.asInstanceOf[Long])
            case FloatType => anonymizeFloatType(value.asInstanceOf[Float])
            case DoubleType => anonymizeDoubleType(value.asInstanceOf[Double])
            case _: DecimalType => anonymizeDecimalType(value.asInstanceOf[Decimal])

            /* String types */
            case StringType => anonymizeStringType(value.asInstanceOf[String])
            case _: VarcharType => anonymizeVarcharType(value.asInstanceOf[String])
            case _: CharType => anonymizeCharType(value.asInstanceOf[String])

            /* Binary types */
            case BinaryType => anonymizeBinaryType(value.asInstanceOf[Array[Byte]])
            
            /* Boolean types */
            case BooleanType => anonymizeBooleanType()

            /* TODO: Date types */
            case DateType => value
            case TimestampType => value 

            /* TODO: Interval types */
            case _: YearMonthIntervalType => value
            case _: DayTimeIntervalType => value
            
            /* Complex types */
            case t: ArrayType => anonymizeArrayType(value.asInstanceOf[Seq[Any]], t.elementType)
            case t: MapType => anonymizeMapType(value.asInstanceOf[Map[Any, Any]], t.keyType, t.valueType)
            case t: StructType => anonymizeStructType(value.asInstanceOf[Row], t)

            case _ => value
        }
    }

    def anonymizeRow(row: Row): Row = {
        anonymizeDataType(row, row.schema).asInstanceOf[Row]
    }
}


