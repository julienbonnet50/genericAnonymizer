package anonymizer

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
    var excludeTypeNames: Seq[String] = Seq()

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
        value.map(value => anonymizeDataType(values, elementType))
    }

    private def anonymizeMapType(values: Map[Any, Any], elementType: DataType): Map[Any, Any] = {
        values.map { case(key, value) => anonymizeDataType(key, keyType) -> anonymizeDataType(value, valueType)}
    }

    private def anonymizeStructType(values: Row, schema: StructType): Row = {
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
        if (excludeTypeNames.contains(dataType.typeName)) return value
        dataType match {
            /* Numeric types */
            case ByteType => anonymizeByteType(value.asInstanceOf[Byte])
            case ShortType => anonymizeByteType(value.asInstanceOf[ShortType])
            case
        }
    }
}


