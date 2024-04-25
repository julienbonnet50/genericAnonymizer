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

}
