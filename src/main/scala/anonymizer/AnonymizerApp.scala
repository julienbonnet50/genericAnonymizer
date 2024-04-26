package anonymizer

import anonymizer.Anonymizer
import java.beans.Expression

object AnonymizerApp {
  
    import org.apache.spark.sql._
    import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
    import org.apache.spark.sql.functions._

    def main(args: Array[String]): Unit = {

        @scala.annotation.tailrec
        def nextArg(map: Map[String, Any], list: List[String]): Map[String, Any] = {
        list match {
            case Nil => map
            case "--source-basepath" :: value :: tail =>
            nextArg(map ++ Map("sourceBasePath" -> value), tail)
            case "--source-partition-filter" :: value :: tail =>
            nextArg(map ++ Map("sourcePartitionFilter" -> value), tail)
            case "--target-basepath" :: value :: tail =>
            nextArg(map ++ Map("targetBasePath" -> value), tail)
            case "--save-mode" :: value :: tail =>
            nextArg(map ++ Map("saveMode" -> value), tail)
            case "--compression" :: value :: tail =>
            nextArg(map ++ Map("compression" -> value), tail)
            case "--partition-by" :: value :: tail =>
            nextArg(map ++ Map("partitionBy" -> value), tail)
            case "--anonymizer-byte-key-factor" :: value :: tail =>
            nextArg(map ++ Map("anonymizerByteKeyFactor" -> value.toDouble), tail)
            case "--anonymizer-short-key-factor" :: value :: tail =>
            nextArg(map ++ Map("anonymizerShortKeyFactor" -> value.toDouble), tail)
            case "--anonymizer-integer-key-factor" :: value :: tail =>
            nextArg(map ++ Map("anonymizerIntegerKeyFactor" -> value.toDouble), tail)
            case "--anonymizer-long-key-factor" :: value :: tail =>
            nextArg(map ++ Map("anonymizerLongKeyFactor" -> value.toDouble), tail)
            case "--anonymizer-float-key-factor" :: value :: tail =>
            nextArg(map ++ Map("anonymizerFloatKeyFactor" -> value.toDouble), tail)
            case "--anonymizer-double-key-factor" :: value :: tail =>
            nextArg(map ++ Map("anonymizerDoubleKeyFactor" -> value.toDouble), tail)
            case "--anonymizer-decimal-key-factor" :: value :: tail =>
            nextArg(map ++ Map("anonymizerDecimalKeyFactor" -> value.toDouble), tail)
            case "--anonymizer-sha256-keep-source-length" :: value :: tail =>
            nextArg(map ++ Map("anonymizerSha256KeepSourceLength" -> value.toBoolean), tail)
            case "--anonymizer-sha256-min-length" :: value :: tail =>
            nextArg(map ++ Map("anonymizerSha256MinLength" -> value.toInt), tail)
            case "--anonymizer-exclude-col-name" :: value :: tail =>
            nextArg(map ++ Map("anonymizerExcludeColName" -> value.split(',').map(_.trim).toSeq), tail)
            case "--anonymizer-exclude-types-name" :: value :: tail =>
            nextArg(map ++ Map("anonymizerExcludeTypesName" -> value.split(',').map(_.trim).toSeq), tail)
            case ("--") :: _ =>
            map
            case unknown :: _ =>
            println("Unknown option " + unknown)
            scala.sys.exit(1)
        }
        }

        val options = nextArg(Map(), args.toList)

        def getOption(name: String, defaultValue: Any = null): Any = {
            try {
                options(name)
            } catch {
                case _: Throwable =>
                    if (defaultValue != null)
                        defaultValue
                    else scala.sys.error(s"No option '$name' found in command line")
            }
        }

        val spark = SparkSession
            .builder
            .appName("Spark Generic Anonymizer")
            .getOrCreate()

        
        val sourceBasePath: String = getOption("sourceBasePath").asInstanceOf[String]
        val targetBasePath: String = getOption("targetBasePath").asInstanceOf[String]
        val sourcePartitionFilter: String = getOption("sourcePartitionFilter", "").asInstanceOf[String]
        val saveMode: String = getOption("saveMode", "overwrite").asInstanceOf[String]
        val compression: String = getOption("compression", "gzip").asInstanceOf[String]
        val partitionBy: String = getOption("partitionBy").asInstanceOf[String]
        val withInnerRepartition: Boolean = getOption("withInnerRepartition", false).asInstanceOf[Boolean]
        val preSqlStatements: Seq[String] = getOption("preSqlStatements", Seq()).asInstanceOf[Seq[String]]
        val innerRepartitionExpr: String = getOption("innerRepartitionExpr", "cast(rand() * 99 as int) % 3").asInstanceOf[String]

        Anonymizer.byteKeyFactor = getOption("anonymizerByteKeyFactor", Anonymizer.byteKeyFactor).asInstanceOf[Double]
        Anonymizer.shortKeyFactor = getOption("anonymizerShortKeyFactor", Anonymizer.shortKeyFactor).asInstanceOf[Double]
        Anonymizer.integerKeyFactor = getOption("anonymizerIntegerKeyFactor", Anonymizer.integerKeyFactor).asInstanceOf[Double]
        Anonymizer.longKeyFactor = getOption("anonymizerLongKeyFactor", Anonymizer.longKeyFactor).asInstanceOf[Double]
        Anonymizer.floatKeyFactor = getOption("anonymizerFloatKeyFactor", Anonymizer.floatKeyFactor).asInstanceOf[Double]
        Anonymizer.doubleKeyFactor = getOption("anonymizerDoubleKeyFactor", Anonymizer.doubleKeyFactor).asInstanceOf[Double]
        Anonymizer.decimalKeyFactor = getOption("anonymizerDecimalKeyFactor", Anonymizer.decimalKeyFactor).asInstanceOf[Double]
        Anonymizer.sha256KeepSourceLength = getOption("anonymizerSha256KeepSourceLength", Anonymizer.sha256KeepSourceLength).asInstanceOf[Boolean]
        Anonymizer.sha256MinLength = getOption("anonymizerSha256MinLength", Anonymizer.sha256MinLength).asInstanceOf[Int]
        Anonymizer.excludeColNames = getOption("anonymizerExcludeColNames", Anonymizer.excludeColNames).asInstanceOf[Seq[String]]
        Anonymizer.excludeTypesNames = getOption("anonymizerExcludeTypesNames", Anonymizer.excludeTypesNames).asInstanceOf[Seq[String]]

        val partitionColumnNames = partitionBy.split(',')

        var df = spark
            .read
            .option("basePath", sourceBasePath)
            .format("parquet")
            .load(s"$sourceBasePath/$sourcePartitionFilter")

        // Encoder
        val encoder: ExpressionEncoder[Row] = RowEncoder.apply(df.schema).resolveAndBind()

        df = df.map(row => Anonymizer.anonymizeRow(row), encoder)

        if(withInnerRepartition) {
            spark.udf.register("sha256", udf((s: String) => Anonymizer.sha256(s)))
            preSqlStatements.foreach(sql => spark.sql(sql))
            val partitionColumns = partitionColumnNames.map(name => col(name))
            val withInnerRepartitionColumns = partitionColumns :+ col("__inner_repartition")
            df = df.withColumn("__inner_repartition", expr(innerRepartitionExpr))
                .repartition(withInnerRepartitionColumns: _*)
                .drop("__inner_repartition")
        }

        df
            .write
            .mode(saveMode)
            .format("parquet")
            .option("compression", compression)
            .partitionBy(partitionColumnNames: _*)
            .save(targetBasePath)
        
    }
}
