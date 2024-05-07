import java.beans.Expression

object AnonymizerApp extends Serializable {
  
    import org.apache.spark.sql._
    import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
    import org.apache.spark.sql.functions._

    var sourceOptions = Map[String, String]()
    private var sourceBasePath = ""
    private var sourcePartitionFilter = ""
    private var sourceFormat = ""
    private var sourceDelimiter = ""
    private var sourcePath = ""

    var targetOptions = Map[String, String]()
    private var targetDelimiter = ""
    private var targetBasePath = ""
    private var targetFormat = ""
    private var targetPath = ""

    private var saveMode = ""
    private var compression = ""
    private var partitionBy = ""
    private var withInnerRepartition = false
    private var preSqlStatements = Seq[String]()
    private var innerRepartitionExpr = ""

    def init(args: Array[String]): Unit = {

        @scala.annotation.tailrec
        def nextArg(map: Map[String, Any], list: List[String]): Map[String, Any] = {
            list match {
                case Nil => map
                /* Source */
                case "--source-basepath" :: value :: tail =>
                nextArg(map ++ Map("sourceBasePath" -> value), tail)
                case "--source-partition-filter" :: value :: tail =>
                nextArg(map ++ Map("sourcePartitionFilter" -> value), tail)
                case "--source-format" :: value :: tail =>
                nextArg(map ++ Map("sourceFormat" -> value), tail)
                case "--source-delimiter" :: value :: tail =>
                nextArg(map ++ Map("sourceDelimiter" -> value), tail)
                case "--source-path" :: value :: tail =>
                nextArg(map ++ Map("sourcePath" -> value), tail)

                /* Target */
                case "--target-basepath" :: value :: tail =>
                nextArg(map ++ Map("targetBasePath" -> value), tail)
                case "--save-mode" :: value :: tail =>
                nextArg(map ++ Map("saveMode" -> value), tail)
                case "--compression" :: value :: tail =>
                nextArg(map ++ Map("compression" -> value), tail)
                case "--partition-by" :: value :: tail =>
                nextArg(map ++ Map("partitionBy" -> value), tail)
                case "--target-format" :: value :: tail =>
                nextArg(map ++ Map("outputFormat" -> value), tail)
                case "--target-path" :: value :: tail =>
                nextArg(map ++ Map("targetPath" -> value), tail)

                /* Anonymizer */
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
        
        sourceBasePath = getOption("sourceBasePath", "").asInstanceOf[String]
        sourceFormat = getOption("sourceFormat", "").asInstanceOf[String]
        sourceDelimiter = getOption("sourceDelimiter", "").asInstanceOf[String]
        sourcePath = getOption("sourcePath", "").asInstanceOf[String]
        sourcePartitionFilter = getOption("sourcePartitionFilter", "").asInstanceOf[String]


        targetBasePath = getOption("targetBasePath", "").asInstanceOf[String]
        targetFormat = getOption("targetFormat", "parquet").asInstanceOf[String]
        targetDelimiter = getOption("targetDelimiter", ";").asInstanceOf[String]
        saveMode = getOption("saveMode", "overwrite").asInstanceOf[String]
        compression = getOption("compression", "gzip").asInstanceOf[String]
        partitionBy = getOption("partitionBy", "").asInstanceOf[String]

        withInnerRepartition = getOption("withInnerRepartition", false).asInstanceOf[Boolean]
        preSqlStatements = getOption("preSqlStatements", Seq()).asInstanceOf[Seq[String]]
        innerRepartitionExpr = getOption("innerRepartitionExpr", "cast(rand() * 99 as int) % 3").asInstanceOf[String]

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
    }

    def loadOptions(): Unit = {
        /* Source options */
        if (!(sourceBasePath == "") || !(sourceBasePath.isEmpty) || !(sourceBasePath == null)) {
            sourceOptions = sourceOptions ++ Map("basePath" -> sourceBasePath)
        } 
        else if (!(sourceDelimiter == "") || !(sourceDelimiter.isEmpty) || !(sourceDelimiter == null)) {
            sourceOptions = sourceOptions ++ Map("delimiter" -> sourceDelimiter)
        } 
        else if (!(sourcePath == "") || !(sourcePath.isEmpty) || !(sourcePath == null)) {
            sourceOptions = sourceOptions ++ Map("path" -> sourcePath)
        }
        else if (!(sourceFormat == "") || !(sourceFormat.isEmpty) || !(sourceFormat == null)) {
            sourceOptions = sourceOptions ++ Map("format" -> sourceFormat)
        }
        
        /* Target options */
        if (!(targetDelimiter == "") || !(targetDelimiter.isEmpty) || !(targetDelimiter == null)) {
            targetOptions = targetOptions ++ Map("delimiter" -> targetDelimiter)
        }
        else if (!(targetBasePath == "") || !(targetBasePath.isEmpty) || !(targetBasePath == null)) {
            targetOptions = targetOptions ++ Map("basePath" -> targetBasePath)
        }
        else if (!(targetPath == "") || !(targetPath.isEmpty) || !(targetPath == null)) {
            targetOptions = targetOptions ++ Map("path" -> targetPath)
        }
        else if (!(saveMode == "") || !(saveMode.isEmpty) || !(saveMode == null)) {
            targetOptions = targetOptions ++ Map("mode" -> saveMode)
        }
        else if (!(targetFormat == "") || !(targetFormat.isEmpty) || !(targetFormat == null)) {
            targetOptions = targetOptions ++ Map("format" -> targetFormat)
        }
    }

    def main(): Unit = {

        println("Starting app ")
        loadOptions()

        println("source Options : " + sourceOptions)
        println("target Options : " + targetOptions)
        
        val spark = SparkSession
            .builder
            .appName("Spark Generic Anonymizer")
            .getOrCreate()
        
        val partitionColumnNames = partitionBy.split(',')

        var df = spark
            .read
            .options(sourceOptions)
            .load(sourcePath)

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
            .options(targetOptions)
            .save(targetPath)

    }  
    
}
