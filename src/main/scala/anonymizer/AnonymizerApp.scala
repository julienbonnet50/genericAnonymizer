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
    private var sourceHeader = ""

    var targetOptions = Map[String, String]()
    private var targetDelimiter = ""
    private var targetBasePath = ""
    private var targetFormat = ""
    private var targetPath = ""
    private var targetHeader = ""
    private var targetReferentialPath = ""

    private var encoding = ""
    private var saveMode = ""
    private var compression = ""
    private var partitionBy = ""
    private var withInnerRepartition = false
    private var preSqlStatements = Seq[String]()
    private var innerRepartitionExpr = ""
    private var filename = ""

    def init(args: Array[String]): Unit = {

        @scala.annotation.tailrec
        def nextArg(map: Map[String, Any], list: List[String]): Map[String, Any] = {            
            list match {
                case Nil => map
                /* Source */
                case "--source-basepath" :: value :: tail =>
                nextArg(map ++ Map("sourceBasePath" -> value), tail)
                case "--source-path" :: value :: tail =>
                nextArg(map ++ Map("sourcePath" -> value), tail)
                case "--source-partition-filter" :: value :: tail =>
                nextArg(map ++ Map("sourcePartitionFilter" -> value), tail)
                case "--source-format" :: value :: tail =>
                nextArg(map ++ Map("sourceFormat" -> value), tail)
                case "--source-delimiter" :: value :: tail =>
                nextArg(map ++ Map("sourceDelimiter" -> value), tail)
                case "--source-header" :: value :: tail =>
                nextArg(map ++ Map("sourceHeader" -> value), tail)

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
                nextArg(map ++ Map("targetFormat" -> value), tail)
                case "--target-path" :: value :: tail =>
                nextArg(map ++ Map("targetPath" -> value), tail)
                case "--target-delimiter" :: value :: tail =>
                nextArg(map ++ Map("targetDelimiter" -> value), tail)
                case "--encoding" :: value :: tail =>
                nextArg(map  ++ Map("encoding" -> value), tail)
                case "--target-referential-path" :: value :: tail =>
                nextArg(map ++ Map("targetReferentialPath" -> value), tail)
                case "--target-header" :: value :: tail =>
                nextArg(map ++ Map("targetHeader" -> value), tail)


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
                case "--anonymizer-exclude-col-names" :: value :: tail =>
                nextArg(map ++ Map("anonymizerExcludeColNames" -> value.split(',').map(_.trim).toSeq), tail)
                case "--anonymizer-exclude-type-names" :: value :: tail =>
                nextArg(map ++ Map("anonymizerExcludeTypeNames" -> value.split(',').map(_.trim).toSeq), tail)

                case "--with-inner-repartition" :: value :: tail =>
                nextArg(map ++ Map("withInnerRepartition" -> value.toBoolean), tail)
                case "--pre-sql-statements" :: value :: tail =>
                nextArg(map ++ Map("preSqlStatements" -> value.split(';').map(_.trim).toSeq), tail)
                case "--inner-repartition-expr" :: value :: tail =>
                nextArg(map ++ Map("innerRepartitionExpr" -> value), tail)

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
        sourceHeader = getOption("sourceHeader", "").asInstanceOf[String]

        targetBasePath = getOption("targetBasePath", "").asInstanceOf[String]
        targetFormat = getOption("targetFormat", "parquet").asInstanceOf[String]
        targetDelimiter = getOption("targetDelimiter", "").asInstanceOf[String]
        targetPath = getOption("targetPath", "").asInstanceOf[String]
        targetHeader = getOption("targetHeader", "").asInstanceOf[String]
        targetReferentialPath = getOption("targetReferentialPath", "").asInstanceOf[String]

        encoding = getOption("encoding", "").asInstanceOf[String]
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
        Anonymizer.excludeTypeNames = getOption("anonymizerExcludeTypeNames", Anonymizer.excludeTypeNames).asInstanceOf[Seq[String]]
    }

    def setFilename(): Unit = {
        import java.time.LocalDateTime
        import java.time.format.DateTimeFormatter
        import scala.util.matching.Regex

        val currentDateTime = LocalDateTime.now()
        val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss")
        val regex = new Regex("([^/]+)+$")
        val lastElement = regex.findFirstMatchIn(sourcePath).get.group(1)
        val formattedDateTime = currentDateTime.format(formatter)

        filename = s"$lastElement" + "_" + s"$formattedDateTime"

    }

    def writeReferential(): Unit = {
        import org.apache.spark.sql.functions.struct
        import org.apache.spark.sql.types.{StringType, StructField, StructType}

        val attributeSeq = List(
            List("filename", filename),
            List("sourceBasePath", sourceBasePath.toString),
            List("sourceFormat", sourceFormat.toString),
            List("sourceDelimiter", sourceDelimiter.toString),
            List("sourcePath", sourcePath.toString),
            List("sourcePartitionFilter", sourcePartitionFilter.toString),
            List("sourceHeader", sourceHeader.toString),
            List("targetReferentialPath", targetReferentialPath.toString),
            List("targetBasePath", targetBasePath.toString),
            List("targetFormat", targetFormat.toString),
            List("targetDelimiter", targetDelimiter.toString),
            List("targetPath", targetPath.toString),
            List("encoding", encoding.toString),
            List("saveMode", saveMode.toString),
            List("compression", compression.toString),
            List("partitionBy", partitionBy.toString),
            List("withInnerRepartition", withInnerRepartition.toString),
            List("preSqlStatements", preSqlStatements.toString),
            List("innerRepartitionExpr", innerRepartitionExpr.toString),
            List("Anonymizer.byteKeyFactor", Anonymizer.byteKeyFactor.toString),
            List("Anonymizer.shortKeyFactor", Anonymizer.shortKeyFactor.toString),
            List("Anonymizer.integerKeyFactor", Anonymizer.integerKeyFactor.toString),
            List("Anonymizer.longKeyFactor", Anonymizer.longKeyFactor.toString),
            List("Anonymizer.floatKeyFactor", Anonymizer.floatKeyFactor.toString),
            List("Anonymizer.doubleKeyFactor", Anonymizer.doubleKeyFactor.toString),
            List("Anonymizer.decimalKeyFactor", Anonymizer.decimalKeyFactor.toString),
            List("Anonymizer.sha256KeepSourceLength", Anonymizer.sha256KeepSourceLength.toString),
            List("Anonymizer.sha256MinLength", Anonymizer.sha256MinLength.toString),
            List("Anonymizer.excludeColNames", Anonymizer.excludeColNames.toString),
            List("Anonymizer.excludeTypeNames", Anonymizer.excludeTypeNames.toString)
        )

        val schema = StructType(
            Seq(
                StructField("conf", StringType, true),
                StructField("value", StringType, true)
            )
        )

        val rows = attributeSeq.map(attributes => Row(attributes(0).toString, attributes(1)))

        val dfReferential = spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)

        dfReferential 
            .repartition(1)
            .write
            .format("json")
            .save(targetReferentialPath + "/" + filename + ".json")

    }

    def loadOptions(): Unit = {
        /* Source options */
        if (!(sourceBasePath == "")) {
            sourceOptions = sourceOptions ++ Map("basePath" -> sourceBasePath)
        } 
        if (!(sourceDelimiter == "")) {
            sourceOptions = sourceOptions ++ Map("delimiter" -> sourceDelimiter)
        } 
        if (!(sourceHeader == "")) {
            sourceOptions = sourceOptions ++ Map("header" -> sourceHeader)
        } 
        if (!(encoding == "")) {
            sourceOptions = sourceOptions ++ Map("encoding" -> encoding)
        }
        if (sourceHeader == "" && sourceFormat == "csv") {
            sourceOptions = sourceOptions ++ Map("header" -> "true")
        }
        
        /* Target options */
        if (!(targetDelimiter == "")) {
            targetOptions = targetOptions ++ Map("delimiter" -> targetDelimiter)
        }
        if (!(targetBasePath == "")) {
            targetOptions = targetOptions ++ Map("basePath" -> targetBasePath)
        }
        if (!(saveMode == "")) {
            targetOptions = targetOptions ++ Map("mode" -> saveMode)
        }
        if (!(targetDelimiter == "")) {
            targetOptions = targetOptions ++ Map("delimiter" -> sourceDelimiter)
        }
        if (!(encoding == "")) {
            targetOptions = targetOptions ++ Map("encoding" -> encoding)
        }
        if (targetHeader == "" && targetFormat == "csv") {
            targetOptions = targetOptions ++ Map("header" -> "true")
        }
    }

    def main(): Unit = {

        println("Starting app ")

        // Load conf
        loadOptions()
        setFilename()
        
        val partitionColumnNames = partitionBy.split(',')

        var df = spark
            .read
            .format(sourceFormat)
            .options(sourceOptions)
            .load(sourcePath)

        // Encoder
        val encoder: ExpressionEncoder[Row] = RowEncoder.apply(df.schema).resolveAndBind()

        df = df.map(row => Anonymizer.anonymizeRow(row), encoder)

        // Write referential
        if (targetReferentialPath != "") {
            writeReferential()
        }

        if(withInnerRepartition) {
            spark.udf.register("sha256", udf((s: String) => Anonymizer.sha256(s)))
            preSqlStatements.foreach(sql => spark.sql(sql))
            val partitionColumns = partitionColumnNames.map(name => col(name))
            val withInnerRepartitionColumns = partitionColumns :+ col("__inner_repartition")
            df = df.withColumn("__inner_repartition", expr(innerRepartitionExpr))
                .repartition(withInnerRepartitionColumns: _*)
                .drop("__inner_repartition")
        } 

        if (partitionBy != "") {
            df  
                .write
                .format(targetFormat)
                .options(targetOptions)
                .mode(saveMode)
                .partitionBy(partitionColumnNames: _*)
                .save(targetPath)
        }
        else {
            df
                .write
                .format(targetFormat)
                .options(targetOptions)
                .mode(saveMode)
                .save(targetPath)

        }
    }  
    
}
