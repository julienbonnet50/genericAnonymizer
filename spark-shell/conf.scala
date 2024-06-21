val sourcePath                  = ""
val sourceFormat                = ""
val sourceDelimiter             = ""
val sourceHeader                = ""
val sourcePartitionFilter       = ""

val targetPath                  = ""
val targetSaveMode              = ""
val targetFormat                = ""
val targetDelimiter             = ""
val targetHeader                = ""
val targetReferentialPath       = ""
val targetMaxRows               = ""


val compression                 = "gzip"
val partitionBy                 = ""
val encoding                    = ""
val withInnerRepartition        = "true"
val preSqlStatements            = "SET sha256_one = sha256('one')"
val innerRepartitionExpr        = ""
val sha256KeepSourceLength      = "true"
val sha256MinLength             = "8"
val excludeColNames             = ""
val excludeTypeNames            = ""


val byteKeyFactor               = Anonymizer.generateKeyFactor().toString
val floatKeyFactor              = Anonymizer.generateKeyFactor().toString
val integerKeyFactor            = Anonymizer.generateKeyFactor().toString
val longKeyFactor               = Anonymizer.generateKeyFactor().toString
val shortKeyFactor              = Anonymizer.generateKeyFactor().toString
val doubleKeyFactor             = Anonymizer.generateKeyFactor().toString
val decimalKeyFactor            = Anonymizer.generateKeyFactor().toString

val confArray: Array[String] = Array(

    /* Principal setting */
        /* Source */

    // "--source-path", "file:///C:/Users/julie/Documents/genericAnonymizer/anonymizedDataset/basic1_json.json",
    "--source-path", sourcePath,
    // "--source-basepath",  "",
    // "--source-partition-filter", "lol",
    "--source-format", sourceFormat,
    "--source-delimiter", sourceDelimiter,
    "--source-header", sourceHeader,
    "--source-partition-filter", sourcePartitionFilter,
    "--encoding", encoding,

        /* Target */
    "--target-path", targetPath,
    "--save-mode", targetSaveMode,
    "--compression", compression,
    "--target-format", targetFormat,
    "--target-delimiter", targetDelimiter,
    "--target-header", targetHeader,
    "--target-referential-path", targetReferentialPath,
    "--target-max-rows", targetMaxRows,

    "--partition-by", partitionBy,
    "--partition-num", partitionNum,

    /* Anonymizer */
        /* Principals param */

    // "--with-inner-repartition", withInnerRepartition,
    "--pre-sql-statements", preSqlStatements,
    "--inner-repartition-expr", innerRepartitionExpr,
    
        /* Numeric type */
    "--anonymizer-byte-key-factor", byteKeyFactor,
    "--anonymizer-short-key-factor", shortKeyFactor,
    "--anonymizer-integer-key-factor", integerKeyFactor,
    "--anonymizer-long-key-factor", longKeyFactor,
    "--anonymizer-short-key-factor", shortKeyFactor,
    "--anonymizer-double-key-factor", doubleKeyFactor,
    "--anonymizer-float-key-factor", floatKeyFactor,
    "--anonymizer-decimal-key-factor", decimalKeyFactor,
    "--anonymizer-exclude-col-names", excludeColNames,
    "--anonymizer-exclude-type-names", excludeTypeNames
)