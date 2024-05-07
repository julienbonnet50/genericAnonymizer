val confArray: Array[String] = Array(
    /* Principal setting */
    "--source-basepath", "pathTo/datasetToGenerate",
    // "--source-partition-filter", "lol",
    "--target-basepath", "pathTo/generatedDataset",
    "--save-mode", "overwrite",
    "--compression", "gzip",
    "--output-format", "parquet"
    // "--partition-by", ""

    /* Numeric type */
    "--anonymizer-byte-key-factor", "1",
    "--anonymizer-short-key-factor", "1",
    "--anonymizer-integer-key-factor", "1",
    "--anonymizer-long-key-factor", "1"
)