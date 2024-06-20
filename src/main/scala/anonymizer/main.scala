val hadoopConf = spark.sparkContext.hadoopConfiguration
val fs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
fs.setWriteChecksum(false)

AnonymizerApp.init(confArray)

AnonymizerApp.main()