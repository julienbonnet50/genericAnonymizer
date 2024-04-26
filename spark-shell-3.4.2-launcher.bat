@ECHO OFF

:: Assign all Path variables
set "PATH=C:\Windows\System32"
set "HOME=C:\{TODO: DEFINE}"

:: Assign versions
set "SPARK_VERSION={TODO: DEFINE}"
set "JAVA_VERSION={TODO: DEFINE}"
set "HADOOP_VERSION={TODO: DEFINE}"

:: Assign Path home
set "JAVA_HOME=%HOME%\bin\%JAVA_VERSION%"
set "SPARK_HOME=%HOME%\bin\spark-%SPARK_VERSION%-{TODO: DEFINE}"
set "HADOOP_HOME=%HOME%\bin\hadoop-%HADOOP_VERSION%"

set "PATH=%JAVA_HOME%\bin;%HADOOP_HOME%\bin;%SPARK_HOME%\bin;%PATH%"

:: Run spark-shell
spark-shell.cmd ^
    -i {TODO: DEFINE}\datasetGenerator\dataset\randomDataGenerator.scala ^
    -i {TODO: DEFINE}\datasetGenerator\dataset\exception.scala ^
    -i {TODO: DEFINE}\datasetGenerator\dataset\datasetGenerator.scala ^
    -i {TODO: DEFINE}\datasetGenerator\dataset\datasetArgs\conf.scala ^
    -i {TODO: DEFINE}\Documents\datasetGenerator\main.scala ^
    -i {TODO: DEFINE}\main.scala ^
    --name "Dataset Generic Generator" ^
    ::--master local[8] ^
    ::--driver-memory 6g