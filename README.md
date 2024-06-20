# Generic Anonymzer

genericAnonymizer is a data anonymization tool working with ***Spark*** allowing you to anonymize a dataset with **any** schema and any **types**.

Benefits of genericAnonymizer is to anonymize **deeply nested** dataset, keep your column distribution and
possibility of avoiding columns names or types.

#### How ?

genericAnonymizer is mapping your dataset by rows and return anonymized values.

Our app is using different anonymization method for specific type encountered.

- String values : SHA256 Hash
- Numeric values : Random decimal number between 1 and 4
- TODO : Date values

## Pre-require

- Spark shell with spark **3.4.2**

- Build jar file on your own and use it in with your spark engine

## Example

Example of anonymization with **iris.json** sample

#### Before anonymization
```json
{"sepalLength": 5.1, "sepalWidth": 3.5, "petalLength": 1.4, "petalWidth": 0.2, "species": "setosa"},
{"sepalLength": 4.9, "sepalWidth": 3.0, "petalLength": 1.4, "petalWidth": 0.2, "species": "setosa"},
{"sepalLength": 4.7, "sepalWidth": 3.2, "petalLength": 1.3, "petalWidth": 0.2, "species": "setosa"},
```

#### After anonymization
Ratios has been modifiated, and species values has been anonymized
- Json
```json
{"petalLength":0.5899705014749261,"petalWidth":0.08428150021070374,"sepalLength":2.1491782553729455,"sepalWidth":1.4749262536873156,"species":"ef80075a"}
{"petalLength":0.5899705014749261,"petalWidth":0.08428150021070374,"sepalLength":2.0648967551622417,"sepalWidth":1.264222503160556,"species":"ef80075a"}
{"petalLength":0.5478297513695743,"petalWidth":0.08428150021070374,"sepalLength":1.9806152549515381,"sepalWidth":1.3485040033712599,"species":"ef80075a"}
```
- Csv
```json
petalLength,petalWidth,sepalLength,sepalWidth,species
0.44150110375275936,0.0630715862503942,1.608325449385052,1.1037527593818985,ef80075a
0.44150110375275936,0.0630715862503942,1.545253863134658,0.946073793755913,ef80075a
0.4099653106275623,0.0630715862503942,1.4821822768842638,1.0091453800063073,ef80075a
```

#### Sample of referential written 

Referential is written to keep numeric factor and metadata of anonymization
```json
{"conf":"sourceBasePath","value":""}
{"conf":"sourceFormat","value":"json"}
{"conf":"sourceDelimiter","value":""}
{"conf":"sourcePath","value":"./sample/input/iris.json"}
{"conf":"sourcePartitionFilter","value":""}
{"conf":"sourceHeader","value":""}
{"conf":"targetReferentialPath","value":"./sample/output/referential"}
{"conf":"targetBasePath","value":""}
{"conf":"targetFormat","value":"csv"}
{"conf":"targetDelimiter","value":""}
{"conf":"targetPath","value":"./sample/output/iris.csv"}
{"conf":"encoding","value":""}
{"conf":"saveMode","value":"overwrite"}
{"conf":"compression","value":"gzip"}
{"conf":"partitionBy","value":""}
{"conf":"withInnerRepartition","value":"false"}
{"conf":"preSqlStatements","value":"WrappedArray(SET sha256_one = sha256('one'))"}
{"conf":"innerRepartitionExpr","value":""}
{"conf":"Anonymizer.byteKeyFactor","value":"2.133"}
{"conf":"Anonymizer.shortKeyFactor","value":"1.976"}
{"conf":"Anonymizer.integerKeyFactor","value":"2.797"}
{"conf":"Anonymizer.longKeyFactor","value":"3.724"}
{"conf":"Anonymizer.floatKeyFactor","value":"2.165"}
{"conf":"Anonymizer.doubleKeyFactor","value":"3.171"}
{"conf":"Anonymizer.decimalKeyFactor","value":"3.245"}
{"conf":"Anonymizer.sha256KeepSourceLength","value":"true"}
{"conf":"Anonymizer.sha256MinLength","value":"8"}
{"conf":"Anonymizer.excludeColNames","value":"WrappedArray()"}
{"conf":"Anonymizer.excludeTypeNames","value":"WrappedArray()"}
```


## Documentation
Most of the properties that control internal settings have reasonable default values. Some of the most common options to set are:


| PropertyName                 | Default                        | Meaning                                                        | SinceVersion 
| ----                         | --------                       | --------------                                                 | -------  
| sourceFormat                 | json                           | File format of your source dataset                             | 0.1    
| sourceBasePath               | (none)                         | Path of your spark basePath option                             | 0.1         
| sourceDelimiter              | ,                              | Delimiter between columns (mainly for CSV)                     | 0.1      
| sourcePath                   | ./sample/input/iris.json       | Path of your source dataset                                    | 0.1    
| sourcePartitionFilter        | (none)                         | TODO                                                           | 0.1         
| targetDelimiter              | ,                              | Delimiter between columns (mainly for CSV) of target dataset   | 0.1   
| targetFormat                 | (none)                         | File format of your target dataset                             | 0.1    
| targetBasePath               | (none)                         | Path of your spark basePath option of target dataset           | 0.1 
| targetReferentialPath        | /sample/output/referential     | Path of referential                                            | 0.1     
| encoding                     | (none)                         | Encoding value for spark option                                | 0.1     
| compression                  | gzip                           | Compression value for spark option                             | 0.1
| withInnerRepartition         | false                          | TODO                                                           | 0.1
| preSqlStatements             | SET sha256_one = sha256('one') | SQL statements to execute before anonymization                 | 0.1  
| innerRepartitionExpr         | (none)                         | TODO                                                           | 0.1     
| saveMode                     | overwrite                      | File format for target saving                                  | 0.1         
| Anonymizer.byteKeyFactor     | 1                              | Random number key factor for anonymizer                        | 0.1   
| Anonymizer.shortKeyFactor    | 1                              | Random number key factor for anonymizer                        | 0.1 
| Anonymizer.integerKeyFactor  | 1                              | Random number key factor for anonymizer                        | 0.1 
| Anonymizer.longKeyFactor     | 1                              | Random number key factor for anonymizer                        | 0.1 
| Anonymizer.fLoatKeyFactor    | 1                              | Random number key factor for anonymizer                        | 0.1 
| Anonymizer.doubleKeyFactor   | 1                              | Random number key factor for anonymizer                        | 0.1 
| Anonymizer.decimalKeyFactor  | 1                              | Random number key factor for anonymizer                        | 0.1 
