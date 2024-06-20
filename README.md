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
```csv
0.6499535747446611,0.09285051067780874,2.3676880222841223,1.6248839368616528,ef80075a
0.6499535747446611,0.09285051067780874,2.274837511606314,1.392757660167131,ef80075a
0.6035283194057568,0.09285051067780874,2.181987000928505,1.4856081708449398,ef80075a
```
