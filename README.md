# devodstoolkit

## Installing 

The Devo DS Toolkit requires Python 3+

```
pip install git+https://github.com/devods/devodstoolkit.git
```

## Querying Devo

To query Devo, create an `API` object found in [api.py](https://github.com/devods/devodstoolkit/blob/master/api.py)

`devo_api = API(api_key={your api key}, api_secret={your api secret key}, end_point)`   

The `end_point` for the US is `'https://api-us.logtrust.com/search/query'` and 
for the EU is `'https://api-eu.logtrust.com/search/query'`


Once you have created an API object, you can query Devo by calling the query method by specifying a LINQ query and a start time.
You may also optionaly specify a stop time and a type of output.  In the example below, results will be an iterator of dictionaries. 
Each dictionary will coorespond to a row where the keys reference the column names.
```
linq_query = '''from siem.logtrust.web.activity
select *'''

results = devo_api.query(linq_query, start='2018-12-01', stop='2018-12-02')
```

To get the results as a pandas dataframe, specify `output='dataframe'` as an argument to the query method.  
For a continuous query, ommit the stop argument when calling the query method. 
