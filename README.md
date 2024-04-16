# DataIntensiveComputing



## Run



To run in regular test (python) mode :
```
python .\job_test.py --stopwords=.\stopwords.txt  .\reviews_devset.json > .\output.txt 
````

To run in Hadoop "simulation" mode :
> It utilizes subtasks and kinda does it in a way hadoop does.  
```
python .\job_test.py --runner=local --no-bootstrap-mrjob --stopwords=.\stopwords.txt .\reviews_devset.json > .\output.txt

```

To calculate execution time uncomment main to :

> ❗ Not working in Hadoop "simulation" mode because of subtasks ❗ 

```
if __name__ == '__main__':
    start_time = datetime.now()
    MRWordFrequencyCount.run()
    end_time = datetime.now()
    elapsed_time = end_time - start_time
    print ("Time : " + str(elapsed_time))

```