# DataIntensiveComputing

## Run

all following commands run in /src directory
To run in regular test (python) mode :
```
python job_test3.py --stopwords=stopwords.txt  reviews_devset.json > ../output.txt 
````

To run in Hadoop "simulation" mode :
> It utilizes subtasks and kinda does it in a way hadoop does.  
```
python job_test3.py --runner=local --no-bootstrap-mrjob --stopwords=stopwords.txt reviews_devset.json > ../output.txt
```

To run on Hadoop real mode:
```
python job_test3.py --hadoop-streaming-jar /usr/lib/hadoop/tools/lib/hadoop-streaming-3.3.5.jar -r hadoop --stopwords=stopwords.txt hdfs:///user/dic24_shared/amazon-reviews/full/reviewscombined.json > ../output.txt
```
or there is an option to run run.sh which is set to make the same as the last command above

## Report

https://www.overleaf.com/read/hgmvxbfwfsmn#992dcf