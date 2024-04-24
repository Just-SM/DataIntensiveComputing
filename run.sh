#!/bin/bash

# python .\job_test3.py --stopwords=.\stopwords.txt  .\reviews_devset.json > .\output.txt 
python job_test3.py --hadoop-streaming-jar /usr/lib/hadoop/tools/lib/hadoop-streaming-3.3.5.jar -r hadoop --stopwords=stopwords.txt hdfs:///user/dic24_shared/amazon-reviews/full/reviewscombined.json > output.txt