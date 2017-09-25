# EEL6761-Cloud-Computing-and-Storage

Usage of WordCount.java:
Command line: hadoop jar jarfile.jar WordCount /input output WordCountType .
WordCountType would be Single if no. of single words need to be computed, else it would be Double.
This is necessary, else no results will be computed.

Usage of DistributedCacheWordCount.java:
Command line: hadoop jar jarfile.jar DistributedCacheWordCount /input output --cacheFile /inputdataforcachefile
