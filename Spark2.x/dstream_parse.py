# Design Pattern | Spark DStream | foreachRDD
# Parse stream data
# To Execute: spark-submit dstream_parse.py

import sys
import sched, time
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

if __name__ == "__main__":

    # Initialize Spark Context Objects
    sc = SparkContext(appName='SparkStreaming')
    ssc = StreamingContext(sc, 10)
    
    # Create Checkpoint
    ssc.checkpoint("file:///tmp/spark")
    
    # Function | Word Count    
    def countWords(newValues, lastSum):
        if lastSum is None:
            lastSum = 0
        return sum(newValues, lastSum)
        
    # Function | Print Output
    def writeToFile(time, rdd):
        arr = rdd.collect()
        arr.sort(key=lambda tup: tup[1], reverse=True)
        #arr_sorted = sorted(arr, key=lambda tup: tup[1])
        print("\n Top 10 Hastags: ")
        if len(arr)> 0:
            for i in range(0, 10):
                print(arr[i])
        
        return 0
     
    # Initialize Stream Object     
    lines = ssc.textFileStream("file:///home/nitin/projects/kafka/data/spark")
    
    # Parse Stream Data
    word_count = lines.flatMap(lambda line: line.split(" ")).filter(lambda w: w.startswith("#")).map(lambda word: (word, 1)).updateStateByKey(countWords)
    
    # Process DStream RDD's data
    word_count.foreachRDD(writeToFile)
    
    # Start Process
    ssc.start()
    
    #s = sched.scheduler(time.time, time.sleep)
    #s.enter(10, 1, print_details, (s))
    #s.run()
    
    # Wait for Process Termination
    ssc.awaitTermination()
