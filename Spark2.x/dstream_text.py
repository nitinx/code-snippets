# Design Pattern | Spark DStream | Text
# Count words received via a text stream (files)
# To Execute: spark-submit dstream_text.py

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
        
    # Initialize Stream Object    
    lines = ssc.textFileStream("file:///home/nitin/projects/kafka/data/spark")
    
    # Parse Stream Data    
    word_count = lines.flatMap(lambda line: line.split(" ")).filter(lambda w: w.startswith("#")).map(lambda word: (word, 1)).updateStateByKey(countWords)
    
    # Print first ten elements of each DStream RDD
    word_count.pprint(num=10)    
        
    # Start Process
    ssc.start()
    
    #s = sched.scheduler(time.time, time.sleep)
    #s.enter(10, 1, print_details, (s))
    #s.run()
    
    # Wait for Process Termination
    ssc.awaitTermination()
