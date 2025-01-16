docker run --rm -p 8088:8088 -p 9870:9870 -p 9864:9864 -v "C:/Users/latri/IdeaProjects/hadoop-tp3/jars:/tmp/jars" -v "C:/Users/latri/IdeaProjects/hadoop-tp3/data:/tmp/data" --name hadoop-tp3-cont hadoop-tp3-img
docker exec -it hadoop-tp3-cont su - epfuser

hadoop fs -put /tmp/data /
hadoop jar /tmp/jars/hadoop-tp3-collaborativeFiltering-job1-1.0.jar /data/relationships/data.txt /data/outputJob1/
hadoop jar /tmp/jars/hadoop-tp3-collaborativeFiltering-job2-1.0.jar /data/outputJob1/ /data/outputJob2/
hadoop jar /tmp/jars/hadoop-tp3-collaborativeFiltering-job3-1.0.jar /data/outputJob2/ /data/outputJob3/

hadoop fs -cat /data/outputJob3/part-r-00000 | head -n 1

hadoop fs -rm -r /data/outputJob3
