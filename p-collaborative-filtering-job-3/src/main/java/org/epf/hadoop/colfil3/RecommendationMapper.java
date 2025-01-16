package org.epf.hadoop.colfil3;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class RecommendationMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().split("\t");
        String[] users = line[0].split(",");
        int commonCount = Integer.parseInt(line[1]);

        Text userKey = new Text(users[0]);
        Text recommendationValue = new Text(users[1] + "," + commonCount);
        context.write(userKey, recommendationValue);

        userKey.set(users[1]);
        recommendationValue.set(users[0] + "," + commonCount);
        context.write(userKey, recommendationValue);
    }
}
