package org.epf.hadoop.colfil2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class UserPairReducer extends Reducer<UserPair, IntWritable, UserPair, IntWritable> {

    @Override
    protected void reduce(UserPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        boolean isDirectlyConnected = false;
        int commonConnections = 0;

        for (IntWritable val : values) {
            if (val.get() == 0) {
                isDirectlyConnected = true;
            } else {
                commonConnections++;
            }
        }

        if (!isDirectlyConnected && commonConnections > 0) {
            IntWritable result = new IntWritable(commonConnections);
            context.write(key, result);
        }
    }
}

