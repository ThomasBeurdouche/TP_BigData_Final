package org.epf.hadoop.colfil2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class UserPairMapper extends Mapper<LongWritable, Text, UserPair, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //User\t  realtion1,relation2....
        String[] parts = value.toString().split("\\t");
        String user = parts[0];
        String[] relations = parts[1].split(",");

        for (String relation : relations) {
            UserPair userPair = new UserPair(user, relation);
            IntWritable relationType = new IntWritable(0);
            context.write(userPair, relationType);
        }

        for (int i = 0; i < relations.length; i++) {
            for (int j = i + 1; j < relations.length; j++) {
                UserPair userPair = new UserPair(relations[i], relations[j]);
                IntWritable relationType = new IntWritable(1);
                context.write(userPair, relationType);
            }
        }
    }
}
