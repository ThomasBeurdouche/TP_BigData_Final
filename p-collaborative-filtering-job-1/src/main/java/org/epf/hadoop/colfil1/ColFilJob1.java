package org.epf.hadoop.colfil1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;

public class ColFilJob1 {
    public static void main(String[]args) throws IOException, InterruptedException, ClassNotFoundException{

        if(args.length != 2){
            for (int index = 0; index < args.length; ++index)
            {
                System.out.println("args[" + index + "]: " + args[index]);
            }
            System.err.println("Invalid command");
            System.err.println("Usage: ColFilJob1 <input path> <output path>");
            System.exit(0);
        }

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "ColFilJob1");
        job.setJarByClass(ColFilJob1.class);
        job.setNumReduceTasks(2);

        job.setMapperClass(RelationshipMapper.class);
        job.setReducerClass(RelationshipReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(RelationshipInputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}