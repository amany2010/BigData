package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TFIDF_Driver
{
    public static void main(String[] args) throws Exception {

        if (args.length != 2) {
            System.err.println("Usage IndexDriver <input_dir> <output_dir>");
            System.exit(2);
        }

        String input = args[0];
        String output = args[1];


        Configuration conf = new Configuration();
        conf.set("totalPages", String.valueOf(50));

        Job job = new Job(conf);
        FileSystem fs = FileSystem.get(conf);

        // To avoid output error (using  the same directory).
        boolean exists = fs.exists(new Path(output));
        if(exists) {
            fs.delete(new Path(output), true);
        }
        job.setJarByClass(TFIDF_Driver.class);
        job.setMapperClass(TFIDF_Mapper.class);
        job.setCombinerClass(TFIDF_Combiner.class);
        job.setReducerClass(TFIDF_Reducer.class);


        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        System.exit(job.waitForCompletion(true)?0:1);


    }
}
