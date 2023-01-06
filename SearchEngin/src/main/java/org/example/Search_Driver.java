package org.example;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import java.sql.Driver;
import java.util.Scanner;

public class Search_Driver
{
    public static void main(String[] args) throws Exception {

        if (args.length != 2) {
            System.err.println("Usage IndexDriver <input_dir> <output_dir>");
            System.exit(2);
        }
        Scanner sc=new Scanner(System.in);
        System.out.println("Enter Word To Search:");
        String word=sc.nextLine();
        Configuration conf = new Configuration();
        conf.set("word", word);

        Job job = new Job(conf);
        String input = args[0];
        String output = args[1];
        FileSystem fs = FileSystem.get(conf);

        // To avoid output error (using  the same directory).
        boolean exists = fs.exists(new Path(output));
        if(exists) {
            fs.delete(new Path(output), true);
        }


        job.setJarByClass(Search_Driver.class);
        job.setMapperClass(Search_Mapper.class);
        job.setCombinerClass(Search_Reducer.class);
        job.setReducerClass(Search_Reducer.class);


        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        System.exit(job.waitForCompletion(true)?0:1);


    }
}
