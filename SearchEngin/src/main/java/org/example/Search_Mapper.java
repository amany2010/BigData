package org.example;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.yarn.util.StringHelper;

import java.util.HashMap;
import java.util.Map;
public class Search_Mapper extends  Mapper<LongWritable, Text, Text, Text>
{
    private Text keyinfo = new Text();
    private Text valueinfo = new Text();

    private FileSplit split;

    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {

        Configuration conf = context.getConfiguration();
        String word=conf.get("word").toString();
        word=word.toLowerCase();
        this.split = (FileSplit)context.getInputSplit();
        StringTokenizer tokenizer = new StringTokenizer(value.toString());
        while(tokenizer.hasMoreTokens())
        {
            String s=tokenizer.nextToken();
            int indx=s.indexOf('*');
            if(indx!=-1&&word.equals(s.substring(0,indx)))
            {
                System.out.println(s);
                System.out.println(word);
                this.keyinfo.set(tokenizer.nextToken());
                this.valueinfo.set(s.substring(indx+1,s.length()));
                context.write(this.keyinfo,this.valueinfo);
            }

        }
    }
}
/* Mapper
input:
    files
outputs:
    key: string -->url+'-'+totalwordsnumber+word
    value :count 1
    <url-totalword@word,1>
 */