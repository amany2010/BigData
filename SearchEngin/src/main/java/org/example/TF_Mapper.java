package org.example;
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.yarn.util.StringHelper;

import java.util.HashMap;
import java.util.Map;
public class TF_Mapper extends  Mapper<LongWritable, Text, Text, Text>
{
    private Text keyinfo = new Text();
    private Text valueinfo = new Text();

    private FileSplit split;

    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {


        this.split = (FileSplit)context.getInputSplit();
        StringTokenizer tokenizer = new StringTokenizer(value.toString());
        //Count number of word in page file (decrement by 1 becouse page url is  the first taken
        String totalWordas=String.valueOf(tokenizer.countTokens()-1);
        String url = tokenizer.nextToken();
        while(tokenizer.hasMoreTokens())
         {
             // igonre special charchter from word
             String word=tokenizer.nextToken().replaceAll("('s)|('S)[^a-zA-z0-9]","");
             word.toLowerCase();
             this.keyinfo.set(url+"-"+totalWordas+"@"+word);
             this.valueinfo.set("1");
             context.write(this.keyinfo,this.valueinfo);
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