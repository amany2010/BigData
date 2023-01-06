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
public class TFIDF_Mapper extends  Mapper<LongWritable, Text, Text, Text>
{
    private Text keyinfo = new Text();
    private Text valueinfo = new Text();

    private FileSplit split;

    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {


        this.split = (FileSplit)context.getInputSplit();
        StringTokenizer tokenizer = new StringTokenizer(value.toString());
        while(tokenizer.hasMoreTokens())
        {
            // s=url@word*tf
            String s=tokenizer.nextToken()+'*'+tokenizer.nextToken();
            // start word index
            int indx1=s.indexOf('@');
            //start tf index
            int indx2=s.indexOf('*');
            //key=word
            this.keyinfo.set(s.substring(indx1+1,indx2));
            //value=url=tf
            this.valueinfo.set(s.substring(0,indx1)+'='+s.substring(indx2+1,s.length()));
            context.write(this.keyinfo,this.valueinfo);
        }
    }
}
/*MAppeer OUtput
*  Map<word,url=tf>
* */