package org.example;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
public class TFIDF_Combiner extends Reducer<Text, Text, Text, Text>
{
    private final Text fileAtWordFreqValue = new Text();
    @Override
    protected void reduce(Text key, Iterable<Text> values,Context context) throws IOException ,InterruptedException {

       String x= key.toString().replaceAll("[^a-zA-Z]+","");
        if (!x.isEmpty())
        {
            x=x.toLowerCase();
            StringBuilder filelist = new StringBuilder("");
            for(Text value:values) {
                //ur=tf
                filelist.append(value.toString()).append(";");
            }

            fileAtWordFreqValue.set(filelist.toString());
                context.write(new Text(x), fileAtWordFreqValue);
        }


    }
}

/* Combiner
input:
    Map<word,url=tf>
output:
map
  Map<word,list(url=tf)>
*/