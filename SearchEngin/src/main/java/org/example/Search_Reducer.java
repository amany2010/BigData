package org.example;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Search_Reducer extends  Reducer<Text, Text, Text, Text>
{
    private final Text allFilesConcatValue = new Text();
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws java.io.IOException ,InterruptedException {
        for(Text value:values) {
            context.write(key,value);
        }
    };
}
/* Reducer
input:
    map<string,lis[tf]>(<url@word,list[tf]>)
output:
    map<string,double>(<url@word,tf>)
*/