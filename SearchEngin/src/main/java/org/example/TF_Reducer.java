package org.example;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TF_Reducer extends  Reducer<Text, Text, Text, Text>
{
    private final Text allFilesConcatValue = new Text();
    @Override
    protected void reduce(Text key, Iterable<Text> values,
                          Context context) throws java.io.IOException ,InterruptedException {
        //string builder enable append to string without create new string(optimize memory)
        StringBuilder filelist = new StringBuilder("");
        for(Text value:values) {
            context.write(key, value);
        }

        allFilesConcatValue.set(filelist.toString());

    };
}
/* Reducer
input:
  key-->url@word
  value-->tf
  <url@word,lis[tf]>
output:
    <url@word,tf>
*/