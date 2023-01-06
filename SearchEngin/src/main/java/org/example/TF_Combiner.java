package org.example;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
public class TF_Combiner extends Reducer<Text, Text, Text, Text>
{
    private final Text fileAtWordFreqValue = new Text();

    @Override
    protected void reduce(Text key, java.lang.Iterable<Text> values,Context context) throws IOException ,InterruptedException {
        int sum = 0;
        for(Text value:values) {
            sum ++;
        }
        // split[0]=url,split[1]=number of total words,split[2]=word
        int indx1 = key.toString().indexOf("-");
        int indx2=key.toString().indexOf("@");
        // tf-->term frequency(word)=(wordfrequency in page)/(total number of words in page)
        double tf=(sum*1.0)/Double.parseDouble(key.toString().substring(indx1+1,indx2));
        fileAtWordFreqValue.set(String.valueOf(tf));  // word :tf
        key.set(key.toString().substring(0,indx1)+"@"+key.toString().substring(indx2+1,key.toString().length())); //word
        context.write(key, fileAtWordFreqValue);  // <word filename:3>
    }
}

/* Combiner
input:
    List of map<string,string>(url-totalwordinurl@word,1>
output:
map
  key-->url@word
  value-->tf
  <url@word,tf>
*/