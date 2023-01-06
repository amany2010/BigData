package org.example;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.zookeeper.txn.Txn;

import java.util.ArrayList;
import java.util.List;

public class TFIDF_Reducer extends  Reducer<Text, Text, Text, Text>
{

    private final Text allFilesConcatValue = new Text();
    @Override
    protected void reduce(Text key, Iterable<Text> values,Context context) throws java.io.IOException ,InterruptedException {
        Configuration conf = context.getConfiguration();
        int numOfPages= Integer.parseInt(conf.get("totalPages"));
        StringBuilder filelist = new StringBuilder("");
        for(Text value:values)
        {
            List<String> url = new ArrayList<String>();
            int first = 0, indx = 0;
            String s = value.toString();
            while (true) {
                indx = s.indexOf(';',first);
                if(indx==-1)break;
                url.add(s.substring(first, indx));
                first = indx + 1;
            }
            int numOfURL = url.size();
            String x=key.toString() ;
            for (String ss : url)
            {
                //to split url and tf

                int indx2 = ss.indexOf('=');
                double IDF = Math.log10(numOfPages / (1 + numOfURL));
                double TFIDF = IDF * (Double.parseDouble(ss.substring(indx2 + 1, ss.length())));
                key.set(x+'*'+ss.substring(0, indx2));
                filelist.append(TFIDF);
                allFilesConcatValue.set(filelist.toString());

                context.write(key, new Text(String.valueOf(TFIDF)));
            }
        }
    };
}
/* Combiner
input:
    Map<word,list(url=tf)>
output:
    Map<word#url,TFIDF>
*/