package project;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.io.StringWriter;

public class SortReducer extends Reducer<SortBeans, NullWritable, StringWriter, NullWritable> {

    int i=0;
    @Override
    protected void reduce(SortBeans key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        i++;
        String s=i+"   "+key.getNum();
        StringWriter stringWriter=new StringWriter();
        stringWriter.write(s);
        context.write(stringWriter,NullWritable.get());
    }
}
