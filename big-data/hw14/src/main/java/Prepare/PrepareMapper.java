package Prepare;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class PrepareMapper extends Mapper<Object, Text, LongTextWritable, IntWritable> {

    private LongTextWritable outKey = new LongTextWritable();
    private IntWritable zero = new IntWritable(0);

    @Override
    protected void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        String[] split = value.toString().split("\\t");
        String pair = String.format("%s\t%s", split[0], split[1]);
        Long count = Long.parseLong(split[split.length - 1]);

        outKey.set(count, pair);

        context.write(outKey, zero);
    }
}
