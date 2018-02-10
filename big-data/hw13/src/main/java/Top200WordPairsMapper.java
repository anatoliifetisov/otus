import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Top200WordPairsMapper extends Mapper<Object, Text, TextLongWritablePair, IntWritable> {

    private TextLongWritablePair outputKey = new TextLongWritablePair();
    private IntWritable zero = new IntWritable(0);

    @Override
    protected void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        String[] split = value.toString().split("\\s+");

        outputKey.set(split[0], split[1], Long.parseLong(split[2]));
        context.write(outputKey, zero);
    }
}
