import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TotalWordPairsReducer extends Reducer<IntWritable, LongWritable, Text, LongWritable> {

    @Override
    protected void reduce(IntWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

        Long total = 0L;
        for (LongWritable value : values) {
            total += value.get();
        }

        context.write(new Text("Total pairs: "), new LongWritable(total));
    }
}
