import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordPairsCountReducer extends Reducer<TextPair, LongWritable, TextPair, LongWritable> {

    private TextPair outputKey = new TextPair();
    private LongWritable outputValue = new LongWritable();

    @Override
    protected void reduce(TextPair key, Iterable<LongWritable> values, Context context)
            throws IOException, InterruptedException {

        Long count = 0L;
        for (LongWritable value: values) {
            count += value.get();
        }

        outputKey.set(key.getText1(), key.getText2());
        outputValue.set(count);

        context.write(outputKey, outputValue);
    }
}
