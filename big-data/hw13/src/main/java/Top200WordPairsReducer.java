import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Top200WordPairsReducer extends Reducer<TextLongWritablePair, IntWritable, Text, LongWritable> {

    private Text outputKey = new Text();
    private LongWritable outputValue = new LongWritable();

    @Override
    public void run(Context context) throws IOException, InterruptedException {
        this.setup(context);

        try {
            for (int i = 0; i < 200; i++) {
                context.nextKey();
                TextLongWritablePair key = context.getCurrentKey();
                outputKey.set(key.getText1() + " " + key.getText2());
                outputValue.set(key.getLong());
                context.write(outputKey, outputValue);
            }
        } finally {
            this.cleanup(context);
        }
    }
}
