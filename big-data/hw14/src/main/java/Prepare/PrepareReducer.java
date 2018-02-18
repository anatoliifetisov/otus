package Prepare;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class PrepareReducer extends Reducer<LongTextWritable, IntWritable, Text, LongWritable> {

    private Text outKey = new Text();
    private LongWritable outValue = new LongWritable();

    @Override
    public void run(Context context) throws IOException, InterruptedException {
        this.setup(context);

        try {
            Integer i = 0;
            while (context.nextKey() && i < 10000) {
                LongTextWritable key = context.getCurrentKey();
                outKey.set(key.getText());
                outValue.set(key.getLong());

                context.write(outKey, outValue);

                i += 1;
            }
        }
        finally {
            this.cleanup(context);
        }
    }
}
