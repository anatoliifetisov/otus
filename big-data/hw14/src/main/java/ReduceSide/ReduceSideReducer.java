package ReduceSide;

import Prepare.LongTextWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ReduceSideReducer extends Reducer<Text, LongTextWritable, Text, LongWritable> {

    private Text outKey = new Text();
    private LongWritable outValue = new LongWritable();

    private String secondFileName = null;

    @Override
    protected void setup(Context context)
            throws IOException, InterruptedException {

        Configuration conf = context.getConfiguration();
        secondFileName = conf.get("second_file");

        super.setup(context);
    }

    @Override
    protected void reduce(Text key, Iterable<LongTextWritable> values, Context context)
            throws IOException, InterruptedException {

        String prev = null;
        Long prevCount = 0L;
        for (LongTextWritable value : values) {
            if (prev == null) {
                prev = value.getText();
                prevCount = value.getLong();
            } else {
                String curr = value.getText();

                if (prev.equals(curr)) { // only happens if we have > 1 entry per file
                    break;
                }

                outKey.set(key);

                if (curr.equals(secondFileName)) {
                    outValue.set(value.getLong());
                } else {
                    outValue.set(prevCount);
                }

                context.write(outKey, outValue);
                break;
            }
        }
    }
}
