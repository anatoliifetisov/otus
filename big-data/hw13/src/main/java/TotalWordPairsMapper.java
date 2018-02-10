import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TotalWordPairsMapper extends Mapper<Object, Text, IntWritable, LongWritable> {

    private IntWritable zero = new IntWritable(0);

    @Override
    public void run(Context context) throws IOException, InterruptedException {
        this.setup(context);

        Long total = 0L;
        try {
            while (context.nextKeyValue()) {
                total += Long.parseLong(context.getCurrentValue().toString().split("\\t")[1]);
            }
            context.write(zero, new LongWritable(total));
        } finally {
            this.cleanup(context);
        }

    }
}
