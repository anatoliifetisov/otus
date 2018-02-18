package Broadcast;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

public class BroadcastMapper extends Mapper<Object, Text, Text, LongWritable> {

    private static HashMap<String, Long> cached = new HashMap<>();

    private Text outKey = new Text();
    private LongWritable outValue = new LongWritable();

    @Override
    protected void setup(Context context)
            throws IOException, InterruptedException {

        String cacheFileName = context.getConfiguration().get("top10k");

        URI[] cachedFiles = context.getCacheFiles();
        for (URI path : cachedFiles) {
            if (path.toString().trim().endsWith(cacheFileName)) {
                loadFirstMonth(path, context);
            }
        }

        super.setup(context);
    }

    private void loadFirstMonth(URI path, Context context) throws IOException {
        String line;
        BufferedReader br = null;
        try {
            br = new BufferedReader(new InputStreamReader(FileSystem.get(context.getConfiguration()).open(new Path(path))));
            line = br.readLine();
            while (line != null) {
                String[] split = line.split("\\t");
                String pair = String.format("%s\t%s", split[0].trim(), split[1].trim());
                Long count = Long.parseLong(split[2].trim());
                cached.put(pair, count);
                line = br.readLine();
            }
        } finally {
            if (br != null) {
                br.close();
            }
        }
    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        String[] split = value.toString().split("\\t");
        String pair = String.format("%s\t%s", split[0].trim(), split[1].trim());
        Long count = Long.parseLong(split[3].trim());

        if (cached.containsKey(pair)) {
            outKey.set(pair);
            outValue.set(count);
            context.write(outKey, outValue);
        }
    }
}
