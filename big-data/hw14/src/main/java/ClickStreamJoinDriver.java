import Broadcast.BroadcastMapper;
import Broadcast.BroadcastReducer;
import Prepare.LongTextWritable;
import Prepare.PrepareComparator;
import Prepare.PrepareMapper;
import Prepare.PrepareReducer;
import ReduceSide.ReduceSideMapper;
import ReduceSide.ReduceSideReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class ClickStreamJoinDriver extends Configured implements Tool {

    public static void main(String[] args) throws Exception {

        int res = ToolRunner.run(new Configuration(), new ClickStreamJoinDriver(), args);
        System.exit(res);
    }


    @Override
    public int run(String[] args)
            throws Exception {


        Configuration conf = getConf();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 1) {
            System.err.println("Please, specify args");
            System.exit(2);
        }


        Integer mode = null;
        try {
            mode = Integer.parseInt(otherArgs[0]);
        } catch (NumberFormatException e) {
            System.err.println("Mode not valid");
            System.exit(3);
        }

        switch (mode) {
            case 1:
                return RunPrepare(conf, otherArgs);
            case 2:
                return RunBroadcastJoin(conf, otherArgs);
            case 3:
                return RunReduceSideJoin(conf, otherArgs);
            case 4:
                return RunCollect(conf, otherArgs);
            default:
                throw new Exception("Unrecognized mode");
        }
    }

    private int RunPrepare(Configuration conf, String[] otherArgs)
            throws IOException, ClassNotFoundException, InterruptedException {

        Job job = Job.getInstance(conf);

        job.setNumReduceTasks(1);
        job.setJobName("PrepareFirstMonth");
        job.setJarByClass(ClickStreamJoinDriver.class);

        job.setMapOutputKeyClass(LongTextWritable.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        job.setMapperClass(PrepareMapper.class);
        job.setReducerClass(PrepareReducer.class);
        job.setSortComparatorClass(PrepareComparator.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    private int RunBroadcastJoin(Configuration conf, String[] otherArgs)
            throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {

        conf.set("top10k", new Path(otherArgs[1]).getName());

        Job job = Job.getInstance(conf);

        job.setNumReduceTasks(3);
        job.setJobName("BroadcastJoin");
        job.setJarByClass(ClickStreamJoinDriver.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        job.setMapperClass(BroadcastMapper.class);
        job.setReducerClass(BroadcastReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.addCacheFile(new URI(otherArgs[1]));
        FileInputFormat.addInputPath(job, new Path(otherArgs[2]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[3]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    private int RunReduceSideJoin(Configuration conf, String[] otherArgs)
            throws IOException, ClassNotFoundException, InterruptedException {

        conf.set("second_file", new Path(otherArgs[2]).getName());
        Job job = Job.getInstance(conf);

        job.setNumReduceTasks(3);
        job.setJobName("ReduceSideJoin");
        job.setJarByClass(ClickStreamJoinDriver.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongTextWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        job.setMapperClass(ReduceSideMapper.class);
        job.setReducerClass(ReduceSideReducer.class);

        job.setOutputFormatClass(TextOutputFormat.class);

        for (Integer i = 1; i < otherArgs.length - 1; i++) {
            MultipleInputs.addInputPath(job, new Path(otherArgs[i]), TextInputFormat.class);
        }

        FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    private int RunCollect(Configuration conf, String[] otherArgs)
            throws IOException, ClassNotFoundException, InterruptedException {

        Job job = Job.getInstance(conf);

        job.setNumReduceTasks(1);
        job.setJobName("Collect");
        job.setJarByClass(ClickStreamJoinDriver.class);

        job.setMapOutputKeyClass(LongTextWritable.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        job.setMapperClass(PrepareMapper.class);
        job.setReducerClass(PrepareReducer.class);
        job.setSortComparatorClass(PrepareComparator.class);

        job.setOutputFormatClass(TextOutputFormat.class);

        FileSystem fs = FileSystem.get(conf);

        RemoteIterator<LocatedFileStatus> fileStatusListIterator = fs.listFiles(new Path(otherArgs[1]), false);
        while(fileStatusListIterator.hasNext()){
            LocatedFileStatus fileStatus = fileStatusListIterator.next();
            if (!fileStatus.isFile() || !fileStatus.getPath().getName().matches("part-r-\\d+"))
                continue;

            MultipleInputs.addInputPath(job, fileStatus.getPath(), TextInputFormat.class);
        }

        FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
