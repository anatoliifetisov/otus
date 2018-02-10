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

public class WordPairsDriver extends Configured implements Tool {

    public static void main(String[] args) throws Exception {

        int res = ToolRunner.run(new Configuration(), new WordPairsDriver(), args);
        System.exit(res);
    }

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
                return RunPairsCount(conf, otherArgs);
            case 2:
                return RunTop200Pairs(conf, otherArgs);
            case 3:
                return RunTotalPairsCount(conf, otherArgs);
            default:
                throw new Exception("Unrecognized mode");
        }
    }

    private int RunPairsCount(Configuration conf, String[] otherArgs)
            throws IOException, ClassNotFoundException, InterruptedException {

        if (otherArgs.length != 3) {
            System.err.println("Please, specify exactly three parameters: 1 <in-file> <out-dir>");
            System.exit(4);
        }

        conf.set("textinputformat.record.delimiter", ".");

        Job job = Job.getInstance(conf);

        job.setNumReduceTasks(3);
        job.setJobName("WordPairsStats");
        job.setJarByClass(WordPairsDriver.class);

        job.setMapOutputKeyClass(TextPair.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setOutputKeyClass(TextPair.class);
        job.setOutputValueClass(LongWritable.class);

        job.setMapperClass(WordPairsCountMapper.class);
        job.setReducerClass(WordPairsCountReducer.class);
        job.setCombinerClass(WordPairsCountReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    private int RunTop200Pairs(Configuration conf, String[] otherArgs)
            throws IOException, ClassNotFoundException, InterruptedException {

        if (otherArgs.length != 3) {
            System.err.println("Please, specify exactly three parameters: 2 <in-dir> <out-dir>");
            System.exit(5);
        }

        Job job = Job.getInstance(conf);

        job.setJobName("Top200WordPairs");
        job.setJarByClass(WordPairsDriver.class);

        job.setMapOutputKeyClass(TextLongWritablePair.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        job.setMapperClass(Top200WordPairsMapper.class);
        job.setReducerClass(Top200WordPairsReducer.class);
        job.setSortComparatorClass(Top200WordPairsComparator.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileSystem fs = FileSystem.get(conf);

        RemoteIterator<LocatedFileStatus> fileStatusListIterator = fs.listFiles(new Path(otherArgs[1]), false);
        while(fileStatusListIterator.hasNext()){
            LocatedFileStatus fileStatus = fileStatusListIterator.next();
            if (!fileStatus.isFile() || !fileStatus.getPath().getName().matches("part-r-\\d+"))
                continue;

            MultipleInputs.addInputPath(job, fileStatus.getPath(), TextInputFormat.class, Top200WordPairsMapper.class);
        }

        FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    private int RunTotalPairsCount(Configuration conf, String[] otherArgs)
            throws IOException, ClassNotFoundException, InterruptedException {

        if (otherArgs.length != 3) {
            System.err.println("Please, specify exactly three parameters: 3 <in-dir> <out-dir>");
            System.exit(6);
        }

        Job job = Job.getInstance(conf);

        job.setJobName("TotalWordPairsJob");
        job.setJarByClass(WordPairsDriver.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        job.setMapperClass(TotalWordPairsMapper.class);
        job.setReducerClass(TotalWordPairsReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileSystem fs = FileSystem.get(conf);

        RemoteIterator<LocatedFileStatus> fileStatusListIterator = fs.listFiles(new Path(otherArgs[1]), false);
        while(fileStatusListIterator.hasNext()){
            LocatedFileStatus fileStatus = fileStatusListIterator.next();
            if (!fileStatus.isFile() || !fileStatus.getPath().getName().matches("part-r-\\d+"))
                continue;

            MultipleInputs.addInputPath(job, fileStatus.getPath(), TextInputFormat.class, TotalWordPairsMapper.class);
        }

        FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
