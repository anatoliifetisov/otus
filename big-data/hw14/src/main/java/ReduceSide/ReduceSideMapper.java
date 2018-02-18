package ReduceSide;

import Prepare.LongTextWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class ReduceSideMapper extends Mapper<Object, Text, Text, LongTextWritable> {

    private Text outKey = new Text();
    private LongTextWritable outValue = new LongTextWritable();

    private String fileName = null;


    @Override
    protected void setup(Context context)
            throws IOException, InterruptedException {

        try {
            fileName = getFileName(context.getInputSplit());
        } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
            e.printStackTrace();
        }

        super.setup(context);
    }

    private String getFileName(InputSplit is)
            throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {

        Class<? extends InputSplit> cl = is.getClass();

        if (cl.getName().equals("org.apache.hadoop.mapreduce.lib.input.TaggedInputSplit")) {
            Method getInputSplitMethod = cl.getDeclaredMethod("getInputSplit");
            getInputSplitMethod.setAccessible(true);
            FileSplit fileSplit = (FileSplit) getInputSplitMethod.invoke(is);
            return fileSplit.getPath().getName();
        }
        else {
            return "";
        }
    }

    @Override
    protected void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        String[] split = value.toString().split("\\t");
        String pair = String.format("%s\t%s", split[0].trim(), split[1].trim());
        Long count = Long.parseLong(split[split.length - 1].trim());

        outKey.set(pair);
        outValue.set(count, fileName);

        context.write(outKey, outValue);

    }
}
