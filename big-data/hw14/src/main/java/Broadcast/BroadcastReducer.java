package Broadcast;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class BroadcastReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

}
