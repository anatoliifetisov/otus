import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

import java.io.IOException;
import java.util.*;

public class SentenceInputFormat extends FileInputFormat<IntWritable, Text> {

    public RecordReader<IntWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) {
        return new SentenceInputFormatClass();
    }

    public class SentenceInputFormatClass extends RecordReader<IntWritable, Text> {
        private LineRecordReader lineRecordReader = null;
        private IntWritable key = new IntWritable();
        private Text value = new Text();

        private Integer sentenceNumber = 0;
        private String sentenceStart = null;
        private Queue<String> remainingSentences = new LinkedList<>();

        private HashSet<Character> sentenceEnds = new HashSet<>(Arrays.asList('?', '!', '.'));

        @Override
        public void close() throws IOException {
            if (null != lineRecordReader) {
                lineRecordReader.close();
                lineRecordReader = null;
            }
            key = null;
            value = null;
        }

        @Override
        public IntWritable getCurrentKey() {
            return key;
        }

        @Override
        public Text getCurrentValue() {
            return value;
        }

        @Override
        public float getProgress()
                throws IOException {
            return lineRecordReader.getProgress();
        }

        @Override
        public void initialize(InputSplit split, TaskAttemptContext context)
                throws IOException {
            close();

            lineRecordReader = new LineRecordReader();
            lineRecordReader.initialize(split, context);
            key = new IntWritable();
            value = new Text();
        }

        @Override
        public boolean nextKeyValue()
                throws IOException {

            while (true) {
                if (!remainingSentences.isEmpty()) {
                    String sentence = remainingSentences.remove();
                    key.set(sentenceNumber);
                    value.set(sentence);
                    sentenceNumber += 1;
                    return true;
                }

                if (!lineRecordReader.nextKeyValue()) {
                    key = null;
                    value = null;
                    return false;
                }

                Text line = lineRecordReader.getCurrentValue();
                String str = line.toString();

                if (str.equals("")) { // that usually means paragraph break
                    if (sentenceStart != null) {
                        remainingSentences.add(sentenceStart);
                        sentenceStart = null;
                    }
                    continue;
                }

                String[] split = str.split("[.?!]+");
                if (sentenceStart == null) {
                    if (split.length == 1) {
                        if (sentenceEnds.contains(str.charAt(str.length() - 1))) {
                            remainingSentences.add(split[0]);
                        }
                        else {
                            sentenceStart = String.join(" ", split);
                        }
                    }
                    else {
                        remainingSentences.addAll(Arrays.asList(split).subList(0, split.length - 1));

                        String lastPart = split[split.length - 1];
                        if (sentenceEnds.contains(str.charAt(str.length() - 1))) {
                            remainingSentences.add(lastPart);
                        }
                        else {
                            sentenceStart = split[split.length - 1];
                        }
                    }
                }
                else {
                    if (split.length == 1) {
                        if (sentenceEnds.contains(str.charAt(str.length() - 1))) {
                            remainingSentences.add(sentenceStart + " " + split[split.length - 1]);
                            sentenceStart = null;
                        }
                        else {
                            sentenceStart += " " + split[0];
                        }
                    }
                    else {
                        remainingSentences.add(sentenceStart + " " + split[0]);
                        sentenceStart = null;

                        if (sentenceEnds.contains(str.charAt(str.length() - 1))) {
                            remainingSentences.addAll(Arrays.asList(split).subList(1, split.length));
                        }
                        else {
                            sentenceStart = split[split.length - 1];
                            remainingSentences.addAll(Arrays.asList(split).subList(1, split.length - 1));
                        }
                    }
                }
            }
        }

    }
}
