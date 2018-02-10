import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class WordPairsCountMapper extends Mapper<Object, Text, TextPair, LongWritable> {

    private TextPair outputKey = new TextPair();
    private LongWritable one = new LongWritable(1);

    private static final Pattern notWords = Pattern.compile("\\W+");
    private static final Pattern digits = Pattern.compile("\\d+");

    @Override
    protected void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        Matcher notWordsMatcher = notWords.matcher(value.toString().toLowerCase().trim());
        String s = notWordsMatcher.replaceAll(" ");

        Matcher digitsMatcher = digits.matcher(s);
        s = digitsMatcher.replaceAll("");

        String[] words = s.split("\\s+");
        for (int i = 0; i < words.length - 1; i++) {
            String w1 = words[i];

            if (StopWords.Words.contains(w1) || w1.length() < 3 || w1.equals(""))
                continue;

            for (int j = i + 1; j < words.length; j++) {
                String w2 = words[j];
                if (w1.equals(w2) || StopWords.Words.contains(w2) || w2.length() < 3 || w2.equals(""))
                    continue;

                outputKey.set(w1, w2);

                context.write(outputKey, one);
            }
        }
    }
}


