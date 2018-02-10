import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TextLongWritablePair implements WritableComparable<TextLongWritablePair> {

    private TextPair txt = new TextPair();
    private LongWritable lng = new LongWritable();

    public TextLongWritablePair() {

    }

    public TextLongWritablePair(String txt1, String txt2, Integer lng) {
        this.txt.set(txt1, txt2);
        this.lng.set(lng);
    }

    public String getText1() {
        return txt.getText1();
    }

    public String getText2() {
        return txt.getText2();
    }

    public Long getLong() {
        return lng.get();
    }

    public void set(String txt1, String txt2, Long lng) {
        this.txt.set(txt1, txt2);
        this.lng.set(lng);
    }

    public void write(DataOutput dataOutput) throws IOException {
        txt.write(dataOutput);
        lng.write(dataOutput);
    }

    public void readFields(DataInput dataInput) throws IOException {
        txt.readFields(dataInput);
        lng.readFields(dataInput);
    }

    public int compareTo(TextLongWritablePair o) {
        int comp = txt.compareTo(o.txt);
        if (comp == 0) {
            comp = getLong().compareTo(o.getLong());
        }
        return comp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TextLongWritablePair that = (TextLongWritablePair) o;

        return txt.equals(that.txt) && getLong().equals(that.getLong());

    }

    @Override
    public int hashCode() {
        int result = txt.hashCode();
        result = 31 * result + getLong().hashCode();
        return result;
    }

    @Override
    public String toString() {
        return txt.toString() + ", " + getLong();
    }
}
