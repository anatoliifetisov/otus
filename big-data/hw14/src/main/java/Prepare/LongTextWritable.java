package Prepare;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class LongTextWritable implements WritableComparable<LongTextWritable> {

    private LongWritable lng = new LongWritable();
    private Text txt = new Text();

    public LongTextWritable(){

    }

    public LongTextWritable(Long lng, String txt) {
        this.set(lng, txt);
    }

    public Long getLong() {
        return lng.get();
    }

    public String getText() {
        return txt.toString();
    }

    public void set(Long lng, String txt) {
        this.lng.set(lng);
        this.txt.set(txt);
    }


    public int compareTo(LongTextWritable o) {
        int comp = getLong().compareTo(o.getLong());
        if (comp == 0) {
            comp = getText().compareTo(o.getText());
        }
        return comp;
    }

    public void write(DataOutput dataOutput) throws IOException {
        lng.write(dataOutput);
        txt.write(dataOutput);
    }

    public void readFields(DataInput dataInput) throws IOException {
        lng.readFields(dataInput);
        txt.readFields(dataInput);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LongTextWritable that = (LongTextWritable) o;

        return getLong().equals(that.getLong()) && getText().equals(that.getText());
    }

    @Override
    public int hashCode() {
        int result = getLong().hashCode();
        result = 31 * result + getText().hashCode();
        return result;
    }

    @Override
    public String toString() {
        return getText() + " " + getLong();
    }
}