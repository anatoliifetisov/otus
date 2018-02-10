import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TextPair implements WritableComparable<TextPair> {

    private Text txt1 = new Text();
    private Text txt2 = new Text();

    public TextPair(){

    }

    public TextPair(String txt1, String txt2) {
        this.set(txt1, txt2);
    }

    public String getText1() {
        return txt1.toString();
    }

    public String getText2() {
        return txt2.toString();
    }

    public void set(String txt1, String txt2) {
        boolean swap = txt1.compareTo(txt2) > 0;
        this.txt1.set(swap ? txt2 : txt1);
        this.txt2.set(swap ? txt1 : txt2);
    }


    public int compareTo(TextPair o) {
        int comp = getText1().compareTo(o.getText1());
        if (comp == 0) {
            comp = getText2().compareTo(o.getText2());
        }
        return comp;
    }

    public void write(DataOutput dataOutput) throws IOException {
        txt1.write(dataOutput);
        txt2.write(dataOutput);
    }

    public void readFields(DataInput dataInput) throws IOException {
        txt1.readFields(dataInput);
        txt2.readFields(dataInput);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TextPair that = (TextPair) o;

        return getText1().equals(that.getText1()) && getText2().equals(that.getText2());
    }

    @Override
    public int hashCode() {
        int result = getText1().hashCode();
        result = 31 * result + getText2().hashCode();
        return result;
    }

    @Override
    public String toString() {
        return getText1() + " " + getText2();
    }
}
