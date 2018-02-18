package Prepare;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;


public class PrepareComparator extends WritableComparator {

    public PrepareComparator() {
        super(LongTextWritable.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        LongTextWritable tp1 = (LongTextWritable) a;
        LongTextWritable tp2 = (LongTextWritable)b;

        int cmp = tp1.getLong().compareTo(tp2.getLong());
        if (cmp == 0) {
            return tp1.getText().compareTo(tp2.getText());
        }
        return -cmp;
    }
}
