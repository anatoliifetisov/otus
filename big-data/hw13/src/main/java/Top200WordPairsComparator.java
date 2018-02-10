import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class Top200WordPairsComparator extends WritableComparator {

    public Top200WordPairsComparator() {
        super(TextLongWritablePair.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        TextLongWritablePair tlwp1 = (TextLongWritablePair)a;
        TextLongWritablePair tlwp2 = (TextLongWritablePair)b;

        int comp = tlwp1.getLong().compareTo(tlwp2.getLong());
        if (comp == 0) {
            return tlwp1.compareTo(tlwp2);
        }
        return -comp;
    }
}
