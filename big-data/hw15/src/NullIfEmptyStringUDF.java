import org.apache.hadoop.hive.ql.exec.UDF;

import java.text.ParseException;

public class NullIfEmptyStringUDF extends UDF {

    public String evaluate(String str) throws ParseException {
        if ("".equals(str)){
            return null;
        }
        return str;
    }
}