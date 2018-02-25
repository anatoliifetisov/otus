import org.apache.hadoop.hive.ql.exec.UDF;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;
import java.util.regex.Pattern;

import static java.util.Calendar.*;

public class BdateToAgeUDF extends UDF {

    private static final Pattern date = Pattern.compile("\\d{1,2}\\.\\d{1,2}\\.\\d{4}");

    public Integer evaluate(String bdate) throws ParseException {
        if (bdate == null) {
            return null;
        }

        SimpleDateFormat dateFormat = new SimpleDateFormat("dd.MM.yyyy");

        if (date.matcher(bdate).find()) {
            if (bdate.regionMatches(0, "29.2", 0, 4)) {
                return getDiffYears(dateFormat.parse("28.2." + bdate.substring(5)), new Date());
            }
            return getDiffYears(new SimpleDateFormat("dd.MM.yyyy").parse(bdate), new Date());
        }

        return null;
    }

    private static int getDiffYears(Date first, Date last) {
        Calendar a = getCalendar(first);
        Calendar b = getCalendar(last);
        int diff = b.get(YEAR) - a.get(YEAR);
        if (a.get(MONTH) > b.get(MONTH) || (a.get(MONTH) == b.get(MONTH) && a.get(DAY_OF_MONTH) > b.get(DAY_OF_MONTH))) {
            diff--;
        }
        return diff;
    }

    private static Calendar getCalendar(Date date) {
        Calendar cal = Calendar.getInstance(Locale.US);
        cal.setTime(date);
        return cal;
    }
}