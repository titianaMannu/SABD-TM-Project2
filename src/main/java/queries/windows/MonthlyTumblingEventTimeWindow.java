package queries.windows;

import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowStagger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import java.util.*;

/**
 * This class extends a TumblingEventTimeWindow.
 * Explicit declaration of the start date which is aligned with the first day of a month based on the event time.
 * Explicit declaration of the end date which is aligned with the end of a month based on the event time.
 */
public class MonthlyTumblingEventTimeWindow extends TumblingEventTimeWindows {
    //default constructor
    public MonthlyTumblingEventTimeWindow(long size, long offset, WindowStagger windowStagger) {
        super(size, offset, windowStagger);
    }

    @Override
    public Collection<TimeWindow> assignWindows(Object element, long timestamp, WindowAssignerContext context) {
        Calendar calendar = new GregorianCalendar(Locale.US);
        calendar.setTimeInMillis(timestamp);

        calendar.set(Calendar.DAY_OF_MONTH, 1);
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        // first day of month at 00:00:00.000...
        long start = calendar.getTimeInMillis();

        calendar.add(Calendar.MONTH, 1);
        // last day of month at 23:59:59.999...
        long end = calendar.getTimeInMillis() - 1;
        return Collections.singletonList(new TimeWindow(start, end));
    }
}
