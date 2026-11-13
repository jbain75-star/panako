package be.panako.strategy.panako;

import org.junit.jupiter.api.Test;
import be.panako.util.Config;
import be.panako.util.Key;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import static org.junit.jupiter.api.Assertions.*;

class QueryCleanupTest {
    @Test
    void orderedResultsAndQueryLifecycleRemainIdentical() throws Exception {
        QueryCleanupCheck.main(new String[0]);
    }

    @Test
    void actualNumericCacheIsLazyAndDoesNotSurviveAnotherQuery() throws Exception {
        Class<?> type = Class.forName(PanakoStrategy.class.getName() + "$QueryConfig");
        Constructor<?> constructor = type.getDeclaredConstructor();
        constructor.setAccessible(true);
        Method getFloat = type.getDeclaredMethod("getFloat", Key.class);
        Method getInt = type.getDeclaredMethod("getInt", Key.class);
        getFloat.setAccessible(true);
        getInt.setAccessible(true);
        String range = Config.get(Key.PANAKO_QUERY_RANGE);
        String frequency = Config.get(Key.PANAKO_MIN_FREQ_FACTOR);
        try {
            Config.set(Key.PANAKO_QUERY_RANGE, "2");
            Config.set(Key.PANAKO_MIN_FREQ_FACTOR, "invalid");
            Object firstQuery = constructor.newInstance();
            assertEquals(2f, ((Float) getFloat.invoke(firstQuery, Key.PANAKO_QUERY_RANGE)).floatValue());
            assertEquals(2, ((Integer) getInt.invoke(firstQuery, Key.PANAKO_QUERY_RANGE)).intValue());
            Config.set(Key.PANAKO_QUERY_RANGE, "3");
            assertEquals(2f, ((Float) getFloat.invoke(firstQuery, Key.PANAKO_QUERY_RANGE)).floatValue());
            assertEquals(2, ((Integer) getInt.invoke(firstQuery, Key.PANAKO_QUERY_RANGE)).intValue());
            Object nextQuery = constructor.newInstance();
            assertEquals(3f, ((Float) getFloat.invoke(nextQuery, Key.PANAKO_QUERY_RANGE)).floatValue());
            assertEquals(3, ((Integer) getInt.invoke(nextQuery, Key.PANAKO_QUERY_RANGE)).intValue());
            InvocationTargetException error = assertThrows(InvocationTargetException.class,
                    () -> getFloat.invoke(nextQuery, Key.PANAKO_MIN_FREQ_FACTOR));
            assertTrue(error.getCause() instanceof NumberFormatException);
        } finally {
            Config.set(Key.PANAKO_QUERY_RANGE, range);
            Config.set(Key.PANAKO_MIN_FREQ_FACTOR, frequency);
        }
    }
}
