package org.icatproject.ids.integration.util;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

/**
 * A Matcher that checks whether a String argument is a Long value.  The code is
 * heavily based on an answer from Ezequiel to
 * <a href="https://stackoverflow.com/questions/58099695/is-there-a-way-in-hamcrest-to-test-for-a-value-to-be-a-number">
 * a stack overview question</a>.
 */
public class LongValue extends TypeSafeMatcher<String> {

    @Override
    protected boolean matchesSafely(String s) {
        try {
            Long.valueOf(s);
        } catch (NumberFormatException nfe) {
            return false;
        }
        return true;
    }

    @Override
    public void describeTo(Description description) {
        description.appendText("is long");
    }

    public static Matcher<String> longValue() {
        return new LongValue();
    }
}