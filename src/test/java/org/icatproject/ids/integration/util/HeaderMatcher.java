package org.icatproject.ids.integration.util;

import static java.util.Objects.requireNonNull;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import static org.hamcrest.Matchers.equalToIgnoringCase;

/**
 * A custom header that matches HTTP headers in some HttpResponse object.  It
 * supports two kind of matches: that the header is missing or that the header
 * is present and the value matches some other Matcher.
 */
public class HeaderMatcher extends BaseMatcher<HttpResponse> {

    private final String headerName;
    private final Matcher<String> matcher;

    public HeaderMatcher(String headerName, Matcher<String> valueMatcher) {
        this.headerName = requireNonNull(headerName);
        this.matcher = valueMatcher;
    }

    private boolean isHeaderExpected() {
        return matcher != null;
    }

    @Override
    public boolean matches(Object item) {
        HttpResponse response = (HttpResponse) item; // It is a bug if item is not an HttpResponse.

        Header header = response.getFirstHeader(headerName);

        if (!isHeaderExpected()) {
            return header == null;
        }

        if (header == null) {
            return false;
        }

        String headerValue = header.getValue();

        return matcher.matches(headerValue);
    }

    @Override
    public void describeTo(Description description) {
        if (isHeaderExpected()) {
            description.appendText("a '")
                    .appendText(headerName)
                    .appendText("' header that ")
                    .appendDescriptionOf(matcher);
        } else {
            description.appendText("no '").appendText(headerName).appendText("' header");
        }
    }

    /**
     * Match an HttpResponse that does not contain the named header.
     * @param header The name of the HTTP response header.
     * @return a Matcher that verifies expected response.
     */
    public static Matcher<HttpResponse> hasNoHeader(String header) {
        return new HeaderMatcher(header, null);
    }

    /**
     * Match an HttpResponse that contains the named header with a value that
     * matches the supplied matcher.
     * @param header The name of the HTTP response header.
     * @param matcher The matcher for the header value.
     * @return a Matcher that verifies expected response.
     */
    public static Matcher hasHeaderMatching(String header, Matcher<String> matcher) {
        return new HeaderMatcher(header, matcher);
    }

    /**
     * Match an HttpResponse that contains the named header with the specific
     * value (ignoring case).
     * @param header The name of the HTTP response header.
     * @param value The expected value.
     * @return a Matcher that verifies expected response.
     */
    public static Matcher hasHeader(String header, Object value) {
        Matcher<String> m = equalToIgnoringCase(String.valueOf(value));
        return hasHeaderMatching(header, m);
    }
}
