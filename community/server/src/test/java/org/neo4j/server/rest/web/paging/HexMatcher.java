package org.neo4j.server.rest.web.paging;

import java.util.regex.Pattern;

import org.hamcrest.Description;
import org.hamcrest.Factory;
import org.hamcrest.Matcher;
import org.junit.internal.matchers.TypeSafeMatcher;

public class HexMatcher extends TypeSafeMatcher<String>
{
    private static final Pattern pattern = Pattern.compile( "[a-fA-F0-9]*" );
    private String candidate;

    private HexMatcher()
    {
        
    }

    @Override
    public void describeTo( Description description )
    {
        description.appendText( String.format("[%s] is not a pure hexadecimal string", candidate) );
    }

    @Override
    public boolean matchesSafely( String candidate )
    {
        this.candidate = candidate;
        return pattern.matcher( candidate ).matches();
    }

    @Factory
    public static <T> Matcher<String> containsOnlyHex()
    {
        return new HexMatcher();
    }
}
