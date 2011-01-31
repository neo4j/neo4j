package org.neo4j.release.it.std.exec;

import org.hamcrest.Description;
import org.hamcrest.Factory;
import org.junit.internal.matchers.TypeSafeMatcher;

public class HasItemEndingIn extends TypeSafeMatcher<Iterable<String>>{
    private String endingIn;

    public HasItemEndingIn(String endingIn){

        this.endingIn = endingIn;
    }

    @Override
    public boolean matchesSafely(Iterable<String> iterable) {
        for(String current : iterable){
            if(current.endsWith(endingIn))
                return true;
        }

        return false;
    }

    public void describeTo(Description description) {
        description.appendText("Expected at least one item to end in " + endingIn);
    }

    @Factory
    public static HasItemEndingIn hasItemEndingIn(String endingIn) {
            return new HasItemEndingIn(endingIn);
    }
}
