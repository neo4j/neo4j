package org.neo4j.test;

import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;

public class NestedThrowableMatcher extends TypeSafeMatcher<Throwable>
{
    private final Class<? extends Throwable> expectedType;

    public NestedThrowableMatcher( Class<? extends Throwable> expectedType )
    {
        this.expectedType = expectedType;
    }

    @Override
    public void describeTo( Description description )
    {
        description.appendText( "expect " )
                .appendValue( expectedType )
                .appendText( " to be exception cause." );
    }

    @Override
    protected boolean matchesSafely( Throwable item )
    {
        Throwable currentThrowable = item;
        do
        {
            if ( expectedType.isInstance( currentThrowable ) )
            {
                return true;
            }
            currentThrowable = currentThrowable.getCause();
        }
        while ( currentThrowable != null );
        return false;
    }
}
