package org.neo4j.helpers;

import java.lang.reflect.InvocationTargetException;

public class Exceptions
{
    public static <T extends Throwable> T withCause( T exception, Throwable cause )
    {
        try
        {
            exception.initCause( cause );
        }
        catch ( Exception failure )
        {
            // ok, we did our best, guess there will be no cause
        }
        return exception;
    }

    public static RuntimeException launderedException( Throwable exception )
    {
        return launderedException( "Unexpected Exception", exception );
    }

    public static RuntimeException launderedException( String messageForUnexpected, Throwable exception )
    {
        if ( exception instanceof RuntimeException )
        {
            return (RuntimeException) exception;
        }
        else if ( exception instanceof Error )
        {
            throw (Error) exception;
        }
        else if ( exception instanceof InvocationTargetException )
        {
            return launderedException( messageForUnexpected,
                    ( (InvocationTargetException) exception ).getTargetException() );
        }
        else
        {
            throw new RuntimeException( messageForUnexpected, exception );
        }
    }

    private Exceptions()
    {
        // no instances
    }
}
