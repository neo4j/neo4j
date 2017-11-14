package org.neo4j.causalclustering.discovery;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ErrorHandler implements AutoCloseable
{
    private final List<Throwable> throwables = new ArrayList<>();
    private final String message;

    public ErrorHandler( String message )
    {
        this.message = message;
    }

    public void add( Throwable throwable )
    {
        throwables.add( throwable );
    }

    public List<Throwable> throwables()
    {
        return Collections.unmodifiableList( throwables );
    }

    @Override
    public void close() throws RuntimeException
    {
        throwIfException();
    }

    private void throwIfException()
    {
        if ( !throwables.isEmpty() )
        {
            RuntimeException runtimeException = null;
            for ( Throwable throwable : throwables )
            {
                if ( runtimeException == null )
                {
                    runtimeException = new RuntimeException( message, throwable );
                }
                else
                {
                    runtimeException.addSuppressed( throwable );
                }
            }
            throw runtimeException;
        }
    }
}
