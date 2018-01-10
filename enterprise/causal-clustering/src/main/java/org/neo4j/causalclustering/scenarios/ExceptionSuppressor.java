package org.neo4j.causalclustering.scenarios;

import org.neo4j.function.ThrowingAction;

/**
 * Allows clean error handling when closing down multiple resources which are independent.
 * One can differentiate exceptions to be caught and suppressed from those which should cause
 * the current chain of suppressed exceptions to be thrown.
 * <p>
 * On {@link #close()} the chain of suppressed exceptions is thrown.
 */
class ExceptionSuppressor implements AutoCloseable
{
    private final Class defaultSuppressedClass;
    private Throwable first = null;

    public ExceptionSuppressor()
    {
        this( Exception.class );
    }

    public ExceptionSuppressor( Class defaultSuppressedClass )
    {
        this.defaultSuppressedClass = defaultSuppressedClass;
    }

    public <E extends Throwable> void execute( ThrowingAction<E> action, Class suppressedClass ) throws E
    {
        try
        {
            action.apply();
        }
        catch ( Throwable ex )
        {
            if ( first == null )
            {
                first = ex;
            }
            else
            {
                first.addSuppressed( ex );
            }

            if ( !suppressedClass.isInstance( ex ) )
            {
                first = null;
                throw ex;
            }
        }
    }

    public <E extends Throwable> void execute( ThrowingAction<E> action ) throws E
    {
        execute( action, defaultSuppressedClass );
    }

    @Override
    public void close() throws Exception
    {
        if ( first != null )
        {
            if ( first instanceof Error )
            {
                throw (Error) first;
            }
            else if ( first instanceof Exception )
            {
                throw (Exception) first;
            }
            else
            {
                // custom Throwables, can't cast
                throw new RuntimeException( first );
            }
        }
    }
}
