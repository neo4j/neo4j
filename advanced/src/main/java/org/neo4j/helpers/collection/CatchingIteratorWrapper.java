package org.neo4j.helpers.collection;

import java.util.Iterator;

public abstract class CatchingIteratorWrapper<T, U> extends PrefetchingIterator<T>
{
    private final Iterator<U> source;

    public CatchingIteratorWrapper( Iterator<U> source )
    {
        this.source = source;
    }

    @Override
    protected T fetchNextOrNull()
    {
        while ( source.hasNext() )
        {
            try
            {
                U nextItem = source.next();
                return underlyingObjectToObject( nextItem );
            }
            catch ( Throwable t )
            {
                if ( exceptionOk( t ) )
                {
                    continue;
                }
                if ( t instanceof RuntimeException )
                {
                    throw (RuntimeException) t;
                }
                else if ( t instanceof Error )
                {
                    throw (Error) t;
                }
                throw new RuntimeException( t );
            }
        }
        return null;
    }
    
    protected boolean exceptionOk( Throwable t )
    {
        return true;
    }

    protected abstract T underlyingObjectToObject( U object );
}
