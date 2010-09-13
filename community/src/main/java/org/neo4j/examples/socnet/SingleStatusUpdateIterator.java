package org.neo4j.examples.socnet;

import java.util.Iterator;

class SingleStatusUpdateIterator implements Iterator<StatusUpdate>
{
    private Iterator<? extends StatusUpdate> inner;
    private StatusUpdate current;
    private Boolean initiated = false;

    public SingleStatusUpdateIterator( Iterator<? extends StatusUpdate> iterator )
    {
        inner = iterator;
    }

    public boolean hasNext()
    {
        return inner.hasNext();
    }

    public StatusUpdate next()
    {
        initiated = true;
        current = inner.next();
        return current;
    }

    public void remove()
    {
        inner.remove();
    }

    public StatusUpdate current()
    {
        if ( !initiated )
        {
            return next();
        }

        return current;
    }
}
