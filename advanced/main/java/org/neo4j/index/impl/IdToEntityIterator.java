package org.neo4j.index.impl;

import java.util.Iterator;

import org.neo4j.commons.iterator.CatchingIteratorWrapper;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.PropertyContainer;

public abstract class IdToEntityIterator<T extends PropertyContainer>
        extends CatchingIteratorWrapper<T, Long>
{
    public IdToEntityIterator( Iterator<Long> ids )
    {
        super( ids );
    }
    
    @Override
    protected boolean exceptionOk( Throwable t )
    {
        return t instanceof NotFoundException;
    }
}
