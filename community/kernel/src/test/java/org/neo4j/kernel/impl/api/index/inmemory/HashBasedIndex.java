package org.neo4j.kernel.impl.api.index.inmemory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.neo4j.kernel.impl.api.PrimitiveLongIterator;

import static org.neo4j.helpers.collection.IteratorUtil.emptyPrimitiveLongIterator;
import static org.neo4j.helpers.collection.IteratorUtil.toPrimitiveLongIterator;

class HashBasedIndex extends InMemoryIndexImplementation
{
    private final Map<Object, Set<Long>> data = new HashMap<>();

    @Override
    public String toString()
    {
        return data.toString();
    }

    @Override
    void clear()
    {
        data.clear();
    }

    @Override
    PrimitiveLongIterator doLookup( Object propertyValue )
    {
        Set<Long> nodes = data.get( propertyValue );
        return nodes == null ? emptyPrimitiveLongIterator() : toPrimitiveLongIterator( nodes.iterator() );
    }

    @Override
    void doAdd( Object propertyValue, long nodeId )
    {
        Set<Long> nodes = data.get( propertyValue );
        if ( nodes == null )
        {
            data.put( propertyValue, nodes = new HashSet<>() );
        }
        nodes.add( nodeId );
    }

    @Override
    void doRemove( Object propertyValue, long nodeId )
    {
        Set<Long> nodes = data.get( propertyValue );
        if ( nodes != null )
        {
            nodes.remove( nodeId );
        }
    }
}
