package org.neo4j.kernel.impl.api.index.inmemory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexReader;

class NonUniqueInMemoryIndex extends InMemoryIndex
{
    private final Map<Object, Set<Long>> indexData = new HashMap<Object, Set<Long>>();

    @Override
    IndexPopulator getPopulator()
    {
        return new InMemoryIndex.Populator()
        {
        };
    }

    @Override
    IndexAccessor getOnlineAccessor()
    {
        return new InMemoryIndex.OnlineAccessor()
        {
            @Override
            public IndexReader newReader()
            {
                return new NonUniqueInMemoryIndexReader( indexData );
            }
        };
    }

    @Override
    public void add( long nodeId, Object propertyValue )
    {
        Set<Long> nodes = getLongs( propertyValue );
        nodes.add( nodeId );
    }

    @Override
    void remove( long nodeId, Object propertyValue )
    {
        Set<Long> nodes = getLongs( propertyValue );
        nodes.remove( nodeId );
    }

    @Override
    void clear()
    {
        indexData.clear();
    }

    private Set<Long> getLongs( Object propertyValue )
    {
        Set<Long> nodes = indexData.get( propertyValue );
        if ( nodes == null )
        {
            nodes = new HashSet<Long>();
            indexData.put( propertyValue, nodes );
        }
        return nodes;
    }
}
