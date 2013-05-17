package org.neo4j.kernel.impl.api.index.inmemory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.kernel.api.index.IndexReader;

class NonUniqueInMemoryIndexReader implements IndexReader
{
    private final HashMap<Object, Set<Long>> indexData;

    NonUniqueInMemoryIndexReader( Map<Object, Set<Long>> indexData )
    {
        this.indexData = new HashMap<Object, Set<Long>>( indexData );
    }

    @Override
    public Iterator<Long> lookup( Object value )
    {
        Set<Long> result = indexData.get( value );
        return result != null ? result.iterator() : IteratorUtil.<Long>emptyIterator();
    }

    @Override
    public void close()
    {
    }
}
