package org.neo4j.kernel.impl.api.index.inmemory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.kernel.api.index.IndexReader;

class UniqueInMemoryIndexReader implements IndexReader
{
    private final HashMap<Object, Long> indexData;

    UniqueInMemoryIndexReader( Map<Object, Long> indexData )
    {
        this.indexData = new HashMap<Object, Long>( indexData );
    }

    @Override
    public Iterator<Long> lookup( Object value )
    {
        Long result = indexData.get( value );
        return result != null ? IteratorUtil.singletonIterator( result ) : IteratorUtil.<Long>emptyIterator();
    }

    @Override
    public void close()
    {
    }
}
