package org.neo4j.kernel.impl.api.index.inmemory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.helpers.collection.PrefetchingIterator;
import org.neo4j.kernel.impl.api.PrimitiveLongIterator;

class ListBasedIndex extends InMemoryIndexImplementation
{
    private final List<Entry> data = new ArrayList<>();

    @Override
    void clear()
    {
        data.clear();
    }

    @Override
    PrimitiveLongIterator doLookup( Object propertyValue )
    {
        return IteratorUtil.toPrimitiveLongIterator( find( data.iterator(), propertyValue ) );
    }

    @Override
    void doAdd( Object propertyValue, long nodeId )
    {
        data.add( new Entry( propertyValue, nodeId ) );
    }

    @Override
    void doRemove( Object propertyValue, long nodeId )
    {
        for ( Iterator<Entry> iterator = data.iterator(); iterator.hasNext(); )
        {
            if ( iterator.next().entryEquals( propertyValue, nodeId ) )
            {
                iterator.remove();
                return;
            }
        }
    }

    private static class Entry
    {
        private final Object propertyValue;
        private final long nodeId;

        Entry( Object propertyValue, long nodeId )
        {
            this.propertyValue = propertyValue;
            this.nodeId = nodeId;
        }

        boolean entryEquals( Object propertyValue, long nodeId )
        {
            return this.nodeId == nodeId && this.propertyValue.equals( propertyValue );
        }

        boolean valueEquals( Object propertyValue )
        {
            return this.propertyValue.equals( propertyValue );
        }
    }

    private static Iterator<Long> find( final Iterator<Entry> source, final Object propertyValue )
    {
        return new PrefetchingIterator<Long>()
        {
            @Override
            protected Long fetchNextOrNull()
            {
                while ( source.hasNext() )
                {
                    Entry entry = source.next();
                    if ( entry.valueEquals( propertyValue ) )
                    {
                        return entry.nodeId;
                    }
                }
                return null;
            }
        };
    }
}
