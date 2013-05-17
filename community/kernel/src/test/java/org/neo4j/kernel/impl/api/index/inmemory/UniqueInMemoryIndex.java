package org.neo4j.kernel.impl.api.index.inmemory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexReader;
import org.neo4j.kernel.api.index.NodePropertyUpdate;
import org.neo4j.kernel.api.index.PreexistingIndexEntryConflictException;
import org.neo4j.kernel.impl.api.index.PropertyUpdateUniquenessValidator;

class UniqueInMemoryIndex extends InMemoryIndex implements PropertyUpdateUniquenessValidator.Lookup
{
    private final Map<Object, Long> indexData = new HashMap<Object, Long>();

    @Override
    IndexPopulator getPopulator()
    {
        return new InMemoryIndex.Populator()
        {
            @Override
            public void add( long nodeId, Object propertyValue ) throws IndexEntryConflictException, IOException
            {
                Long previous = indexData.get( propertyValue );
                if ( previous != null )
                {
                    throw new PreexistingIndexEntryConflictException( propertyValue, previous, nodeId );
                }

                super.add( nodeId, propertyValue );
            }
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
                return new UniqueInMemoryIndexReader( indexData );
            }

            @Override
            public void updateAndCommit( Iterable<NodePropertyUpdate> updates ) throws IOException,
                    IndexEntryConflictException
            {
                PropertyUpdateUniquenessValidator.validateUniqueness( updates, UniqueInMemoryIndex.this );

                super.updateAndCommit( updates );
            }
        };
    }

    @Override
    protected void add( long nodeId, Object propertyValue ) throws IndexEntryConflictException
    {
        indexData.put( propertyValue, nodeId );
    }

    @Override
    protected void remove( long nodeId, Object propertyValue )
    {
        indexData.remove( propertyValue );
    }

    @Override
    protected void clear()
    {
        indexData.clear();
    }

    @Override
    public Long currentlyIndexedNode( Object value ) throws IOException
    {
        return indexData.get( value );
    }
}
