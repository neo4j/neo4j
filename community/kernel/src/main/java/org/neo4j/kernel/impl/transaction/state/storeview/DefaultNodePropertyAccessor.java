package org.neo4j.kernel.impl.transaction.state.storeview;

import org.neo4j.internal.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.io.IOUtils;
import org.neo4j.kernel.api.index.NodePropertyAccessor;
import org.neo4j.storageengine.api.StorageNodeCursor;
import org.neo4j.storageengine.api.StoragePropertyCursor;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.values.storable.Value;

import static org.neo4j.values.storable.Values.NO_VALUE;

public class DefaultNodePropertyAccessor implements NodePropertyAccessor
{
    private final StorageReader reader;
    private final StorageNodeCursor nodeCursor;
    private final StoragePropertyCursor propertyCursor;

    DefaultNodePropertyAccessor( StorageReader reader )
    {
        this.reader = reader;
        nodeCursor = reader.allocateNodeCursor();
        propertyCursor = reader.allocatePropertyCursor();
    }

    @Override
    public Value getNodePropertyValue( long nodeId, int propertyKeyId ) throws EntityNotFoundException
    {
        nodeCursor.single( nodeId );
        if ( nodeCursor.next() && nodeCursor.hasProperties() )
        {
            propertyCursor.init( nodeCursor.propertiesReference() );
            while ( propertyCursor.next() )
            {
                if ( propertyCursor.propertyKey() == propertyKeyId )
                {
                    return propertyCursor.propertyValue();
                }
            }
        }
        return NO_VALUE;
    }

    @Override
    public void close()
    {
        IOUtils.closeAllUnchecked( nodeCursor, propertyCursor, reader );
    }
}
