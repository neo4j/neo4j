package org.neo4j.kernel.impl.storemigration;

import org.junit.Test;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;

import java.io.IOException;
import java.net.URL;

import static org.junit.Assert.assertEquals;

public class ReadRecordsTest
{
    @Test
    public void shouldReadNodeRecords() throws IOException
    {
        URL nodeStoreFile = getClass().getResource( "oldformatstore/neostore.nodestore.db" );

        Iterable<NodeRecord> records = new LegacyNodeStoreReader().readNodeStore( nodeStoreFile.getFile() );
        int nodeCount = 0;
        for ( NodeRecord record : records )
        {
            nodeCount++;
        }
        assertEquals( 1001, nodeCount );
    }

    @Test
    public void shouldReadPropertyRecords() throws IOException
    {
        URL propertyStoreFile = getClass().getResource( "oldformatstore/neostore.propertystore.db" );

        Iterable<LegacyPropertyStoreReader.LegacyPropertyRecord> records = new LegacyPropertyStoreReader().readPropertyStore( propertyStoreFile.getFile() );
        int propertyCount = 0;
        for ( LegacyPropertyStoreReader.LegacyPropertyRecord record : records )
        {
            propertyCount++;
        }
        assertEquals( 1000, propertyCount );
    }
}
