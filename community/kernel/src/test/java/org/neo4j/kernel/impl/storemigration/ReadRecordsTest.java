/**
 * Copyright (c) 2002-2011 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.storemigration;

import org.junit.Test;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyIndexRecord;

import java.io.IOException;
import java.net.URL;

import static org.junit.Assert.assertEquals;

public class ReadRecordsTest
{
    @Test
    public void shouldReadNodeRecords() throws IOException
    {
        URL nodeStoreFile = getClass().getResource( "oldformatstore/neostore.nodestore.db" );

        Iterable<NodeRecord> records = new LegacyNodeStoreReader( nodeStoreFile.getFile() ).readNodeStore();
        int nodeCount = 0;
        for ( NodeRecord record : records )
        {
            nodeCount++;
        }
        assertEquals( 1001, nodeCount );
    }

    @Test
    public void shouldReadASimplePropertyRecordById() throws IOException
    {
        URL propertyStoreFile = getClass().getResource( "oldformatstore/neostore.propertystore.db" );

        LegacyPropertyRecord propertyRecord = new LegacyPropertyStoreReader( propertyStoreFile.getFile() ).readPropertyRecord( 24 );

        int keyIndexId = propertyRecord.getKeyIndexId();
        assertEquals( 0, keyIndexId );
        Object value = propertyRecord.getType().getValue( propertyRecord, null );
        assertEquals( true, value );
    }

    @Test
    public void shouldReadAStringPropertyRecordById() throws IOException
    {
        URL propertyStoreFile = getClass().getResource( "oldformatstore/neostore.propertystore.db" );
        URL stringStoreFile = getClass().getResource( "oldformatstore/neostore.propertystore.db.strings" );
        URL arrayStoreFile = getClass().getResource( "oldformatstore/neostore.propertystore.db.arrays" );

        LegacyPropertyRecord propertyRecord = new LegacyPropertyStoreReader( propertyStoreFile.getFile() ).readPropertyRecord( 25 );

        int keyIndexId = propertyRecord.getKeyIndexId();
        assertEquals( 1, keyIndexId );
        Object value = propertyRecord.getType().getValue( propertyRecord, new LegacyDynamicRecordFetcher( stringStoreFile.getFile(), arrayStoreFile.getFile() ) );
        assertEquals( 1000, ((String) value).length() );
    }

    @Test
    public void shouldReadAnArrayPropertyRecordById() throws IOException
    {
        URL propertyStoreFile = getClass().getResource( "oldformatstore/neostore.propertystore.db" );
        URL stringStoreFile = getClass().getResource( "oldformatstore/neostore.propertystore.db.strings" );
        URL arrayStoreFile = getClass().getResource( "oldformatstore/neostore.propertystore.db.arrays" );

        LegacyPropertyRecord propertyRecord2 = new LegacyPropertyStoreReader( propertyStoreFile.getFile() ).readPropertyRecord( 26 );

        int keyIndexId = propertyRecord2.getKeyIndexId();
        assertEquals( 2, keyIndexId );
        Object value = propertyRecord2.getType().getValue( propertyRecord2, new LegacyDynamicRecordFetcher( stringStoreFile.getFile(), arrayStoreFile.getFile() ) );
        int[] array = (int[]) value;
        assertEquals( 100, array.length );
        for ( int i = 0; i < array.length; i++ )
        {
            assertEquals( i, array[i] );
        }
    }

    @Test
    public void shouldReadPropertyIndexRecords() throws IOException
    {
        URL legacyStoreResource = getClass().getResource( "oldformatstore/neostore" );
        LegacyStore legacyStore = new LegacyStore( legacyStoreResource.getFile() );

        LegacyPropertyIndexStoreReader propertyIndexStoreReader = legacyStore.getPropertyIndexStoreReader();
        int recordCount = 0;
        for ( PropertyIndexRecord record : propertyIndexStoreReader.readPropertyIndexStore() )
        {
            recordCount++;
        }
    }
}
