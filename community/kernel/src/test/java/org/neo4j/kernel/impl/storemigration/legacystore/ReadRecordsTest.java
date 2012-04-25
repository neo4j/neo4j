/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.kernel.impl.storemigration.legacystore;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.net.URL;

import org.junit.Test;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyIndexRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipTypeRecord;
import org.neo4j.kernel.impl.storemigration.MigrationTestUtils;

public class ReadRecordsTest
{
    @Test
    public void shouldReadNodeRecords() throws IOException
    {
        URL nodeStoreFile = getClass().getResource( "exampledb/neostore.nodestore.db" );

        LegacyNodeStoreReader nodeStoreReader = new LegacyNodeStoreReader( nodeStoreFile.getFile() );
        assertEquals( 1001, nodeStoreReader.getMaxId() );
        Iterable<NodeRecord> records = nodeStoreReader.readNodeStore();
        int nodeCount = 0;
        for ( NodeRecord record : records )
        {
            if ( record.inUse() ) nodeCount++;
        }
        assertEquals( 501, nodeCount );
        nodeStoreReader.close();
    }

    @Test
    public void shouldReadRelationshipRecords() throws IOException
    {
        URL nodeStoreFile = getClass().getResource( "exampledb/neostore.relationshipstore.db" );

        LegacyRelationshipStoreReader relationshipStoreReader = new LegacyRelationshipStoreReader( nodeStoreFile.getFile() );
        assertEquals( 1500, relationshipStoreReader.getMaxId() );
        Iterable<RelationshipRecord> records = relationshipStoreReader.readRelationshipStore();
        int relationshipCount = 0;
        for ( RelationshipRecord record : records )
        {
            if ( record.inUse() ) relationshipCount++;
        }
        assertEquals( 500, relationshipCount );
        relationshipStoreReader.close();
    }

    @Test
    public void shouldReadASimplePropertyRecordById() throws IOException
    {
        URL propertyStoreFile = getClass().getResource( "exampledb/neostore.propertystore.db" );

        LegacyPropertyRecord propertyRecord = new LegacyPropertyStoreReader( propertyStoreFile.getFile() ).readPropertyRecord( 24 );

        int keyIndexId = propertyRecord.getKeyIndexId();
        assertEquals( 2, keyIndexId );
        Object value = propertyRecord.getType().getValue( propertyRecord, null );
        assertEquals( Integer.MAX_VALUE, value );
    }

    @Test
    public void shouldReadAStringPropertyRecordById() throws IOException
    {
        URL propertyStoreFile = getClass().getResource( "exampledb/neostore.propertystore.db" );
        URL stringStoreFile = getClass().getResource( "exampledb/neostore.propertystore.db.strings" );
        URL arrayStoreFile = getClass().getResource( "exampledb/neostore.propertystore.db.arrays" );

        LegacyPropertyRecord propertyRecord = new LegacyPropertyStoreReader( propertyStoreFile.getFile() ).readPropertyRecord( 25 );

        int keyIndexId = propertyRecord.getKeyIndexId();
        assertEquals( 3, keyIndexId );
        Object value = propertyRecord.getType().getValue( propertyRecord, new LegacyDynamicRecordFetcher( stringStoreFile.getFile(), arrayStoreFile.getFile() ) );
        assertEquals( 1000, ((String) value).length() );
    }

    @Test
    public void shouldReadAnArrayPropertyRecordById() throws IOException
    {
        URL propertyStoreFile = getClass().getResource( "exampledb/neostore.propertystore.db" );
        URL stringStoreFile = getClass().getResource( "exampledb/neostore.propertystore.db.strings" );
        URL arrayStoreFile = getClass().getResource( "exampledb/neostore.propertystore.db.arrays" );

        LegacyPropertyRecord propertyRecord2 = new LegacyPropertyStoreReader( propertyStoreFile.getFile() ).readPropertyRecord( 32 );

        int keyIndexId = propertyRecord2.getKeyIndexId();
        assertEquals( 10, keyIndexId );
        Object value = propertyRecord2.getType().getValue( propertyRecord2, new LegacyDynamicRecordFetcher( stringStoreFile.getFile(), arrayStoreFile.getFile() ) );
        assertArrayEquals( MigrationTestUtils.makeLongArray(), (int[]) value );
    }

    @Test
    public void shouldReadPropertyIndexRecords() throws IOException
    {
        URL legacyStoreResource = getClass().getResource( "exampledb/neostore" );
        LegacyStore legacyStore = new LegacyStore( legacyStoreResource.getFile() );

        LegacyPropertyIndexStoreReader propertyIndexStoreReader = legacyStore.getPropertyIndexStoreReader();
        int recordCount = 0;
        for ( PropertyIndexRecord record : propertyIndexStoreReader.readPropertyIndexStore() )
        {
            recordCount++;
        }
        assertEquals( 12, recordCount );
    }

    @Test
    public void shouldReadRelationshipTypeRecords() throws IOException
    {
        URL legacyStoreResource = getClass().getResource( "exampledb/neostore" );
        LegacyStore legacyStore = new LegacyStore( legacyStoreResource.getFile() );

        LegacyRelationshipTypeStoreReader relationshipTypeStoreReader = legacyStore.getRelationshipTypeStoreReader();
        int recordCount = 0;
        for ( RelationshipTypeRecord record : relationshipTypeStoreReader.readRelationshipTypes() )
        {
            recordCount++;
        }
        assertEquals( 1000, recordCount );
    }
}
