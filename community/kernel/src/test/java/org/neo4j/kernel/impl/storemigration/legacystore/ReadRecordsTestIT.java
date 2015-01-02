/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import static org.neo4j.helpers.collection.IteratorUtil.count;

import java.io.File;
import java.io.IOException;

import org.junit.Test;

import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;
import org.neo4j.kernel.impl.storemigration.MigrationTestUtils;

public class ReadRecordsTestIT
{
    @Test
    public void shouldReadNodeRecords() throws IOException
    {
        File file = exampleDbStore( "neostore.nodestore.db" );

        LegacyNodeStoreReader nodeStoreReader = new LegacyNodeStoreReader( fs, file );
        assertEquals( 1001, nodeStoreReader.getMaxId() );
        Iterable<NodeRecord> records = nodeStoreReader.readNodeStore();
        int nodeCount = 0;
        for ( NodeRecord record : records )
        {
            if ( record.inUse() )
                nodeCount++;
        }
        assertEquals( 501, nodeCount );
        nodeStoreReader.close();
    }

    @Test
    public void shouldReadRelationshipRecords() throws IOException
    {
        File file = exampleDbStore( "neostore.relationshipstore.db" );

        LegacyRelationshipStoreReader relationshipStoreReader = new LegacyRelationshipStoreReader( fs, file );
        assertEquals( 1500, relationshipStoreReader.getMaxId() );
        Iterable<RelationshipRecord> records = relationshipStoreReader.readRelationshipStore();
        int relationshipCount = 0;
        for ( RelationshipRecord record : records )
        {
            if ( record.inUse() )
                relationshipCount++;
        }
        assertEquals( 500, relationshipCount );
        relationshipStoreReader.close();
    }

    @Test
    public void shouldReadASimplePropertyRecordById() throws IOException
    {
        File file = exampleDbStore( "neostore.propertystore.db" );

        LegacyPropertyStoreReader reader = new LegacyPropertyStoreReader( fs, file );
        LegacyPropertyRecord propertyRecord = reader.readPropertyRecord( 24 );

        int keyIndexId = propertyRecord.getKeyIndexId();
        assertEquals( 2, keyIndexId );
        Object value = propertyRecord.getType().getValue( propertyRecord, null );
        assertEquals( Integer.MAX_VALUE, value );
        reader.close();
    }

    @Test
    public void shouldReadAStringPropertyRecordById() throws IOException
    {
        File legacyStoreDir = exampleDbStore();
        File propertyStoreFile = new File( legacyStoreDir, "neostore.propertystore.db" );
        File stringStoreFile = new File( legacyStoreDir, "neostore.propertystore.db.strings" );
        File arrayStoreFile = new File( legacyStoreDir, "neostore.propertystore.db.arrays" );

        LegacyPropertyStoreReader reader = new LegacyPropertyStoreReader( fs, propertyStoreFile );
        LegacyPropertyRecord propertyRecord = reader.readPropertyRecord( 25 );

        int keyIndexId = propertyRecord.getKeyIndexId();
        assertEquals( 3, keyIndexId );
        LegacyDynamicRecordFetcher dynamicRecordReader = new LegacyDynamicRecordFetcher( fs,
                stringStoreFile, arrayStoreFile );
        Object value = propertyRecord.getType().getValue( propertyRecord, dynamicRecordReader );
        assertEquals( 1000, ((String) value).length() );
        reader.close();
        dynamicRecordReader.close();
    }

    @Test
    public void shouldReadAnArrayPropertyRecordById() throws IOException
    {
        File legacyStoreDir = exampleDbStore();
        File propertyStoreFile = new File( legacyStoreDir, "neostore.propertystore.db" );
        File stringStoreFile = new File( legacyStoreDir, "neostore.propertystore.db.strings" );
        File arrayStoreFile = new File( legacyStoreDir, "neostore.propertystore.db.arrays" );

        LegacyPropertyStoreReader reader = new LegacyPropertyStoreReader( fs, propertyStoreFile );
        LegacyPropertyRecord propertyRecord2 = reader.readPropertyRecord( 32 );

        int keyIndexId = propertyRecord2.getKeyIndexId();
        assertEquals( 10, keyIndexId );
        LegacyDynamicRecordFetcher dynamicRecordReader = new LegacyDynamicRecordFetcher( fs,
                stringStoreFile, arrayStoreFile );
        Object value = propertyRecord2.getType().getValue( propertyRecord2, dynamicRecordReader );
        assertArrayEquals( MigrationTestUtils.makeLongArray(), (int[]) value );
        reader.close();
        dynamicRecordReader.close();
    }

    @Test
    public void shouldReadPropertyIndexRecords() throws IOException
    {
        LegacyStore legacyStore = new LegacyStore( fs, exampleDbStore( "neostore" ) );

        LegacyPropertyIndexStoreReader propertyIndexStoreReader = legacyStore.getPropertyIndexStoreReader();
        assertEquals( 12, count( propertyIndexStoreReader.readPropertyIndexStore() ) );
        legacyStore.close();
    }

    @Test
    public void shouldReadRelationshipTypeRecords() throws IOException
    {
        LegacyStore legacyStore = new LegacyStore( fs, exampleDbStore( "neostore" ) );

        LegacyRelationshipTypeStoreReader relationshipTypeStoreReader = legacyStore.getRelationshipTypeStoreReader();
        assertEquals( 1000, count( relationshipTypeStoreReader.readRelationshipTypes() ) );
        legacyStore.close();
    }

    private final FileSystemAbstraction fs = new DefaultFileSystemAbstraction();

    private File exampleDbStore( String fileName ) throws IOException
    {
        return new File( exampleDbStore(), fileName );
    }

    private File exampleDbStore() throws IOException
    {
        return MigrationTestUtils.findOldFormatStoreDirectory();
    }
}
