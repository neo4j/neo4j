/*
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
package org.neo4j.kernel.impl.nioneo.store;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.impl.store.DynamicStringStore;
import org.neo4j.kernel.impl.store.LabelTokenStore;
import org.neo4j.kernel.impl.store.NeoStore;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.RelationshipTypeTokenStore;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.transaction.state.DataSourceManager;
import org.neo4j.kernel.impl.transaction.state.NeoStoreProvider;
import org.neo4j.test.EphemeralFileSystemRule;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.junit.Assert.assertEquals;

import static org.neo4j.graphdb.DynamicLabel.label;
import static org.neo4j.graphdb.DynamicRelationshipType.withName;
import static org.neo4j.helpers.UTF8.encode;
import static org.neo4j.kernel.impl.store.CommonAbstractStore.buildTypeDescriptorAndVersion;
import static org.neo4j.kernel.impl.store.StoreFactory.LABEL_TOKEN_STORE_NAME;
import static org.neo4j.kernel.impl.store.StoreFactory.NODE_STORE_NAME;
import static org.neo4j.kernel.impl.store.StoreFactory.PROPERTY_STRINGS_STORE_NAME;
import static org.neo4j.kernel.impl.store.StoreFactory.RELATIONSHIP_TYPE_TOKEN_STORE_NAME;

public class StoreHighIdInflationTest
{
    @Test
    public void shouldFindActualHighIdWhenStartingOnInflatedStore() throws Exception
    {
        // GIVEN
        FileSystemAbstraction fs = fsr.get();
        fs.mkdirs( new File( storeDir ) );
        GraphDatabaseAPI db = (GraphDatabaseAPI) newImpermanentDb();
        long highestCreatedNodeId = createACoupleOfNodes( db, 3 );
        long highestPropertyStringRecord = getHighestDynamicStringPropertyId( db );
        db.shutdown();
        inflateStore( NodeStore.TYPE_DESCRIPTOR, NODE_STORE_NAME, megabyteWorthOfZeros() );
        inflateStore( DynamicStringStore.TYPE_DESCRIPTOR, PROPERTY_STRINGS_STORE_NAME, megabyteWorthOfZeros() );

        // WHEN
        db = (GraphDatabaseAPI) newImpermanentDb();
        long nodeIdAfterInflation = createACoupleOfNodes( db, 1 );
        long stringPropertyIdAfterInflation = getHighestDynamicStringPropertyId( db );
        db.shutdown();

        // THEN
        assertEquals( highestCreatedNodeId+1, nodeIdAfterInflation );
        assertEquals( highestPropertyStringRecord+1, stringPropertyIdAfterInflation );
    }

    @Test
    public void shouldTrimInflatedTokenStoreDuringRecovery() throws IOException
    {
        // Given
        int nodesWithUniqueLabels = 10;
        int relsWithUniqueTypes = 5;
        int labelTokenRecordSize;

        FileSystemAbstraction fs = fsr.get();
        fs.mkdirs( new File( storeDir ) );
        GraphDatabaseService db = newImpermanentDb();
        createLabeledNodesAndRels( nodesWithUniqueLabels, relsWithUniqueTypes, db );
        labelTokenRecordSize = neoStoreOf( db ).getLabelTokenStore().getRecordSize();
        db.shutdown();

        // When
        inflateStore( LabelTokenStore.TYPE_DESCRIPTOR, LABEL_TOKEN_STORE_NAME, megabyteWorthOfZeros() );
        newImpermanentDb().shutdown();

        // Then
        long fileSize = fs.getFileSize( new File( storeDir, NeoStore.DEFAULT_NAME + LABEL_TOKEN_STORE_NAME ) );
        int trailerLength = trailerLength( LabelTokenStore.TYPE_DESCRIPTOR );
        assertEquals( "Unexpected file size; trailerLength=" + trailerLength,
                nodesWithUniqueLabels * labelTokenRecordSize, fileSize - trailerLength );
    }

    private static NeoStore neoStoreOf( GraphDatabaseService db )
    {
        return ((GraphDatabaseAPI) db).getDependencyResolver().resolveDependency( NeoStoreProvider.class ).evaluate();
    }

    @Test
    public void shouldMarkReservedRelationshipTypesAsNotInUse() throws IOException
    {
        // Given
        int nodesWithUniqueLabels = 5;
        int relsWithUniqueTypes = 10;
        int reservedRelTypeRecordsCount = 100;
        int relTypeTokenRecordSize;
        String relTypeStoreFileName = NeoStore.DEFAULT_NAME + RELATIONSHIP_TYPE_TOKEN_STORE_NAME;

        FileSystemAbstraction fs = fsr.get();
        fs.mkdirs( new File( storeDir ) );
        GraphDatabaseService db = newImpermanentDb();
        createLabeledNodesAndRels( nodesWithUniqueLabels, relsWithUniqueTypes, db );
        relTypeTokenRecordSize = relTypeTokenStore( db ).getRecordSize();
        assertEquals( "Unexpected highest inUse id",
                relsWithUniqueTypes - 1, relTypeTokenStore( db ).getHighestPossibleIdInUse() );
        db.shutdown();

        // When
        inflateStore( RelationshipTypeTokenStore.TYPE_DESCRIPTOR, RELATIONSHIP_TYPE_TOKEN_STORE_NAME,
                reservedRelTypeRecords( reservedRelTypeRecordsCount, relTypeTokenRecordSize ) );

        int lastInUseRecordId = findLastInUseRecord( relTypeStoreFileName, relTypeTokenRecordSize, 0 );
        assertEquals( reservedRelTypeRecordsCount + relsWithUniqueTypes, lastInUseRecordId );
        newImpermanentDb().shutdown();

        // Then
        lastInUseRecordId = findLastInUseRecord( relTypeStoreFileName, relTypeTokenRecordSize,
                trailerLength( RelationshipTypeTokenStore.TYPE_DESCRIPTOR ) );
        assertEquals( "Unexpected number of inUse records", relsWithUniqueTypes, lastInUseRecordId );
        RelationshipTypeTokenStore relTypeTokenStore = relTypeTokenStore( newImpermanentDb() );
        for ( int i = 0; i < reservedRelTypeRecordsCount; i++ )
        {
            assertEquals( relsWithUniqueTypes + i, relTypeTokenStore.nextId() );
        }
    }

    private long getHighestDynamicStringPropertyId( GraphDatabaseAPI db )
    {
        return db.getDependencyResolver().resolveDependency( DataSourceManager.class )
                .getDataSource().getNeoStore().getPropertyStore()
                .getStringStore().getHighestPossibleIdInUse();
    }

    private static RelationshipTypeTokenStore relTypeTokenStore( GraphDatabaseService db )
    {
        return neoStoreOf( db ).getRelationshipTypeTokenStore();
    }

    private void inflateStore( String trailerTypeDescriptor, String store, ByteBuffer data ) throws IOException
    {
        int trailerLength = trailerLength( trailerTypeDescriptor );
        File neoStore = new File( storeDir, NeoStore.DEFAULT_NAME );
        File storeFile = new File( neoStore.getAbsolutePath() + store );
        FileSystemAbstraction fs = fsr.get();
        long size = fs.getFileSize( storeFile );
        try ( StoreChannel channel = fs.open( storeFile, "rw" ) )
        {
            channel.position( size - trailerLength );
            channel.write( data );
        }
        fs.deleteFile( new File( storeFile, ".id" ) );
    }

    private ByteBuffer megabyteWorthOfZeros()
    {
        ByteBuffer buffer = ByteBuffer.allocate( 1024*1024 );
        while ( buffer.hasRemaining() )
        {
            buffer.put( (byte) 0 );
        }
        buffer.flip();
        return buffer;
    }

    private ByteBuffer reservedRelTypeRecords( int count, int size )
    {
        ByteBuffer buffer = ByteBuffer.allocate( count * size );
        for ( int i = 0; i < count; i++ )
        {
            buffer.put( Record.IN_USE.byteValue() ).putInt( Record.RESERVED.intValue() );
        }
        buffer.flip();
        return buffer;
    }

    private int findLastInUseRecord( String storeFile, int recordSize, int trailerLength ) throws IOException
    {
        try ( StoreChannel channel = fsr.get().open( new File( storeDir, storeFile ), "rw" ) )
        {
            ByteBuffer buffer = ByteBuffer.allocate( recordSize );
            long position = channel.size() - trailerLength - recordSize;
            while ( position > 0 )
            {
                buffer.clear();
                channel.read( buffer, position );
                buffer.flip();
                if ( buffer.get() == Record.IN_USE.byteValue() )
                {
                    return (int) (position / recordSize) + 1;
                }
                position -= recordSize;
            }
        }
        throw new IllegalStateException( "No inUse records found" );
    }

    private long createACoupleOfNodes( GraphDatabaseService db, int count )
    {
        try ( Transaction tx = db.beginTx() )
        {
            long last = 0;
            for ( int i = 0; i < count; i++ )
            {
                Node node = db.createNode();
                node.setProperty( "key", "A very very very loooooooooooooooooooooooooooooooooooooooooooooong string " +
                                         "that should spill over into a dynamic record" );
                last = node.getId();
            }
            tx.success();
            return last;
        }
    }

    private static void createLabeledNodesAndRels( int nodeCount, int relCount, GraphDatabaseService db )
    {
        try ( Transaction tx = db.beginTx() )
        {
            Node[] nodes = new Node[nodeCount];
            for ( int i = 0; i < nodeCount; i++ )
            {
                nodes[i] = db.createNode( label( "LABEL" + i ) );
            }

            for ( int i = 0; i < relCount; i++ )
            {
                Node source = nodes[ThreadLocalRandom.current().nextInt( nodes.length )];
                Node target = nodes[ThreadLocalRandom.current().nextInt( nodes.length )];
                source.createRelationshipTo( target, withName( "REL" + i ) );
            }
            tx.success();
        }
    }

    private GraphDatabaseService newImpermanentDb()
    {
        return new TestGraphDatabaseFactory().setFileSystem( fsr.get() ).newImpermanentDatabase( storeDir );
    }

    private static int trailerLength( String typeDescriptor )
    {
        String trailer = buildTypeDescriptorAndVersion( typeDescriptor );
        byte[] trailerBytes = encode( trailer );
        return trailerBytes.length;
    }

    public final @Rule EphemeralFileSystemRule fsr = new EphemeralFileSystemRule();
    private final String storeDir = new File( "dir" ).getAbsolutePath();
}
