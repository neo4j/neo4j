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
package org.neo4j.kernel.impl.nioneo.store;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.impl.transaction.XaDataSourceManager;
import org.neo4j.test.EphemeralFileSystemRule;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.junit.Assert.assertEquals;

import static org.neo4j.helpers.UTF8.encode;
import static org.neo4j.kernel.impl.nioneo.store.CommonAbstractStore.buildTypeDescriptorAndVersion;

public class StoreHighIdInflationTest
{
    @Test
    public void shouldFindActualHighIdWhenStartingOnInflatedStore() throws Exception
    {
        // GIVEN
        FileSystemAbstraction fs = fsr.get();
        fs.mkdirs( new File( storeDir ) );
        GraphDatabaseAPI db = (GraphDatabaseAPI)
                new TestGraphDatabaseFactory().setFileSystem( fs ).newImpermanentDatabase( storeDir );
        long highestCreatedNodeId = createACoupleOfNodes( db, 3 );
        long highestPropertyStringRecord = getHighestDynamicStringPropertyId( db );
        db.shutdown();
        inflateStore( NodeStore.TYPE_DESCRIPTOR, NodeStore.RECORD_SIZE, ".nodestore.db" );
        inflateStore( DynamicStringStore.TYPE_DESCRIPTOR, DynamicStringStore.BLOCK_HEADER_SIZE, ".nodestore.db" );

        // WHEN
        db = (GraphDatabaseAPI)
                new TestGraphDatabaseFactory().setFileSystem( fs ).newImpermanentDatabase( storeDir );
        long nodeIdAfterInflation = createACoupleOfNodes( db, 1 );
        long stringPropertyIdAfterInflation = getHighestDynamicStringPropertyId( db );
        db.shutdown();

        // THEN
        assertEquals( highestCreatedNodeId+1, nodeIdAfterInflation );
        assertEquals( highestPropertyStringRecord+1, stringPropertyIdAfterInflation );
    }

    private long getHighestDynamicStringPropertyId( GraphDatabaseAPI db )
    {
        return db.getDependencyResolver().resolveDependency( XaDataSourceManager.class )
                .getNeoStoreDataSource().getNeoStore().getPropertyStore()
                .getStringStore().getHighestPossibleIdInUse();
    }

    private void inflateStore( String trailerTypeDescriptor, int recordSize, String store ) throws IOException
    {
        int trailerLength = encode( buildTypeDescriptorAndVersion( trailerTypeDescriptor ) ).length;
        File neoStore = new File( storeDir, NeoStore.DEFAULT_NAME );
        File storeFile = new File( neoStore.getAbsolutePath() + store );
        FileSystemAbstraction fs = fsr.get();
        StoreChannel channel = fs.open( storeFile, "rw" );
        try
        {
            channel.position( channel.size() - trailerLength );
            channel.write( megabyteWorthOfZeros() );
        }
        finally
        {
            channel.close();
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

    private long createACoupleOfNodes( GraphDatabaseService db, int count )
    {
        Transaction tx = db.beginTx();
        try
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
        finally
        {
            tx.finish();
        }
    }

    public final @Rule EphemeralFileSystemRule fsr = new EphemeralFileSystemRule();
    private final String storeDir = new File( "dir" ).getAbsolutePath();
}
