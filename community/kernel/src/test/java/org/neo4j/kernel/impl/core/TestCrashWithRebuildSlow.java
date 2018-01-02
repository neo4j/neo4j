/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.impl.core;

import org.hamcrest.Matchers;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.helpers.collection.Visitor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.impl.MyRelTypes;
import org.neo4j.kernel.impl.store.CommonAbstractStore;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.transaction.state.DataSourceManager;
import org.neo4j.test.EphemeralFileSystemRule;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.TargetDirectory.TestDirectory;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.tooling.GlobalGraphOperations;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import static org.neo4j.graphdb.Neo4jMatchers.hasProperty;
import static org.neo4j.graphdb.Neo4jMatchers.inTx;
import static org.neo4j.helpers.collection.IteratorUtil.count;
import static org.neo4j.test.EphemeralFileSystemRule.shutdownDbAction;

/**
 * Test for making sure that slow id generator rebuild is exercised
 */
public class TestCrashWithRebuildSlow
{
    @Rule
    public EphemeralFileSystemRule fs = new EphemeralFileSystemRule();
    // for dumping data about failing build
    public final @Rule TestDirectory testDir = TargetDirectory.testDirForTest( getClass() );

    private static List<Long> produceNonCleanDefraggedStringStore( GraphDatabaseService db )
    {
        // Create some strings
        List<Node> nodes = new ArrayList<>();
        try ( Transaction tx = db.beginTx() )
        {
            Node previous = null;
            for ( int i = 0; i < 20; i++ )
            {
                Node node = db.createNode();
                node.setProperty( "name",
                        "a looooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooong" +
                        " string" );
                nodes.add( node );
                if ( previous != null )
                {
                    previous.createRelationshipTo( node, MyRelTypes.TEST );
                }
                previous = node;
            }
            tx.success();
        }

        // Delete some of them, but leave some in between deletions
        List<Long> deletedNodeIds = new ArrayList<>();
        try ( Transaction tx = db.beginTx() )
        {
            Node a = nodes.get( 5 );
            Node b = nodes.get( 7 );
            Node c = nodes.get( 8 );
            Node d = nodes.get( 10 );
            deletedNodeIds.add( a.getId() );
            deletedNodeIds.add( b.getId() );
            deletedNodeIds.add( c.getId() );
            deletedNodeIds.add( d.getId() );
            delete( a );
            delete( b );
            delete( c );
            delete( d );
            tx.success();
        }
        return deletedNodeIds;
    }

    private static void delete( Node node )
    {
        for ( Relationship rel : node.getRelationships() )
        {
            rel.delete();
        }
        node.delete();
    }

    @Test
    public void crashAndRebuildSlowWithDynamicStringDeletions() throws Exception
    {
        File storeDir = new File( "dir" ).getAbsoluteFile();
        final GraphDatabaseAPI db = (GraphDatabaseAPI) new TestGraphDatabaseFactory()
                .setFileSystem( fs.get() ).newImpermanentDatabase( storeDir );
        List<Long> deletedNodeIds = produceNonCleanDefraggedStringStore( db );
        Map<IdType,Long> highIdsBeforeCrash = getHighIds( db );

        long checksumBefore = fs.get().checksum();
        long checksumBefore2 = fs.get().checksum();

        assertThat( checksumBefore, Matchers.equalTo( checksumBefore2 ) );

        EphemeralFileSystemAbstraction snapshot = fs.snapshot( shutdownDbAction( db ) );

        long snapshotChecksum = snapshot.checksum();
        if ( snapshotChecksum != checksumBefore )
        {
            try ( OutputStream out = new FileOutputStream( testDir.file( "snapshot.zip" ) ) )
            {
                snapshot.dumpZip( out );
            }
            try ( OutputStream out = new FileOutputStream( testDir.file( "fs.zip" ) ) )
            {
                fs.get().dumpZip( out );
            }
        }
        assertThat( snapshotChecksum, equalTo( checksumBefore ) );

        // Recover with rebuild_idgenerators_fast=false
        assertNumberOfFreeIdsEquals( storeDir, snapshot, 0 );
        GraphDatabaseAPI newDb = (GraphDatabaseAPI) new TestGraphDatabaseFactory()
                .setFileSystem( snapshot )
                .newImpermanentDatabaseBuilder( storeDir )
                .setConfig( GraphDatabaseSettings.rebuild_idgenerators_fast, Settings.FALSE )
                .newGraphDatabase();
        Map<IdType,Long> highIdsAfterCrash = getHighIds( newDb );
        assertEquals( highIdsBeforeCrash, highIdsAfterCrash );

        try ( Transaction tx = newDb.beginTx() )
        {
            // Verify that the data we didn't delete is still around
            int nameCount = 0;
            int relCount = 0;
            for ( Node node : GlobalGraphOperations.at( newDb ).getAllNodes() )
            {
                nameCount++;
                assertThat( node, inTx( newDb, hasProperty( "name" ), true ) );
                relCount += count( node.getRelationships( Direction.OUTGOING ) );
            }

            assertEquals( 16, nameCount );
            assertEquals( 12, relCount );

            // Verify that the ids of the nodes we deleted are reused
            List<Long> newIds = new ArrayList<>();
            newIds.add( newDb.createNode().getId() );
            newIds.add( newDb.createNode().getId() );
            newIds.add( newDb.createNode().getId() );
            newIds.add( newDb.createNode().getId() );
            assertThat( newIds, is( deletedNodeIds ) );
            tx.success();
        }
        finally
        {
            newDb.shutdown();
        }
    }

    private Map<IdType,Long> getHighIds( GraphDatabaseAPI db )
    {
        final Map<IdType,Long> highIds = new HashMap<>();
        NeoStores neoStores = db.getDependencyResolver().resolveDependency(
                DataSourceManager.class ).getDataSource().getNeoStores();
        neoStores.visitStore( new Visitor<CommonAbstractStore,RuntimeException>()
        {
            @Override
            public boolean visit( CommonAbstractStore store ) throws RuntimeException
            {
                highIds.put( store.getIdType(), store.getHighId() );
                return true;
            }
        } );
        return highIds;
    }

    private void assertNumberOfFreeIdsEquals( File storeDir, FileSystemAbstraction fs, long numberOfFreeIds )
    {
        long fileSize = fs.getFileSize( new File( storeDir, "neostore.propertystore.db.strings.id" ) );
        long fileSizeWithoutHeader = fileSize - 9;
        long actualFreeIds = fileSizeWithoutHeader / 8;

        assertThat( "Id file should at least have a 9 byte header",
                fileSize, greaterThanOrEqualTo( 9L ) );
        assertThat( "File should contain the expected number of free ids",
                actualFreeIds, is( numberOfFreeIds ) );
        assertThat( "File size should not contain more bytes than expected",
                8 * numberOfFreeIds, is( fileSizeWithoutHeader ) );
    }
}
