/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.kernel.DefaultIdGeneratorFactory;
import org.neo4j.kernel.DefaultTxHook;
import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.core.Token;
import org.neo4j.kernel.impl.nioneo.store.DefaultWindowPoolFactory;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.store.PropertyKeyTokenStore;
import org.neo4j.kernel.impl.nioneo.store.PropertyType;
import org.neo4j.kernel.impl.nioneo.store.StoreFactory;
import org.neo4j.kernel.impl.storemigration.legacystore.LegacyStore;
import org.neo4j.kernel.impl.storemigration.monitoring.MigrationProgressMonitor;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.test.CleanupRule;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.Unzip;
import org.neo4j.tooling.GlobalGraphOperations;

import static java.lang.Integer.MAX_VALUE;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import static org.neo4j.graphdb.DynamicRelationshipType.withName;
import static org.neo4j.graphdb.Neo4jMatchers.hasProperty;
import static org.neo4j.graphdb.Neo4jMatchers.inTx;
import static org.neo4j.kernel.impl.nioneo.store.CommonAbstractStore.ALL_STORES_VERSION;
import static org.neo4j.kernel.impl.nioneo.store.NeoStore.versionLongToString;
import static org.neo4j.kernel.impl.nioneo.store.StoreFactory.PROPERTY_KEY_TOKEN_STORE_NAME;
import static org.neo4j.kernel.impl.storemigration.MigrationTestUtils.findOldFormatStoreDirectory;
import static org.neo4j.kernel.impl.storemigration.UpgradeConfiguration.ALLOW_UPGRADE;

public class StoreMigratorIT
{
    @Test
    public void shouldMigrate() throws IOException
    {
        // WHEN
        upgrader( new StoreMigrator( monitor, fs ) )
                .migrateIfNeeded( findOldFormatStoreDirectory( storeDir ) );

        // THEN
        assertEquals( 100, monitor.events.size() );
        assertTrue( monitor.started );
        assertTrue( monitor.finished );

        GraphDatabaseService database = cleanup.add( new GraphDatabaseFactory().newEmbeddedDatabase(
                storeDir.getAbsolutePath() ) );

        DatabaseContentVerifier verifier = new DatabaseContentVerifier( database );
        verifier.verifyNodes();
        verifier.verifyRelationships();
        verifier.verifyNodeIdsReused();
        verifier.verifyRelationshipIdsReused();
        verifier.verifyLegacyIndex();

        database.shutdown();

        NeoStore neoStore = cleanup.add( storeFactory.newNeoStore( storeFileName ) );
        verifyNeoStore( neoStore );
        neoStore.close();
    }

    private StoreUpgrader upgrader( StoreMigrator storeMigrator )
    {
        StoreUpgrader upgrader = new StoreUpgrader( ALLOW_UPGRADE, fs, StoreUpgrader.NO_MONITOR );
        upgrader.addParticipant( storeMigrator );
        return upgrader;
    }

    @Test
    public void shouldDedupUniquePropertyIndexKeys() throws Exception
    {
        // GIVEN
        // a store that contains two nodes with property "name" of which there are two key tokens
        // that should be merged in the store migration
        // WHEN
        Unzip.unzip( LegacyStore.class, "propkeydupdb.zip", storeDir );
        upgrader( new StoreMigrator( monitor, fs ) ).migrateIfNeeded( storeDir );

        // THEN
        // verify that the "name" property for both the involved nodes
        GraphDatabaseService db = cleanup.add( new GraphDatabaseFactory().newEmbeddedDatabase(
                storeDir.getAbsolutePath() ) );
        Node nodeA = getNodeWithName( db, "A" );
        assertThat( nodeA, inTx( db, hasProperty( "name" ).withValue( "A" ) ) );

        Node nodeB = getNodeWithName( db, "B" );
        assertThat( nodeB, inTx( db, hasProperty( "name" ).withValue( "B" ) ) );

        Node nodeC = getNodeWithName( db, "C" );
        assertThat( nodeC, inTx( db, hasProperty( "name" ).withValue( "C" )  ) );
        assertThat( nodeC, inTx( db, hasProperty( "other" ).withValue( "a value" ) ) );
        assertThat( nodeC, inTx( db, hasProperty( "third" ).withValue( "something" ) ) );
        db.shutdown();

        // THEN
        // verify that there are no duplicate keys in the store
        PropertyKeyTokenStore tokenStore = cleanup.add(
                storeFactory.newPropertyKeyTokenStore( new File( storeFileName + PROPERTY_KEY_TOKEN_STORE_NAME ) ) );
        Token[] tokens = tokenStore.getTokens( MAX_VALUE );
        tokenStore.close();
        assertNuDuplicates( tokens );
    }

    @Test
    public void shouldMigrateEmptyDb() throws Exception
    {
        // GIVEN a store that is merely created, no additional data at all
        Unzip.unzip( LegacyStore.class, "emptydb.zip", storeDir );

        // WHEN migrating that to the new version
        upgrader( new StoreMigrator( monitor, fs ) ).migrateIfNeeded( storeDir );

        // THEN it should result in an updated database with the new store format
        GraphDatabaseService db = cleanup.add( new GraphDatabaseFactory().newEmbeddedDatabase(
                storeDir.getAbsolutePath() ) );
        // TODO assert stuff, no?
        db.shutdown();
    }

    private void assertNuDuplicates( Token[] tokens )
    {
        Set<String> visited = new HashSet<>();
        for ( Token token : tokens )
        {
            assertTrue( visited.add( token.name() ) );
        }
    }

    private Node getNodeWithName( GraphDatabaseService db, String name )
    {
        Transaction tx = db.beginTx();
        try
        {
            for ( Node node : GlobalGraphOperations.at( db ).getAllNodes() )
            {
                if ( name.equals( node.getProperty( "name", null ) ) )
                {
                    tx.success();
                    return node;
                }
            }
        }
        finally
        {
            tx.finish();
        }
        throw new IllegalArgumentException( name + " not found" );
    }

    private final FileSystemAbstraction fs = new DefaultFileSystemAbstraction();
    private final File storeDir = TargetDirectory.forTest( getClass() ).makeGraphDbDir();
    private final ListAccumulatorMigrationProgressMonitor monitor = new ListAccumulatorMigrationProgressMonitor();
    private final IdGeneratorFactory idGeneratorFactory = new DefaultIdGeneratorFactory();
    private StoreFactory storeFactory;
    private File storeFileName;

    @Before
    public void setUp()
    {
        Config config = MigrationTestUtils.defaultConfig();
        storeFileName = new File( storeDir, NeoStore.DEFAULT_NAME );
        storeFactory = new StoreFactory( config, idGeneratorFactory,
                new DefaultWindowPoolFactory(), fs, StringLogger.DEV_NULL, new DefaultTxHook(), new Monitors() );
    }

    private void verifyNeoStore( NeoStore neoStore )
    {
        assertEquals( 1317392957120L, neoStore.getCreationTime() );
        assertEquals( -472309512128245482l, neoStore.getRandomNumber() );
        assertEquals( 3l, neoStore.getVersion() );
        assertEquals( ALL_STORES_VERSION, versionLongToString( neoStore.getStoreVersion() ) );
        assertEquals( 1007l, neoStore.getLastCommittedTx() );
    }

    private static class DatabaseContentVerifier
    {
        private final String longString = MigrationTestUtils.makeLongString();
        private final int[] longArray = MigrationTestUtils.makeLongArray();
        private final GraphDatabaseService database;

        public DatabaseContentVerifier( GraphDatabaseService database )
        {
            this.database = database;
        }

        private void verifyRelationships()
        {
            try ( Transaction tx = database.beginTx() )
            {
                int traversalCount = 0;
                for ( Relationship rel : GlobalGraphOperations.at( database ).getAllRelationships() )
                {
                    traversalCount++;
                    verifyProperties( rel );
                }
                tx.success();
                assertEquals( 500, traversalCount );
            }
        }

        private void verifyNodes()
        {
            int nodeCount = 0;
            try ( Transaction tx = database.beginTx() )
            {
                for ( Node node : GlobalGraphOperations.at( database ).getAllNodes() )
                {
                    nodeCount++;
                    if ( node.getId() > 0 )
                    {
                        verifyProperties( node );
                    }
                }
                tx.success();
            }
            assertEquals( 501, nodeCount );
        }

        private void verifyProperties( PropertyContainer node )
        {
            assertEquals( Integer.MAX_VALUE, node.getProperty( PropertyType.INT.name() ) );
            assertEquals( longString, node.getProperty( PropertyType.STRING.name() ) );
            assertEquals( true, node.getProperty( PropertyType.BOOL.name() ) );
            assertEquals( Double.MAX_VALUE, node.getProperty( PropertyType.DOUBLE.name() ) );
            assertEquals( Float.MAX_VALUE, node.getProperty( PropertyType.FLOAT.name() ) );
            assertEquals( Long.MAX_VALUE, node.getProperty( PropertyType.LONG.name() ) );
            assertEquals( Byte.MAX_VALUE, node.getProperty( PropertyType.BYTE.name() ) );
            assertEquals( Character.MAX_VALUE, node.getProperty( PropertyType.CHAR.name() ) );
            assertArrayEquals( longArray, (int[]) node.getProperty( PropertyType.ARRAY.name() ) );
            assertEquals( Short.MAX_VALUE, node.getProperty( PropertyType.SHORT.name() ) );
            assertEquals( "short", node.getProperty( PropertyType.SHORT_STRING.name() ) );
        }

        private void verifyNodeIdsReused()
        {
            try ( Transaction transaction = database.beginTx() )
            {
                database.getNodeById( 1 );
                fail( "Node 1 should not exist" );
            }
            catch ( NotFoundException e )
            {   // expected
            }

            try ( Transaction transaction = database.beginTx() )
            {
                Node newNode = database.createNode();
                assertEquals( 1, newNode.getId() );
                transaction.success();
            }
        }

        private void verifyRelationshipIdsReused()
        {
            try ( Transaction transaction = database.beginTx() )
            {
                Node node1 = database.createNode();
                Node node2 = database.createNode();
                Relationship relationship1 = node1.createRelationshipTo( node2, withName( "REUSE" ) );
                assertEquals( 0, relationship1.getId() );
                transaction.success();
            }
        }

        public void verifyLegacyIndex()
        {
            try ( Transaction tx = database.beginTx() )
            {
                String[] nodeIndexes = database.index().nodeIndexNames();
                String[] relationshipIndexes = database.index().relationshipIndexNames();
                assertArrayEquals( new String[] { "nodekey" }, nodeIndexes );
                assertArrayEquals( new String[] { "relkey" }, relationshipIndexes );
                tx.success();
            }
        }
    }

    private class ListAccumulatorMigrationProgressMonitor implements MigrationProgressMonitor
    {
        private final List<Integer> events = new ArrayList<>();
        private boolean started = false;
        private boolean finished = false;

        @Override
        public void started()
        {
            started = true;
        }

        @Override
        public void percentComplete( int percent )
        {
            events.add( percent );
        }

        @Override
        public void finished()
        {
            finished = true;
        }
    }

    public final @Rule CleanupRule cleanup = new CleanupRule();
}
