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
package org.neo4j.kernel.impl.storemigration;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.graphdb.DynamicRelationshipType.withName;
import static org.neo4j.kernel.CommonFactories.defaultFileSystemAbstraction;
import static org.neo4j.kernel.CommonFactories.defaultIdGeneratorFactory;
import static org.neo4j.kernel.CommonFactories.defaultTxHook;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.nioneo.store.DefaultWindowPoolFactory;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.store.PropertyType;
import org.neo4j.kernel.impl.nioneo.store.StoreFactory;
import org.neo4j.kernel.impl.storemigration.legacystore.LegacyStore;
import org.neo4j.kernel.impl.storemigration.monitoring.MigrationProgressMonitor;
import org.neo4j.kernel.impl.util.FileUtils;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.tooling.GlobalGraphOperations;

public class StoreMigratorTestIT
{
    @Test
    public void shouldMigrate() throws IOException
    {
        URL legacyStoreResource = getClass().getResource( "legacystore/exampledb/neostore" );

        LegacyStore legacyStore = new LegacyStore( legacyStoreResource.getFile(), StringLogger.DEV_NULL );

        Config config = MigrationTestUtils.defaultConfig();
        File outputDir = new File( "target/outputDatabase" );
        FileUtils.deleteRecursively( outputDir );
        assertTrue( outputDir.mkdirs() );

        String storeFileName = "target/outputDatabase/neostore";
        StoreFactory factory = new StoreFactory( config, defaultIdGeneratorFactory(),
                new DefaultWindowPoolFactory(), defaultFileSystemAbstraction(), StringLogger.DEV_NULL, defaultTxHook() );
        NeoStore neoStore = factory.createNeoStore( storeFileName );

        ListAccumulatorMigrationProgressMonitor monitor = new ListAccumulatorMigrationProgressMonitor();

        new StoreMigrator( monitor ).migrate( legacyStore, neoStore );

        verifyNeoStore( neoStore);

        neoStore.close();

        assertEquals( 100, monitor.events.size() );
        assertTrue( monitor.started );
        assertTrue( monitor.finished );

        GraphDatabaseService database = new EmbeddedGraphDatabase( outputDir.getPath() );

        DatabaseContentVerifier verifier = new DatabaseContentVerifier( database );
        verifier.verifyNodes();
        verifier.verifyRelationships();
        verifier.verifyNodeIdsReused();
        verifier.verifyRelationshipIdsReused();

        database.shutdown();
    }

    private void verifyNeoStore( NeoStore neoStore )
    {
        assertEquals( 1317392957120l, neoStore.getCreationTime() );
        assertEquals( -472309512128245482l, neoStore.getRandomNumber() );
        assertEquals( 0l, neoStore.getVersion() );
        assertEquals( 1004l, neoStore.getLastCommittedTx() );
    }

    private static class DatabaseContentVerifier
    {
        private String longString = MigrationTestUtils.makeLongString();
        private int[] longArray = MigrationTestUtils.makeLongArray();
        private GraphDatabaseService database;

        public DatabaseContentVerifier( GraphDatabaseService database )
        {
            this.database = database;
        }

        private void verifyRelationships()
        {
            Node currentNode = database.getReferenceNode();
            int traversalCount = 0;
            while ( currentNode.hasRelationship( Direction.OUTGOING ) )
            {
                traversalCount++;
                Relationship relationship = currentNode.getRelationships( Direction.OUTGOING ).iterator().next();
                verifyProperties( relationship );
                currentNode = relationship.getEndNode();
            }
            assertEquals( 500, traversalCount );
        }

        private void verifyNodes()
        {
            int nodeCount = 0;
            for ( Node node : GlobalGraphOperations.at( database ).getAllNodes() )
            {
                nodeCount++;
                if ( node.getId() > 0 )
                {
                    verifyProperties( node );
                }
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
            try
            {
                database.getNodeById( 1 );
                fail( "Node 2 should not exist" );
            } catch ( NotFoundException e )
            {
                //expected
            }
            Transaction transaction = database.beginTx();
            try
            {
                Node newNode = database.createNode();
                assertEquals( 1, newNode.getId() );
                transaction.success();
            } finally
            {
                transaction.finish();
            }
        }

        private void verifyRelationshipIdsReused()
        {
            Transaction transaction = database.beginTx();
            try
            {
                Node node1 = database.createNode();
                Node node2 = database.createNode();
                Relationship relationship1 = node1.createRelationshipTo( node2, withName( "REUSE" ) );
                assertEquals( 0, relationship1.getId() );
                transaction.success();
            } finally
            {
                transaction.finish();
            }
        }

    }

    private class ListAccumulatorMigrationProgressMonitor implements MigrationProgressMonitor
    {
        private List<Integer> events = new ArrayList<Integer>();
        private boolean started = false;
        private boolean finished = false;

        public void started()
        {
            started = true;
        }

        public void percentComplete( int percent )
        {
            events.add( percent );
        }

        public void finished()
        {
            finished = true;
        }
    }
}
