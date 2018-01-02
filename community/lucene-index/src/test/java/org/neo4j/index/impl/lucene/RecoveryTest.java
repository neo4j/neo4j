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
package org.neo4j.index.impl.lucene;

import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.RelationshipIndex;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.index.IndexConfigStore;
import org.neo4j.test.DatabaseRule;
import org.neo4j.test.EmbeddedDatabaseRule;
import org.neo4j.test.ProcessStreamHandler;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static org.neo4j.graphdb.DynamicRelationshipType.withName;

/**
 * Don't extend Neo4jTestCase since these tests restarts the db in the tests.
 */
public class RecoveryTest
{
    @Rule
    public DatabaseRule db = new EmbeddedDatabaseRule();

    private void shutdownDB()
    {
        db.stopAndKeepFiles();
    }

    private void startDB()
    {
        db.getGraphDatabaseService(); // will ensure started
    }

    private void forceRecover() throws IOException
    {
        db.restartDatabase();
    }

    @Test
    public void testRecovery() throws Exception
    {
        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.createNode();
            Node otherNode = db.createNode();
            Relationship rel = node.createRelationshipTo( otherNode, withName( "recovery" ) );
            db.index().forNodes( "node-index" ).add( node, "key1", "string value" );
            db.index().forNodes( "node-index" ).add( node, "key2", 12345 );
            db.index().forRelationships( "rel-index" ).add( rel, "key1", "string value" );
            db.index().forRelationships( "rel-index" ).add( rel, "key2", 12345 );
            tx.success();
        }

        forceRecover();
    }

    @Test
    public void shouldNotAcceptValuesWithNullToString() throws Exception
    {
        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.createNode();
            Node otherNode = db.createNode();
            Relationship rel = node.createRelationshipTo( otherNode, withName( "recovery" ) );
            Index<Node> nodeIndex = db.index().forNodes( "node-index" );
            RelationshipIndex relationshipIndex = db.index().forRelationships( "rel-index" );

            // Add
            assertAddFailsWithIllegalArgument( nodeIndex, node, "key1", new ClassWithToStringAlwaysNull() );
            assertAddFailsWithIllegalArgument( relationshipIndex, rel, "key1", new ClassWithToStringAlwaysNull() );

            // Remove
            assertRemoveFailsWithIllegalArgument( nodeIndex, node, "key1", new ClassWithToStringAlwaysNull() );
            assertRemoveFailsWithIllegalArgument( relationshipIndex, rel, "key1", new ClassWithToStringAlwaysNull() );
            tx.success();
	}

        forceRecover();
    }

    @Test
    public void testAsLittleAsPossibleRecoveryScenario() throws Exception
    {
        try ( Transaction tx = db.beginTx() )
        {
            db.index().forNodes( "my-index" ).add( db.createNode(), "key", "value" );
            tx.success();
        }

        forceRecover();
    }

    @Test
    public void testIndexDeleteIssue() throws Exception
    {
        try ( Transaction tx = db.beginTx() )
        {
            db.index().forNodes( "index" );
            tx.success();
        }

        String storeDir = db.getStoreDir();
        shutdownDB();

        // NB: AddDeleteQuit will start and shutdown the db
        final Process process = Runtime.getRuntime().exec( new String[]{
                "java",
                "-cp", System.getProperty( "java.class.path" ),
                AddDeleteQuit.class.getName(),
                storeDir
        } );

        int result = new ProcessStreamHandler( process, true ).waitForResult();

        assertEquals( 0, result );

        startDB();

        forceRecover();
    }

    @Test
    public void recoveryForRelationshipCommandsOnly() throws Throwable
    {
        // shutdown db here
        String storeDir = db.getStoreDir();
        File path = new File( storeDir );
        shutdownDB();

        // NB: AddRelToIndex will start and shutdown the db
        Process process = Runtime.getRuntime().exec( new String[]{
                "java", "-cp", System.getProperty( "java.class.path" ),
                AddRelToIndex.class.getName(), storeDir
        } );
        assertEquals( 0, new ProcessStreamHandler( process, false ).waitForResult() );

        FileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction();
        Config config = new Config( MapUtil.stringMap(), GraphDatabaseSettings.class );
        LuceneDataSource ds = new LuceneDataSource( path, config, new IndexConfigStore( path, fileSystem ), fileSystem );
        ds.start();
        ds.stop();
    }

    @Test
    public void recoveryOnDeletedIndex() throws Exception
    {
        try ( Transaction tx = db.beginTx() )
        {
            db.index().forNodes( "index" );
            tx.success();
        }

        // shutdown db here
        String storeDir = db.getStoreDir();
        shutdownDB();

        // NB: AddThenDeleteInAnotherTxAndQuit will start and shutdown the db
        Process process = Runtime.getRuntime().exec( new String[]{
                "java",
                "-cp", System.getProperty( "java.class.path" ),
                AddThenDeleteInAnotherTxAndQuit.class.getName(),
                storeDir
        } );
        assertEquals( 0, new ProcessStreamHandler( process, false ).waitForResult() );

        // restart db
        startDB();

        try ( Transaction tx = db.beginTx() )
        {
            assertFalse( db.index().existsForNodes( "index" ) );
            assertNotNull( db.index().forNodes( "index2" ).get( "key", "value" ).getSingle() );
        }

        // db shutdown handled in tearDown...
    }

    public static class AddDeleteQuit
    {
        public static void main( String[] args )
        {
            GraphDatabaseService db = new TestGraphDatabaseFactory().newEmbeddedDatabase( args[0] );
            try ( Transaction tx = db.beginTx() )
            {
                Index<Node> index = db.index().forNodes( "index" );
                index.add( db.createNode(), "key", "value" );
                index.delete();
                tx.success();
            }

            db.shutdown();
            System.exit( 0 );
        }
    }

    public static class AddRelToIndex
    {
        public static void main( String[] args )
        {
            GraphDatabaseService db = new TestGraphDatabaseFactory().newEmbeddedDatabase( args[0] );
            try ( Transaction tx = db.beginTx() )
            {
                Index<Relationship> index = db.index().forRelationships( "myIndex" );
                Node node = db.createNode();
                Relationship relationship = db.createNode().createRelationshipTo( node,
                        DynamicRelationshipType.withName( "KNOWS" ) );

                index.add( relationship, "key", "value" );
                tx.success();
            }

            db.shutdown();
            System.exit( 0 );
        }
    }

    public static class AddThenDeleteInAnotherTxAndQuit
    {
        public static void main( String[] args )
        {
            GraphDatabaseService db = new TestGraphDatabaseFactory().newEmbeddedDatabase( args[0] );

            Index<Node> index;
            Index<Node> index2;
            try ( Transaction tx = db.beginTx() )
            {
                index = db.index().forNodes( "index" );
                index2 = db.index().forNodes( "index2" );
                Node node = db.createNode();
                index.add( node, "key", "value" );
                tx.success();
            }

            try ( Transaction tx = db.beginTx() )
            {
                index.delete();
                index2.add( db.createNode(), "key", "value" );
                tx.success();
            }

            db.shutdown();
            System.exit( 0 );
        }
    }

    static class ClassWithToStringAlwaysNull {

        @Override
        public String toString()
        {
            return null;
        }

    }

    private <ENTITY extends PropertyContainer> void assertAddFailsWithIllegalArgument( Index<ENTITY> index,
            ENTITY entity, String key, Object value )
    {
        try
        {
            index.add( entity, key, value );
            fail( "Should not accept value with null toString" );
        }
        catch ( IllegalArgumentException e )
        {
            // Good
        }
    }

    private <ENTITY extends PropertyContainer> void assertRemoveFailsWithIllegalArgument( Index<ENTITY> index,
            ENTITY entity, String key, Object value )
    {
        try
        {
            index.remove( entity, key, value );
            fail( "Should not accept value with null toString" );
        }
        catch ( IllegalArgumentException e )
        {
            // Good
        }
    }

}
