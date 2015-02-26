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
package org.neo4j.index.impl.lucene;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.index.Index;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.index.Neo4jTestCase;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.index.IndexConfigStore;
import org.neo4j.test.EmbeddedDatabaseRule;
import org.neo4j.test.ProcessStreamHandler;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.neo4j.graphdb.DynamicRelationshipType.withName;

/**
 * Don't extend Neo4jTestCase since these tests restarts the db in the tests.
 */
public class RecoveryTest
{
    private static final File path = new File( "target/var/recovery" );
    @Rule
    public EmbeddedDatabaseRule dbRule = new EmbeddedDatabaseRule( path );
    private GraphDatabaseService graphDb;

    @Before
    public void setup()
    {
        Neo4jTestCase.deleteFileOrDirectory( path );
        startDB();
    }

    private void shutdownDB()
    {
        dbRule.stopAndKeepFiles();
    }

    private void startDB()
    {
        graphDb = dbRule.getGraphDatabaseService();
    }

    private void forceRecover() throws IOException
    {
        graphDb = dbRule.restartDatabase();
    }

    @Test
    public void testRecovery() throws Exception
    {
        try ( Transaction tx = graphDb.beginTx() )
        {
            Node node = graphDb.createNode();
            Node otherNode = graphDb.createNode();
            Relationship rel = node.createRelationshipTo( otherNode, withName( "recovery" ) );
            graphDb.index().forNodes( "node-index" ).add( node, "key1", "string value" );
            graphDb.index().forNodes( "node-index" ).add( node, "key2", 12345 );
            graphDb.index().forRelationships( "rel-index" ).add( rel, "key1", "string value" );
            graphDb.index().forRelationships( "rel-index" ).add( rel, "key2", 12345 );
            tx.success();
        }

        forceRecover();
    }

    @Test
    public void testAsLittleAsPossibleRecoveryScenario() throws Exception
    {
        try ( Transaction tx = graphDb.beginTx() )
        {
            graphDb.index().forNodes( "my-index" ).add( graphDb.createNode(), "key", "value" );
            tx.success();
        }

        forceRecover();
    }

    @Test
    public void testIndexDeleteIssue() throws Exception
    {
        try ( Transaction tx = graphDb.beginTx() )
        {
            graphDb.index().forNodes( "index" );
            tx.success();
        }

        shutdownDB();

        // NB: AddDeleteQuit will start and shutdown the db
        final Process process = Runtime.getRuntime().exec( new String[]{
                "java",
                "-cp", System.getProperty( "java.class.path" ),
                AddDeleteQuit.class.getName(),
                path.getPath()
        } );

        int result = new ProcessStreamHandler( process, true ).waitForResult();

        assertEquals( 0, result );

        startDB();

        forceRecover();
    }

    @Test
    public void recoveryForRelationshipCommandsOnly() throws Exception
    {
        // shutdown db here
        shutdownDB();

        // NB: AddRelToIndex will start and shutdown the db
        Process process = Runtime.getRuntime().exec( new String[]{
                "java", "-Xdebug", "-Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=5005", "-cp",
                System.getProperty( "java.class.path" ),
                AddRelToIndex.class.getName(), path.getPath()
        } );
        assertEquals( 0, new ProcessStreamHandler( process, false ).waitForResult() );

        FileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction();
        Map<String, String> params = MapUtil.stringMap( "store_dir", path.getPath() );
        Config config = new Config( params, GraphDatabaseSettings.class );
        LuceneDataSource ds = new LuceneDataSource( config, new IndexConfigStore( path, fileSystem ), fileSystem );
        ds.start();
        ds.stop();
    }

    @Test
    public void recoveryOnDeletedIndex() throws Exception
    {
        try ( Transaction tx = graphDb.beginTx() )
        {
            graphDb.index().forNodes( "index" );
            tx.success();
        }

        // shutdown db here
        shutdownDB();

        // NB: AddThenDeleteInAnotherTxAndQuit will start and shutdown the db
        Process process = Runtime.getRuntime().exec( new String[]{
                "java",
                "-cp", System.getProperty( "java.class.path" ),
                AddThenDeleteInAnotherTxAndQuit.class.getName(),
                path.getPath()
        } );
        assertEquals( 0, new ProcessStreamHandler( process, false ).waitForResult() );

        // restart db
        startDB();

        try ( Transaction tx = graphDb.beginTx() )
        {
            assertFalse( graphDb.index().existsForNodes( "index" ) );
            assertNotNull( graphDb.index().forNodes( "index2" ).get( "key", "value" ).getSingle() );
        }

        // db shutdown handled in tearDown...
    }

    public static class AddDeleteQuit
    {
        public static void main( String[] args )
        {
            GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase( args[0] );
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
            GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase( args[0] );
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
            GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase( args[0] );

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
}
