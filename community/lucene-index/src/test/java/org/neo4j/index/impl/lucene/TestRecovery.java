/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.util.Map;

import org.junit.Test;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.index.Index;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.index.Neo4jTestCase;
import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.index.IndexStore;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.transaction.PlaceboTm;
import org.neo4j.kernel.impl.transaction.xaframework.DefaultLogBufferFactory;
import org.neo4j.kernel.impl.transaction.xaframework.LogPruneStrategies;
import org.neo4j.kernel.impl.transaction.xaframework.RecoveryVerifier;
import org.neo4j.kernel.impl.transaction.xaframework.TxIdGenerator;
import org.neo4j.kernel.impl.transaction.xaframework.XaFactory;
import org.neo4j.kernel.logging.DevNullLoggingService;
import org.neo4j.test.ProcessStreamHandler;

/**
 * Don't extend Neo4jTestCase since these tests restarts the db in the tests.
 */
public class TestRecovery
{
    private File getDbPath()
    {
        return new File("target/var/recovery");
    }

    private GraphDatabaseService newGraphDbService()
    {
        File path = getDbPath();
        Neo4jTestCase.deleteFileOrDirectory( path );
        return new GraphDatabaseFactory().newEmbeddedDatabase( path.getPath() );
    }

    @Test
    public void testRecovery() throws Exception
    {
        GraphDatabaseService graphDb = newGraphDbService();
        Index<Node> nodeIndex = graphDb.index().forNodes( "node-index" );
        Index<Relationship> relIndex = graphDb.index().forRelationships( "rel-index" );
        RelationshipType relType = DynamicRelationshipType.withName( "recovery" );

        graphDb.beginTx();
        Node node = graphDb.createNode();
        Node otherNode = graphDb.createNode();
        Relationship rel = node.createRelationshipTo( otherNode, relType );
        nodeIndex.add( node, "key1", "string value" );
        nodeIndex.add( node, "key2", 12345 );
        relIndex.add( rel, "key1", "string value" );
        relIndex.add( rel, "key2", 12345 );
        graphDb.shutdown();

        // Start up and let it recover
        final GraphDatabaseService newGraphDb = new GraphDatabaseFactory().newEmbeddedDatabase( getDbPath().getPath() );
        newGraphDb.shutdown();
    }

    @Test
    public void testAsLittleAsPossibleRecoveryScenario() throws Exception
    {
        GraphDatabaseService db = newGraphDbService();
        Index<Node> index = db.index().forNodes( "my-index" );
        db.beginTx();
        Node node = db.createNode();
        index.add( node, "key", "value" );
        db.shutdown();

        // This doesn't seem to trigger recovery... it really should
        new GraphDatabaseFactory().newEmbeddedDatabase( getDbPath().getPath() ).shutdown();
    }

    @Test
    public void testIndexDeleteIssue() throws Exception
    {
        GraphDatabaseService db = newGraphDbService();
        db.index().forNodes( "index" );
        db.shutdown();

        Process process = Runtime.getRuntime().exec( new String[]{
                "java", "-cp", System.getProperty( "java.class.path" ),
                AddDeleteQuit.class.getName(), getDbPath().getPath()
        } );
        assertEquals( 0, new ProcessStreamHandler( process, true ).waitForResult() );

        new GraphDatabaseFactory().newEmbeddedDatabase( getDbPath().getPath() ).shutdown();
        db.shutdown();
    }

    @Test
    public void recoveryForRelationshipCommandsOnly() throws Exception
    {
        File path = getDbPath();
        Neo4jTestCase.deleteFileOrDirectory( path );
        Process process = Runtime.getRuntime().exec( new String[]{
                "java", "-Xdebug", "-Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=5005", "-cp",
                System.getProperty( "java.class.path" ),
                AddRelToIndex.class.getName(), getDbPath().getPath()
        } );
        assertEquals( 0, new ProcessStreamHandler( process, true ).waitForResult() );

        // I would like to do this, but there's no exception propagated out from the constructor
        // if the recovery fails.
        // new EmbeddedGraphDatabase( getDbPath() ).shutdown();

        // Instead I have to do this
        FileSystemAbstraction fileSystemAbstraction = new DefaultFileSystemAbstraction();
        FileSystemAbstraction fileSystem = fileSystemAbstraction;
        Map<String, String> params = MapUtil.stringMap(
                "store_dir", getDbPath().getPath() );
        Config config = new Config( params, GraphDatabaseSettings.class );
        LuceneDataSource ds = new LuceneDataSource( config, new IndexStore( getDbPath(), fileSystem ), fileSystem,
                new XaFactory( TxIdGenerator.DEFAULT, new PlaceboTm( null, null ), new DefaultLogBufferFactory(),
                        fileSystemAbstraction, new DevNullLoggingService(), RecoveryVerifier.ALWAYS_VALID,
                        LogPruneStrategies.NO_PRUNING ) );
        ds.start();
        ds.stop();
    }

    @Test
    public void recoveryOnDeletedIndex() throws Exception
    {
        GraphDatabaseService db = newGraphDbService();
        db.index().forNodes( "index" );
        db.shutdown();

        Process process = Runtime.getRuntime().exec( new String[]{
                "java", "-cp", System.getProperty( "java.class.path" ),
                AddThenDeleteInAnotherTxAndQuit.class.getName(), getDbPath().getPath()
        } );
        assertEquals( 0, new ProcessStreamHandler( process, true ).waitForResult() );

        db = new GraphDatabaseFactory().newEmbeddedDatabase( getDbPath().getPath() );
        Transaction transaction = db.beginTx();
        assertFalse( db.index().existsForNodes( "index" ) );
        assertNotNull( db.index().forNodes( "index2" ).get( "key", "value" ).getSingle() );
        transaction.finish();
        db.shutdown();
    }
}
