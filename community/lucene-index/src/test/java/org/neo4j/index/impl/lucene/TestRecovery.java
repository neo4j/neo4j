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
package org.neo4j.index.impl.lucene;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.lang.Thread.State;
import java.util.Map;
import java.util.Random;

import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.index.Index;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.index.Neo4jTestCase;
import org.neo4j.kernel.CommonFactories;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.impl.index.IndexStore;
import org.neo4j.kernel.impl.transaction.xaframework.LogBufferFactory;

/**
 * Don't extend Neo4jTestCase since these tests restarts the db in the tests. 
 */
public class TestRecovery
{
    private String getDbPath()
    {
        return "target/var/recovery";
    }
    
    private GraphDatabaseService newGraphDbService()
    {
        String path = getDbPath();
        Neo4jTestCase.deleteFileOrDirectory( new File( path ) );
        return new EmbeddedGraphDatabase( path );
    }
    
    @Test
    public void testRecovery() throws Exception
    {
        final GraphDatabaseService graphDb = newGraphDbService();
        final Index<Node> nodeIndex = graphDb.index().forNodes( "node-index" );
        final Index<Relationship> relIndex = graphDb.index().forRelationships( "rel-index" );
        final RelationshipType relType = DynamicRelationshipType.withName( "recovery" );
        
        graphDb.beginTx();
        Random random = new Random();
        Thread stopper = new Thread()
        {
            @Override public void run()
            {
                sleepNice( 1000 );
                graphDb.shutdown();
            }
        };
        final String[] keys = { "apoc", "zion", "neo" };
        try
        {
            stopper.start();
            for ( int i = 0; i < 50; i++ )
            {
                Node node = graphDb.createNode();
                Node otherNode = graphDb.createNode();
                Relationship rel = node.createRelationshipTo( otherNode, relType );
                for ( int ii = 0; ii < 3; ii++ )
                {
                    nodeIndex.add( node, keys[random.nextInt( keys.length )], random.nextInt() );
                    relIndex.add( rel, keys[random.nextInt( keys.length )], random.nextInt() );
                }
                sleepNice( 10 );
            }
        }
        catch ( Exception e )
        {
            // Ok
        }
        
        // Wait until the stopper has run, i.e. the graph db is shut down
        while ( stopper.getState() != State.TERMINATED )
        {
            sleepNice( 100 );
        }
        
        // Start up and let it recover
        final GraphDatabaseService newGraphDb = new EmbeddedGraphDatabase( getDbPath() );
        newGraphDb.shutdown();
    }
    
    private static void sleepNice( long time )
    {
        try
        {
            Thread.sleep( time );
        }
        catch ( InterruptedException e )
        {
            // Ok
        }
    }
    
    @Ignore
    @Test
    public void testHardCoreRecovery() throws Exception
    {
    	String path = "target/hcdb";
        Neo4jTestCase.deleteFileOrDirectory( new File( path ) );
    	Process process = Runtime.getRuntime().exec( new String[] {
    			"java", "-cp", System.getProperty( "java.class.path" ),
    			Inserter.class.getName(), path
    	} );
    	
    	// Let it run for a while and then kill it, and wait for it to die
    	Thread.sleep( 6000 );
    	process.destroy();
    	process.waitFor();
    	
    	GraphDatabaseService db = new EmbeddedGraphDatabase( path );
    	assertTrue( db.index().existsForNodes( "myIndex" ) );
    	Index<Node> index = db.index().forNodes( "myIndex" );
    	for ( Node node : db.getAllNodes() )
    	{
    	    for ( String key : node.getPropertyKeys() )
    	    {
    	        String value = (String) node.getProperty( key );
    	        boolean found = false;
    	        for ( Node indexedNode : index.get( key, value ) )
                {
    	            if ( indexedNode.equals( node ) )
    	            {
    	                found = true;
    	                break;
    	            }
                }
    	        if ( !found )
    	        {
    	            throw new IllegalStateException( node + " has property '" + key + "'='" +
    	                    value + "', but not in index" );
    	        }
    	    }
    	}
    	db.shutdown();
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
        new EmbeddedGraphDatabase( getDbPath() ).shutdown();
    }
    
    @Test
    public void testIndexDeleteIssue() throws Exception
    {
        GraphDatabaseService db = newGraphDbService();
        db.index().forNodes( "index" );
        db.shutdown();
        
        assertEquals( 0, Runtime.getRuntime().exec( new String[] { "java", "-cp", System.getProperty( "java.class.path" ),
                AddDeleteQuit.class.getName(), getDbPath() } ).waitFor() );
        
        new EmbeddedGraphDatabase( getDbPath() ).shutdown();
        db.shutdown();
    }

    @Test
    public void recoveryForRelationshipCommandsOnly() throws Exception
    {
        String path = getDbPath();
        Neo4jTestCase.deleteFileOrDirectory( new File( path ) );
        assertEquals( 0, Runtime.getRuntime().exec( new String[] { "java", "-cp", System.getProperty( "java.class.path" ),
                AddRelToIndex.class.getName(), getDbPath() } ).waitFor() );
        
        // I would like to do this, but there's no exception propagated out from the constructor
        // if the recovery fails.
        // new EmbeddedGraphDatabase( getDbPath() ).shutdown();
        
        // Instead I have to do this
        Map<Object, Object> params = MapUtil.genericMap(
                "store_dir", getDbPath(),
                IndexStore.class, new IndexStore( getDbPath() ),
                LogBufferFactory.class, CommonFactories.defaultLogBufferFactory() );
        LuceneDataSource ds = new LuceneDataSource( params );
        ds.close();
    }
}
