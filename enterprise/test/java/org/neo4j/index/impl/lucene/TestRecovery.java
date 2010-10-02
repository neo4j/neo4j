/*
 * Copyright (c) 2002-2009 "Neo Technology,"
 *     Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 * 
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.index.impl.lucene;

import java.io.File;
import java.util.Random;

import org.junit.Test;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexProvider;
import org.neo4j.index.Neo4jTestCase;
import org.neo4j.kernel.EmbeddedGraphDatabase;

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
        final IndexProvider provider = new LuceneIndexProvider( graphDb );
        final Index<Node> nodeIndex = provider.nodeIndex( "node-index",
                LuceneIndexProvider.EXACT_CONFIG );
        final Index<Relationship> relIndex = provider.relationshipIndex( "rel-index",
                LuceneIndexProvider.EXACT_CONFIG );
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
        
        sleepNice( 1000 );
        final GraphDatabaseService newGraphDb =
            new EmbeddedGraphDatabase( getDbPath() );
        final IndexProvider newProvider = new LuceneIndexProvider( newGraphDb );
        sleepNice( 1000 );
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
    
    @Test
    public void testHardCoreRecovery() throws Exception
    {
    	String path = "target/hcdb";
        Neo4jTestCase.deleteFileOrDirectory( new File( path ) );
    	Process process = Runtime.getRuntime().exec( new String[] {
    			"java", "-cp", System.getProperty( "java.class.path" ),
    			getClass().getPackage().getName() + ".Inserter", path
    	} );
    	Thread.sleep( 7000 );
    	process.destroy();
    	Thread.sleep( 3000 );
    	new EmbeddedGraphDatabase( path ).shutdown();
    }
}
