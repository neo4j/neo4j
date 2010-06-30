package org.neo4j.index.impl.lucene;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.neo4j.index.Neo4jTestCase.assertCollection;

import java.io.File;
import java.util.Random;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.neo4j.index.Neo4jTestCase;
import org.neo4j.index.impl.lucene.LuceneIndexProvider;
import org.neo4j.kernel.EmbeddedGraphDatabase;

public class TestNewLuceneIndex
{
    private static Random random;
    private static GraphDatabaseService graphDb;
    private static LuceneIndexProvider provider;
    private Transaction tx;
    
    @BeforeClass
    public static void setUpStuff()
    {
        String storeDir = "target/var/freshindex";
        Neo4jTestCase.deleteFileOrDirectory( new File( storeDir ) );
        graphDb = new EmbeddedGraphDatabase( storeDir );
        provider = new LuceneIndexProvider( graphDb );
    }
    
    @AfterClass
    public static void tearDownStuff()
    {
        graphDb.shutdown();
    }
    
    @After
    public void commitTx()
    {
        if ( tx != null )
        {
            tx.success();
            tx.finish();
            tx = null;
        }
    }
    
    @Before
    public void beginTx()
    {
        if ( tx == null )
        {
            tx = graphDb.beginTx();
        }
    }
    
    void restartTx()
    {
        commitTx();
        beginTx();
    }
    
    @Test
    public void testDefaultNodeIndex()
    {
        String name = "name";
        String mattias = "Mattias Persson";
        String title = "title";
        String hacker = "Hacker";
        
        Index<Node> nodeIndex = provider.nodeIndex( "default", null );
        assertCollection( nodeIndex.get( name, mattias ) );
        
        Node node1 = graphDb.createNode();
        Node node2 = graphDb.createNode();
        
        assertNull( nodeIndex.get( name, mattias ).getSingle() );
        nodeIndex.add( node1, name, mattias );
        assertCollection( nodeIndex.get( name, mattias ), node1 );
        assertCollection( nodeIndex.query( name, "\"" + mattias + "\"" ), node1 );
        assertCollection( nodeIndex.query( "name:\"" + mattias + "\"" ), node1 );
        assertEquals( node1, nodeIndex.get( name, mattias ).getSingle() );
        assertCollection( nodeIndex.query( "name", "Mattias*" ), node1 );
        commitTx();
        assertCollection( nodeIndex.get( name, mattias ), node1 );
        assertCollection( nodeIndex.query( name, "\"" + mattias + "\"" ), node1 );
        assertCollection( nodeIndex.query( "name:\"" + mattias + "\"" ), node1 );
        assertEquals( node1, nodeIndex.get( name, mattias ).getSingle() );
        assertCollection( nodeIndex.query( "name", "Mattias*" ), node1 );
        
        beginTx();
        nodeIndex.add( node2, title, hacker );
        nodeIndex.add( node1, title, hacker );
        assertCollection( nodeIndex.get( name, mattias ), node1 );
        assertCollection( nodeIndex.get( title, hacker ), node1, node2 );
        assertCollection( nodeIndex.query( "name:\"" + mattias + "\" OR title:\"" +
                hacker + "\"" ), node1, node2 );
        // TODO This won't work. There's a conceptual problem of adding
        // stuff and then querying with AND
//        assertCollection( nodeIndex.query( "name:\"" + mattias + "\" AND title:\"" +
//                hacker + "\"" ), node1 );
        commitTx();
        assertCollection( nodeIndex.get( name, mattias ), node1 );
        assertCollection( nodeIndex.get( title, hacker ), node1, node2 );
        assertCollection( nodeIndex.query( "name:\"" + mattias + "\" OR title:\"" +
                hacker + "\"" ), node1, node2 );
        assertCollection( nodeIndex.query( "name:\"" + mattias + "\" AND title:\"" +
                hacker + "\"" ), node1 );
        
        beginTx();
        nodeIndex.remove( node2, title, hacker );
        assertCollection( nodeIndex.get( name, mattias ), node1 );
        assertCollection( nodeIndex.get( title, hacker ), node1 );
        assertCollection( nodeIndex.query( "name:\"" + mattias + "\" OR title:\"" +
                hacker + "\"" ), node1 );
        commitTx();
        assertCollection( nodeIndex.get( name, mattias ), node1 );
        assertCollection( nodeIndex.get( title, hacker ), node1 );
        assertCollection( nodeIndex.query( "name:\"" + mattias + "\" OR title:\"" +
                hacker + "\"" ), node1 );
        
        beginTx();
        nodeIndex.remove( node1, title, hacker );
        nodeIndex.remove( node1, name, mattias );
        commitTx();
    }
    
    @Test
    public void testMoreThanOneValue()
    {
        Index<Node> index = provider.nodeIndex( "yosa", null );
        Node node = graphDb.createNode();
        index.add( node, "name", "Mattias" );
        index.add( node, "name", "Persson" );
        restartTx();
        index.add( node, "title", "Hacker" );
        index.add( node, "key", "value0" );
        index.add( node, "key", "value1" );
        index.add( node, "key", "value2" );
        index.add( node, "key", "value3" );
        index.add( node, "key", "value4" );
        index.add( node, "key", "value5" );
        assertEquals( node, index.get( "name", "Mattias" ).getSingle() );
        assertEquals( node, index.get( "name", "Persson" ).getSingle() );
        restartTx();
        assertEquals( node, index.get( "name", "Mattias" ).getSingle() );
        assertEquals( node, index.get( "name", "Persson" ).getSingle() );
        
        assertEquals( node, index.get( "key", "value0" ).getSingle() );
        assertEquals( node, index.get( "key", "value1" ).getSingle() );
        assertEquals( node, index.get( "key", "value2" ).getSingle() );
        assertEquals( node, index.get( "key", "value3" ).getSingle() );
        assertEquals( node, index.get( "key", "value4" ).getSingle() );
    }
    
    @Test
    public void testFulltextNodeIndex()
    {
        Index<Node> index = provider.nodeIndex( "fulltext",
                LuceneIndexProvider.FULLTEXT_CONFIG );
        Node node1 = graphDb.createNode();
        Node node2 = graphDb.createNode();
        
        String key = "name";
        index.add( node1, key, "The quick brown fox" );
        index.add( node2, key, "brown fox jumped over" );
        
        assertCollection( index.get( key, "The quick brown fox" ), node1 );
        assertCollection( index.get( key, "brown fox jumped over" ), node2 );
        assertCollection( index.query( key, "quick" ), node1 );
        assertCollection( index.query( key, "brown" ), node1, node2 );
        assertCollection( index.query( key, "quick OR jumped" ), node1, node2 );
        assertCollection( index.query( key, "brown AND fox" ), node1, node2 );
        restartTx();
        assertCollection( index.get( key, "The quick brown fox" ), node1 );
        assertCollection( index.get( key, "brown fox jumped over" ), node2 );
        assertCollection( index.query( key, "quick" ), node1 );
        assertCollection( index.query( key, "brown" ), node1, node2 );
        assertCollection( index.query( key, "quick OR jumped" ), node1, node2 );
        assertCollection( index.query( key, "brown AND fox" ), node1, node2 );
        
        index.clear();
        node1.delete();
        node2.delete();
    }
    
    @Test
    public void testFulltextBug()
    {
        Index<Node> index = provider.nodeIndex( "fulltextbug",
                LuceneIndexProvider.FULLTEXT_CONFIG );
        Node node1 = graphDb.createNode();
        Node node2 = graphDb.createNode();
        String key = "key";
        index.add( node1, key, "value1" );
        index.add( node1, key, "value2" );
        index.add( node2, key, "value1" );
        index.add( node2, key, "value2" );
        assertCollection( index.get( key, "value1" ), node1, node2 );
        restartTx();
        assertCollection( index.get( key, "value1" ), node1, node2 );
    }
    
    @Test
    public void testFulltextRelationshipIndex()
    {
        Index<Relationship> index = provider.relationshipIndex( "fulltext",
                LuceneIndexProvider.FULLTEXT_CONFIG );
        Node node1 = graphDb.createNode();
        Node node2 = graphDb.createNode();
        RelationshipType type = DynamicRelationshipType.withName( "mytype" );
        Relationship rel1 = node1.createRelationshipTo( node2, type );
        Relationship rel2 = node1.createRelationshipTo( node2, type );
        
        String key = "name";
        index.add( rel1, key, "The quick brown fox" );
        index.add( rel2, key, "brown fox jumped over" );
        restartTx();
        
        assertCollection( index.get( key, "The quick brown fox" ), rel1 );
        assertCollection( index.get( key, "brown fox jumped over" ), rel2 );
        assertCollection( index.query( key, "quick" ), rel1 );
        assertCollection( index.query( key, "brown" ), rel1, rel2 );
        assertCollection( index.query( key, "quick OR jumped" ), rel1, rel2 );
        assertCollection( index.query( key, "brown AND fox" ), rel1, rel2 );
        
        index.clear();
        rel1.delete();
        rel2.delete();
        node1.delete();
        node2.delete();
    }
    
    @Ignore
    @Test
    public void testInsertionSpeed()
    {
        Index<Node> index = provider.nodeIndex( "yeah", LuceneIndexProvider.EXACT_CONFIG );
        long t = System.currentTimeMillis();
        for ( int i = 0; i < 100000; i++ )
        {
            Node node = graphDb.createNode();
            index.add( node, "name", "The name " + i );
            index.add( node, "title", random.nextInt() );
            index.add( node, "something", random.nextInt() );
            index.add( node, "else", random.nextInt() );
            index.add( node, i + " whatever " + i, random.nextInt() );
            if ( i%50000 == 0 )
            {
                restartTx();
                System.out.print( "." );
            }
        }
        System.out.println( "insert:" + (System.currentTimeMillis() - t) );
        
        t = System.currentTimeMillis();
        int count = 100;
        for ( int i = 0; i < count; i++ )
        {
            for ( Node n : index.get( "name", "The name " + i ) )
            {
            }
        }
        System.out.println( "get:" + (double)(System.currentTimeMillis() - t)/(double)count );
    }
}
