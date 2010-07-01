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
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.neo4j.index.Neo4jTestCase;
import org.neo4j.kernel.EmbeddedGraphDatabase;

public class TestLuceneIndex
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
    
    private static abstract interface EntityCreator<T extends PropertyContainer>
    {
        T create();
    }
    
    private static final EntityCreator<Node> NODE_CREATOR = new EntityCreator<Node>()
    {
        public Node create()
        {
            return graphDb.createNode();
        }
    };
    private static final EntityCreator<Relationship> RELATIONSHIP_CREATOR =
            new EntityCreator<Relationship>()
    {
        public Relationship create()
        {
            return graphDb.createNode().createRelationshipTo( graphDb.createNode(),
                    DynamicRelationshipType.withName( "TEST_TYPE" ) );
        }
    };
    
    @Test
    public void testClear()
    {
        Index<Node> index = provider.nodeIndex( "index", LuceneIndexProvider.EXACT_CONFIG );
        Node node = graphDb.createNode();
        index.add( node, "name", "Mattias" );
        index.clear();
        assertCollection( index.get( "name", "Mattias" ) );
        restartTx();
        assertCollection( index.get( "name", "Mattias" ) );
        
        index.add( node, "name", "Mattias" );
        restartTx();
        index.clear();
        assertCollection( index.get( "name", "Mattias" ) );
        restartTx();
        assertCollection( index.get( "name", "Mattias" ) );
    }
    
    private <T extends PropertyContainer> void makeSureAdditionsCanBeRead( Index<T> index,
            EntityCreator<T> entityCreator )
    {
        String key = "name";
        String value = "Mattias";
        assertNull( index.get( key, value ).getSingle() );
        assertCollection( index.get( key, value ) );
        assertCollection( index.query( key, "*" ) );
        
        T entity1 = entityCreator.create();
        T entity2 = entityCreator.create();
        index.add( entity1, key, value );
        assertEquals( entity1, index.get( key, value ).getSingle() );
        assertCollection( index.get( key, value ), entity1 );
        assertCollection( index.query( key, "*" ), entity1 );
        assertCollection( index.query( key + ":*" ), entity1 );
        restartTx();
        assertEquals( entity1, index.get( key, value ).getSingle() );
        assertCollection( index.get( key, value ), entity1 );
        assertCollection( index.query( key, "*" ), entity1 );
        assertCollection( index.query( key + ":*" ), entity1 );
        
        index.add( entity2, key, value );
        assertCollection( index.get( key, value ), entity1, entity2 );
        restartTx();
        assertCollection( index.get( key, value ), entity1, entity2 );
        index.clear();
    }
    
    @Test
    public void makeSureAdditionsCanBeReadNodeExact()
    {
        makeSureAdditionsCanBeRead( provider.nodeIndex( "exact", LuceneIndexProvider.EXACT_CONFIG ),
                NODE_CREATOR );
    }
    
    @Test
    public void makeSureAdditionsCanBeReadNodeFulltext()
    {
        makeSureAdditionsCanBeRead( provider.nodeIndex( "fulltext",
                LuceneIndexProvider.FULLTEXT_CONFIG ), NODE_CREATOR );
    }
    
    @Test
    public void makeSureAdditionsCanBeReadRelationshipExact()
    {
        makeSureAdditionsCanBeRead( provider.relationshipIndex( "exact",
                LuceneIndexProvider.EXACT_CONFIG ), RELATIONSHIP_CREATOR );
    }
    
    @Test
    public void makeSureAdditionsCanBeReadRelationshipFulltext()
    {
        makeSureAdditionsCanBeRead( provider.relationshipIndex( "fulltext",
                LuceneIndexProvider.FULLTEXT_CONFIG ), RELATIONSHIP_CREATOR );
    }
    
    @Test
    public void makeSureAdditionsCanBeRemovedInSameTx()
    {
        makeSureAdditionsCanBeRemoved( false );
    }
    
    private void makeSureAdditionsCanBeRemoved( boolean restartTx )
    {
        Index<Node> index = provider.nodeIndex( "index", LuceneIndexProvider.EXACT_CONFIG );
        String key = "name";
        String value = "Mattias";
        assertNull( index.get( key, value ).getSingle() );
        Node node = graphDb.createNode();
        index.add( node, key, value );
        if ( restartTx )
        {
            restartTx();
        }
        assertEquals( node, index.get( key, value ).getSingle() );
        index.remove( node, key, value );
        assertNull( index.get( key, value ).getSingle() );
        restartTx();
        assertNull( index.get( key, value ).getSingle() );
        node.delete();
        index.clear();
    }
    
    @Test
    public void makeSureAdditionsCanBeRemoved()
    {
        makeSureAdditionsCanBeRemoved( true );
    }
    
    private void makeSureSomeAdditionsCanBeRemoved( boolean restartTx )
    {
        Index<Node> index = provider.nodeIndex( "index", LuceneIndexProvider.EXACT_CONFIG );
        String key1 = "name";
        String key2 = "title";
        String value1 = "Mattias";
        assertNull( index.get( key1, value1 ).getSingle() );
        assertNull( index.get( key2, value1 ).getSingle() );
        Node node = graphDb.createNode();
        Node node2 = graphDb.createNode();
        index.add( node, key1, value1 );
        index.add( node, key2, value1 );
        index.add( node2, key1, value1 );
        if ( restartTx )
        {
            restartTx();
        }
        index.remove( node, key1, value1 );
        index.remove( node, key2, value1 );
        assertEquals( node2, index.get( key1, value1 ).getSingle() );
        assertNull( index.get( key2, value1 ).getSingle() );
        assertEquals( node2, index.get( key1, value1 ).getSingle() );
        assertNull( index.get( key2, value1 ).getSingle() );
        node.delete();
        index.clear();
    }
    
    @Test
    public void makeSureSomeAdditionsCanBeRemovedInSameTx()
    {
        makeSureSomeAdditionsCanBeRemoved( false );
    }
    
    @Test
    public void makeSureSomeAdditionsCanBeRemoved()
    {
        makeSureSomeAdditionsCanBeRemoved( true );
    }
    
    @Test
    public void makeSureThereCanBeMoreThanOneValueForAKeyAndEntity()
    {
        makeSureThereCanBeMoreThanOneValueForAKeyAndEntity( false );
    }
    
    @Test
    public void makeSureThereCanBeMoreThanOneValueForAKeyAndEntitySameTx()
    {
        makeSureThereCanBeMoreThanOneValueForAKeyAndEntity( true );
    }
    
    private void makeSureThereCanBeMoreThanOneValueForAKeyAndEntity( boolean restartTx )
    {
        Index<Node> index = provider.nodeIndex( "index", LuceneIndexProvider.EXACT_CONFIG );
        String key = "name";
        String value1 = "Lucene";
        String value2 = "Index";
        String value3 = "Rules";
        assertCollection( index.query( key, "*" ) );
        Node node = graphDb.createNode();
        index.add( node, key, value1 );
        index.add( node, key, value2 );
        if ( restartTx )
        {
            restartTx();
        }
        index.add( node, key, value3 );
        assertCollection( index.get( key, value1 ), node );
        assertCollection( index.get( key, value2 ), node );
        assertCollection( index.get( key, value3 ), node );
        assertCollection( index.get( key, "whatever" ) );
        restartTx();
        assertCollection( index.get( key, value1 ), node );
        assertCollection( index.get( key, value2 ), node );
        assertCollection( index.get( key, value3 ), node );
        assertCollection( index.get( key, "whatever" ) );
        index.clear();
    }
    
    @Test
    public void makeSureArrayValuesAreSupported()
    {
        Index<Node> index = provider.nodeIndex( "index", LuceneIndexProvider.EXACT_CONFIG );
        String key = "name";
        String value1 = "Lucene";
        String value2 = "Index";
        String value3 = "Rules";
        assertCollection( index.query( key, "*" ) );
        Node node = graphDb.createNode();
        index.add( node, key, new String[] { value1, value2, value3 } );
        assertCollection( index.get( key, value1 ), node );
        assertCollection( index.get( key, value2 ), node );
        assertCollection( index.get( key, value3 ), node );
        assertCollection( index.get( key, "whatever" ) );
        restartTx();
        assertCollection( index.get( key, value1 ), node );
        assertCollection( index.get( key, value2 ), node );
        assertCollection( index.get( key, value3 ), node );
        assertCollection( index.get( key, "whatever" ) );
        
        index.remove( node, key, new String[] { value2, value3 } );
        assertCollection( index.get( key, value1 ), node );
        assertCollection( index.get( key, value2 ) );
        assertCollection( index.get( key, value3 ) );
        restartTx();
        assertCollection( index.get( key, value1 ), node );
        assertCollection( index.get( key, value2 ) );
        assertCollection( index.get( key, value3 ) );
        index.clear();
    }
    
    @Test
    public void makeSureWildcardQueriesCanBeAsked()
    {
        Index<Node> index = provider.nodeIndex( "index", LuceneIndexProvider.EXACT_CONFIG );
        String key = "name";
        String value1 = "neo4j";
        String value2 = "nescafe";
        Node node1 = graphDb.createNode();
        Node node2 = graphDb.createNode();
        index.add( node1, key, value1 );
        index.add( node2, key, value2 );
        
        assertCollection( index.query( key, "neo4j" ), node1 );
        assertCollection( index.query( key, "neo*" ), node1 );
        assertCollection( index.query( key, "n?o4j" ), node1 );
        assertCollection( index.query( key, "ne*" ), node1, node2 );
        assertCollection( index.query( key + ":neo4j" ), node1 );
        assertCollection( index.query( key + ":neo*" ), node1 );
        assertCollection( index.query( key + ":n?o4j" ), node1 );
        assertCollection( index.query( key + ":ne*" ), node1, node2 );
        restartTx();
        assertCollection( index.query( key, "neo4j" ), node1 );
        assertCollection( index.query( key, "neo*" ), node1 );
        assertCollection( index.query( key, "n?o4j" ), node1 );
        assertCollection( index.query( key, "ne*" ), node1, node2 );
        assertCollection( index.query( key + ":neo4j" ), node1 );
        assertCollection( index.query( key + ":neo*" ), node1 );
        assertCollection( index.query( key + ":n?o4j" ), node1 );
        assertCollection( index.query( key + ":ne*" ), node1, node2 );
        index.clear();
    }
    
    @Test
    public void makeSureCompositeQueriesCanBeAsked()
    {
        Index<Node> index = provider.nodeIndex( "index", LuceneIndexProvider.EXACT_CONFIG );
        Node neo = graphDb.createNode();
        Node trinity = graphDb.createNode();
        index.add( neo, "username", "neo@matrix" );
        index.add( neo, "sex", "male" );
        index.add( trinity, "username", "trinity@matrix" );
        index.add( trinity, "sex", "female" );
        
        assertCollection( index.query( "username:*@matrix AND sex:male" ), neo );
        assertCollection( index.query( "username:*@matrix OR sex:male" ), neo, trinity );
        restartTx();
        assertCollection( index.query( "username:*@matrix AND sex:male" ), neo );
        assertCollection( index.query( "username:*@matrix OR sex:male" ), neo, trinity );
        index.clear();
    }
    
    private <T extends PropertyContainer> void doSomeRandomUseCaseTestingWithExactIndex(
            Index<T> index, EntityCreator<T> creator )
    {
        String name = "name";
        String mattias = "Mattias Persson";
        String title = "title";
        String hacker = "Hacker";
        
        assertCollection( index.get( name, mattias ) );
        
        T entity1 = creator.create();
        T entity2 = creator.create();
        
        assertNull( index.get( name, mattias ).getSingle() );
        index.add( entity1, name, mattias );
        assertCollection( index.get( name, mattias ), entity1 );
        assertCollection( index.query( name, "\"" + mattias + "\"" ), entity1 );
        assertCollection( index.query( "name:\"" + mattias + "\"" ), entity1 );
        assertEquals( entity1, index.get( name, mattias ).getSingle() );
        assertCollection( index.query( "name", "Mattias*" ), entity1 );
        commitTx();
        assertCollection( index.get( name, mattias ), entity1 );
        assertCollection( index.query( name, "\"" + mattias + "\"" ), entity1 );
        assertCollection( index.query( "name:\"" + mattias + "\"" ), entity1 );
        assertEquals( entity1, index.get( name, mattias ).getSingle() );
        assertCollection( index.query( "name", "Mattias*" ), entity1 );
        
        beginTx();
        index.add( entity2, title, hacker );
        index.add( entity1, title, hacker );
        assertCollection( index.get( name, mattias ), entity1 );
        assertCollection( index.get( title, hacker ), entity1, entity2 );
        assertCollection( index.query( "name:\"" + mattias + "\" OR title:\"" +
                hacker + "\"" ), entity1, entity2 );
        commitTx();
        assertCollection( index.get( name, mattias ), entity1 );
        assertCollection( index.get( title, hacker ), entity1, entity2 );
        assertCollection( index.query( "name:\"" + mattias + "\" OR title:\"" +
                hacker + "\"" ), entity1, entity2 );
        assertCollection( index.query( "name:\"" + mattias + "\" AND title:\"" +
                hacker + "\"" ), entity1 );
        
        beginTx();
        index.remove( entity2, title, hacker );
        assertCollection( index.get( name, mattias ), entity1 );
        assertCollection( index.get( title, hacker ), entity1 );
        assertCollection( index.query( "name:\"" + mattias + "\" OR title:\"" +
                hacker + "\"" ), entity1 );
        commitTx();
        assertCollection( index.get( name, mattias ), entity1 );
        assertCollection( index.get( title, hacker ), entity1 );
        assertCollection( index.query( "name:\"" + mattias + "\" OR title:\"" +
                hacker + "\"" ), entity1 );
        
        beginTx();
        index.remove( entity1, title, hacker );
        index.remove( entity1, name, mattias );
        index.clear();
        commitTx();
    }
    
    @Test
    public void doSomeRandomUseCaseTestingWithExactNodeIndex()
    {
        doSomeRandomUseCaseTestingWithExactIndex( provider.nodeIndex( "index",
                LuceneIndexProvider.EXACT_CONFIG ), NODE_CREATOR );
    }
    
    @Test
    public void doSomeRandomUseCaseTestingWithExactRelationshipIndex()
    {
        doSomeRandomUseCaseTestingWithExactIndex( provider.relationshipIndex( "index",
                LuceneIndexProvider.EXACT_CONFIG ), RELATIONSHIP_CREATOR );
    }
    
    private <T extends PropertyContainer> void doSomeRandomTestingWithFulltextIndex( Index<T> index,
            EntityCreator<T> creator )
    {
        T entity1 = creator.create();
        T entity2 = creator.create();
        
        String key = "name";
        index.add( entity1, key, "The quick brown fox" );
        index.add( entity2, key, "brown fox jumped over" );
        
        assertCollection( index.get( key, "The quick brown fox" ), entity1 );
        assertCollection( index.get( key, "brown fox jumped over" ), entity2 );
        assertCollection( index.query( key, "quick" ), entity1 );
        assertCollection( index.query( key, "brown" ), entity1, entity2 );
        assertCollection( index.query( key, "quick OR jumped" ), entity1, entity2 );
        assertCollection( index.query( key, "brown AND fox" ), entity1, entity2 );
        restartTx();
        assertCollection( index.get( key, "The quick brown fox" ), entity1 );
        assertCollection( index.get( key, "brown fox jumped over" ), entity2 );
        assertCollection( index.query( key, "quick" ), entity1 );
        assertCollection( index.query( key, "brown" ), entity1, entity2 );
        assertCollection( index.query( key, "quick OR jumped" ), entity1, entity2 );
        assertCollection( index.query( key, "brown AND fox" ), entity1, entity2 );
        
        index.clear();
    }
    
    @Test
    public void doSomeRandomTestingWithNodeFulltextInde()
    {
        doSomeRandomTestingWithFulltextIndex( provider.nodeIndex( "fulltext",
                LuceneIndexProvider.FULLTEXT_CONFIG ), NODE_CREATOR );
    }
    
    @Test
    public void doSomeRandomTestingWithRelationshipFulltextInde()
    {
        doSomeRandomTestingWithFulltextIndex( provider.relationshipIndex( "fulltext",
                LuceneIndexProvider.FULLTEXT_CONFIG ), RELATIONSHIP_CREATOR );
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
