package org.neo4j.index.impl.lucene;

import static org.neo4j.index.Neo4jTestCase.assertCollection;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.neo4j.commons.collection.MapUtil;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.index.BatchInserterIndex;
import org.neo4j.graphdb.index.BatchInserterIndexProvider;
import org.neo4j.graphdb.index.Index;
import org.neo4j.index.Neo4jTestCase;
import org.neo4j.index.impl.lucene.LuceneBatchInserterIndexProvider;
import org.neo4j.index.impl.lucene.LuceneIndexProvider;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.impl.batchinsert.BatchInserter;
import org.neo4j.kernel.impl.batchinsert.BatchInserterImpl;

public class TestNewLuceneBatchInsert
{
    private static final String PATH = "target/var/batch";
    
    @Before
    public void cleanDirectory()
    {
        Neo4jTestCase.deleteFileOrDirectory( new File( PATH ) );
    }
    
    @Test
    public void testSome() throws Exception
    {
        BatchInserter inserter = new BatchInserterImpl( PATH );
        BatchInserterIndexProvider provider = new LuceneBatchInserterIndexProvider( inserter );
        BatchInserterIndex index = provider.nodeIndex( "users", null );
        Map<Integer, Long> ids = new HashMap<Integer, Long>();
        for ( int i = 0; i < 100; i++ )
        {
            long id = inserter.createNode( null );
            index.add( id, MapUtil.map( "name", "Joe" + i, "other", "Schmoe" ) );
            ids.put( i, id );
        }
        
        for ( int i = 0; i < 100; i++ )
        {
            assertCollection( index.get( "name", "Joe" + i ), ids.get( i ) );
        }
        assertCollection( index.query( "name:Joe0 AND other:Schmoe" ), ids.get( 0 ) );
        
        assertCollection( index.query( "name", "Joe*" ),
                ids.values().toArray( new Long[ids.size()] ) );
        provider.shutdown();
        inserter.shutdown();
        
        GraphDatabaseService db = new EmbeddedGraphDatabase( PATH );
        LuceneIndexProvider indexProvider = new LuceneIndexProvider( db );
        Index<Node> dbIndex = indexProvider.nodeIndex( "users", null );
        for ( int i = 0; i < 100; i++ )
        {
            assertCollection( dbIndex.get( "name", "Joe" + i ), db.getNodeById(
                    ids.get( i ) ) );
        }
        
        Collection<Node> nodes = new ArrayList<Node>();
        for ( long id : ids.values() )
        {
            nodes.add( db.getNodeById( id ) );
        }
        assertCollection( dbIndex.query( "name", "Joe*" ),
                nodes.toArray( new Node[nodes.size()] ) );
        assertCollection( dbIndex.query( "name:Joe0 AND other:Schmoe" ), db.getNodeById(
                ids.get( 0 ) ) );
        db.shutdown();
    }
    
    @Test
    public void testFulltext()
    {
        BatchInserter inserter = new BatchInserterImpl( PATH );
        BatchInserterIndexProvider provider = new LuceneBatchInserterIndexProvider( inserter );
        String name = "users";
        BatchInserterIndex index = provider.nodeIndex( name,
                LuceneIndexProvider.FULLTEXT_CONFIG );

        long id1 = inserter.createNode( null );
        index.add( id1, MapUtil.map( "name", "Mattias Persson", "email", "something@somewhere",
                "something", "bad" ) );
        long id2 = inserter.createNode( null );
        index.add( id2, MapUtil.map( "name", "Lars PerssoN" ) );
        index.flush();
        assertCollection( index.get( "name", "Mattias Persson" ), id1 );
        assertCollection( index.query( "name", "mattias" ), id1 );
        assertCollection( index.query( "name", "bla" ) );
        assertCollection( index.query( "name", "persson" ), id1, id2 );
        assertCollection( index.query( "email", "*@*" ), id1 );
        assertCollection( index.get( "something", "bad" ), id1 );
        long id3 = inserter.createNode( null );
        index.add( id3, MapUtil.map( "name", new String[] { "What Ever", "Anything" } ) );
        index.flush();
        assertCollection( index.get( "name", "What Ever" ), id3 );
        assertCollection( index.get( "name", "Anything" ), id3 );
        
        provider.shutdown();
        inserter.shutdown();
        
        GraphDatabaseService db = new EmbeddedGraphDatabase( PATH );
        LuceneIndexProvider indexProvider = new LuceneIndexProvider( db );
        Index<Node> dbIndex = indexProvider.nodeIndex( name, LuceneIndexProvider.FULLTEXT_CONFIG );
        Node node1 = db.getNodeById( id1 );
        Node node2 = db.getNodeById( id2 );
        assertCollection( dbIndex.query( "name", "persson" ), node1, node2 );
        db.shutdown();
    }

    @Test
    public void testInsertionSpeed()
    {
        BatchInserter inserter = new BatchInserterImpl( PATH );
        BatchInserterIndexProvider provider = new LuceneBatchInserterIndexProvider( inserter );
        BatchInserterIndex index = provider.nodeIndex( "yeah", null );
        long t = System.currentTimeMillis();
        for ( int i = 0; i < 100000; i++ )
        {
            long id = inserter.createNode( null );
            index.add( id, MapUtil.map( "key", "value" + i ) );
        }
        System.out.println( "insert:" + (System.currentTimeMillis() - t) );
        index.flush();
        
        t = System.currentTimeMillis();
        for ( int i = 0; i < 10000; i++ )
        {
            for ( long n : index.get( "key", "value" + i ) )
            {
            }
        }
        System.out.println( "get:" + (System.currentTimeMillis() - t) );
    }
}
