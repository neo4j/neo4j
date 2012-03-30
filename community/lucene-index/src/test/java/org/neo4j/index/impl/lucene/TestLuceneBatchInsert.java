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

import static org.apache.lucene.search.NumericRangeQuery.newIntRange;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.neo4j.helpers.Exceptions.launderedException;
import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.index.Neo4jTestCase.assertContains;
import static org.neo4j.index.impl.lucene.Contains.contains;
import static org.neo4j.index.impl.lucene.IsEmpty.isEmpty;
import static org.neo4j.index.impl.lucene.LuceneIndexImplementation.EXACT_CONFIG;
import static org.neo4j.index.lucene.ValueContext.numeric;

import java.io.File;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.index.BatchInserterIndex;
import org.neo4j.graphdb.index.BatchInserterIndexProvider;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.index.Neo4jTestCase;
import org.neo4j.index.lucene.ValueContext;
import org.neo4j.kernel.impl.batchinsert.BatchInserter;
import org.neo4j.kernel.impl.batchinsert.BatchInserterImpl;
import org.neo4j.kernel.impl.cache.LruCache;

public class TestLuceneBatchInsert
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
        // Different paths for each tests because there's a bug (on Windows)
        // causing _0.cfs file to stay open 
        String path = new File( PATH, "1" ).getAbsolutePath();
        BatchInserter inserter = new BatchInserterImpl( path );
        BatchInserterIndexProvider provider = new LuceneBatchInserterIndexProvider(
                inserter );
        String indexName = "users";
        BatchInserterIndex index = provider.nodeIndex( indexName, EXACT_CONFIG );
        Map<Integer, Long> ids = new HashMap<Integer, Long>();
        int count = 5;
        for ( int i = 0; i < count; i++ )
        {
            long id = inserter.createNode( null );
            index.add( id, map( "name", "Joe" + i, "other", "Schmoe" ) );
            ids.put( i, id );
        }

        for ( int i = 0; i < count; i++ )
        {
            assertContains( index.get( "name", "Joe" + i ), ids.get( i ) );
        }
        assertContains( index.query( "name:Joe0 AND other:Schmoe" ),
                ids.get( 0 ) );

        assertContains( index.query( "name", "Joe*" ),
                ids.values().toArray( new Long[ids.size()] ) );
        provider.shutdown();
        inserter.shutdown();

        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase( path );
        IndexManager indexManager = db.index();
        assertFalse( indexManager.existsForRelationships( indexName ) );
        assertTrue( indexManager.existsForNodes( indexName ) );
        assertNotNull( indexManager.forNodes( indexName ) );
        Index<Node> dbIndex = db.index().forNodes( "users" );
        for ( int i = 0; i < count; i++ )
        {
            assertContains( dbIndex.get( "name", "Joe" + i ),
                    db.getNodeById( ids.get( i ) ) );
        }

        Collection<Node> nodes = new ArrayList<Node>();
        for ( long id : ids.values() )
        {
            nodes.add( db.getNodeById( id ) );
        }
        assertContains( dbIndex.query( "name", "Joe*" ),
                nodes.toArray( new Node[nodes.size()] ) );
        assertContains( dbIndex.query( "name:Joe0 AND other:Schmoe" ),
                db.getNodeById( ids.get( 0 ) ) );
        db.shutdown();
    }

    @Test
    public void testFulltext()
    {
        String path = new File( PATH, "2" ).getAbsolutePath();
        BatchInserter inserter = new BatchInserterImpl( path );
        BatchInserterIndexProvider provider = new LuceneBatchInserterIndexProvider(
                inserter );
        String name = "users";
        BatchInserterIndex index = provider.nodeIndex( name,
                stringMap( "type", "fulltext" ) );

        long id1 = inserter.createNode( null );
        index.add( id1, map( "name", "Mattias Persson", "email",
                "something@somewhere", "something", "bad" ) );
        long id2 = inserter.createNode( null );
        index.add( id2, map( "name", "Lars PerssoN" ) );
        index.flush();
        assertContains( index.get( "name", "Mattias Persson" ), id1 );
        assertContains( index.query( "name", "mattias" ), id1 );
        assertContains( index.query( "name", "bla" ) );
        assertContains( index.query( "name", "persson" ), id1, id2 );
        assertContains( index.query( "email", "*@*" ), id1 );
        assertContains( index.get( "something", "bad" ), id1 );
        long id3 = inserter.createNode( null );
        index.add( id3, map( "name", new String[] { "What Ever", "Anything" } ) );
        index.flush();
        assertContains( index.get( "name", "What Ever" ), id3 );
        assertContains( index.get( "name", "Anything" ), id3 );

        provider.shutdown();
        inserter.shutdown();

        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase( path );
        Index<Node> dbIndex = db.index().forNodes( name );
        Node node1 = db.getNodeById( id1 );
        Node node2 = db.getNodeById( id2 );
        assertContains( dbIndex.query( "name", "persson" ), node1, node2 );
        db.shutdown();
    }

    @Test
    public void testCanIndexRelationships()
    {
        BatchInserter inserter = new BatchInserterImpl( new File( PATH, "5" ).getAbsolutePath() );
        BatchInserterIndexProvider indexProvider = new LuceneBatchInserterIndexProvider(
                inserter );
        BatchInserterIndex edgesIndex = indexProvider.relationshipIndex(
                "edgeIndex", stringMap( IndexManager.PROVIDER, "lucene", "type", "exact" ) );

        long nodeId1 = inserter.createNode( map( "ID", "1" ) );
        long nodeId2 = inserter.createNode( map( "ID", "2" ) );
        long relationshipId = inserter.createRelationship( nodeId1, nodeId2,
                EdgeType.KNOWS, null );

        edgesIndex.add( relationshipId, map( "EDGE_TYPE", EdgeType.KNOWS.name() ) );
        edgesIndex.flush();

        assertEquals(
                String.format( "Should return relationship id" ),
                new Long( relationshipId ),
                edgesIndex.query( "EDGE_TYPE", EdgeType.KNOWS.name() ).getSingle() );

        indexProvider.shutdown();
        inserter.shutdown();
    }
    
    @Test
    public void triggerNPEAfterFlush()
    {
        BatchInserter inserter = new BatchInserterImpl( new File( PATH, "6" ).getAbsolutePath() );
        BatchInserterIndexProvider provider = new LuceneBatchInserterIndexProvider( inserter );
        BatchInserterIndex index = provider.nodeIndex( "Node-exact", EXACT_CONFIG );
        
        Map<String, Object> map = map( "name", "Something" );
        long node = inserter.createNode( map );
        index.add( node, map );
        index.flush();
        assertContains( index.get( "name", "Something" ), node );
        
        provider.shutdown();
        inserter.shutdown();
    }
    
    @Test
    public void testNumericValues()
    {
        String path = new File( PATH, "7" ).getAbsolutePath();
        BatchInserter inserter = new BatchInserterImpl( path );
        BatchInserterIndexProvider provider = new LuceneBatchInserterIndexProvider(
                inserter );
        BatchInserterIndex index = provider.nodeIndex( "mine", EXACT_CONFIG );
        
        long node1 = inserter.createNode( null );
        index.add( node1, map( "number", numeric( 45 ) ) );
        long node2 = inserter.createNode( null );
        index.add( node2, map( "number", numeric( 21 ) ) );
        assertContains( index.query( "number",
                newIntRange( "number", 21, 50, true, true ) ), node1, node2 );
        
        provider.shutdown();
        inserter.shutdown();
        
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase( path );
        Node n1 = db.getNodeById( node1 );
        Node n2 = db.getNodeById( node2 );
        Index<Node> idx = db.index().forNodes( "mine" );
        assertContains( idx.query( "number", newIntRange( "number", 21, 45, false, true ) ), n1 );
        db.shutdown();
    }

    @Test
    public void testNumericValueArrays()
    {
        String path = new File( PATH, "8" ).getAbsolutePath();
        BatchInserter inserter = new BatchInserterImpl( path );
        BatchInserterIndexProvider provider = new LuceneBatchInserterIndexProvider(
                inserter );
        BatchInserterIndex batchIndex = provider.nodeIndex( "mine", EXACT_CONFIG );

        long nodeId1 = inserter.createNode( null );
        batchIndex.add( nodeId1, map( "number", new ValueContext[]{ numeric( 45 ), numeric( 98 ) } ) );
        long nodeId2 = inserter.createNode( null );
        batchIndex.add( nodeId2, map( "number", new ValueContext[]{ numeric( 47 ), numeric( 100 ) } ) );

        IndexHits<Long> batchIndexResult1 = batchIndex.query( "number", newIntRange( "number", 47, 98, true, true ) );
        assertThat( batchIndexResult1, contains(nodeId1, nodeId2));
        assertThat( batchIndexResult1.size(), is( 2 ));

        IndexHits<Long> batchIndexResult2 = batchIndex.query( "number", newIntRange( "number", 44, 46, true, true ) );
        assertThat( batchIndexResult2, contains(nodeId1));
        assertThat( batchIndexResult2.size(), is( 1 ) );

        IndexHits<Long> batchIndexResult3 = batchIndex.query( "number", newIntRange( "number", 99, 101, true, true ) );
        assertThat( batchIndexResult3, contains( nodeId2 ) );
        assertThat( batchIndexResult3.size(), is( 1 ) );

        IndexHits<Long> batchIndexResult4 = batchIndex.query( "number", newIntRange( "number", 47, 98, false, false ) );
        assertThat( batchIndexResult4, isEmpty() );

        provider.shutdown();
        inserter.shutdown();

        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase( path );
        Node node1 = db.getNodeById( nodeId1 );
        Node node2 = db.getNodeById( nodeId2 );
        Index<Node> index = db.index().forNodes( "mine" );


        IndexHits<Node> indexResult1 = index.query( "number", newIntRange( "number", 47, 98, true, true ) );
        assertThat(indexResult1, contains(node1, node2));
        assertThat( indexResult1.size(), is( 2 ));

        IndexHits<Node> indexResult2 = index.query( "number", newIntRange( "number", 44, 46, true, true ) );
        assertThat(indexResult2, contains(node1));
        assertThat( indexResult2.size(), is( 1 ) );

        IndexHits<Node> indexResult3 = index.query( "number", newIntRange( "number", 99, 101, true, true ) );
        assertThat( indexResult3, contains( node2 ) );
        assertThat( indexResult3.size(), is( 1 ) );

        IndexHits<Node> indexResult4 = index.query( "number", newIntRange( "number", 47, 98, false, false ) );
        assertThat( indexResult4, isEmpty() );

        
        db.shutdown();
    }
    
    @Test
    public void indexNumbers() throws Exception
    {
        BatchInserter inserter = new BatchInserterImpl( new File( PATH, "9" ).getAbsolutePath() );
        BatchInserterIndexProvider provider = new LuceneBatchInserterIndexProvider(
                inserter );
        BatchInserterIndex index = provider.nodeIndex( "mine", EXACT_CONFIG );
        
        long id = inserter.createNode( null );
        Map<String, Object> props = new HashMap<String, Object>();
        props.put( "key", 123L );
        index.add( id, props );
        index.flush();
        
        assertEquals( 1, index.get( "key", 123L ).size() );
        assertEquals( 1, index.get( "key", "123" ).size() );
        
        provider.shutdown();
        inserter.shutdown();
    }

    @Test
    public void addOrUpdateFlushBehaviour() throws Exception
    {
        BatchInserter inserter = new BatchInserterImpl( new File( PATH, "9" ).getAbsolutePath() );
        BatchInserterIndexProvider provider = new LuceneBatchInserterIndexProvider(
                inserter );
        BatchInserterIndex index = provider.nodeIndex( "update", EXACT_CONFIG );
        
        long id = inserter.createNode( null );
        Map<String, Object> props = new HashMap<String, Object>();
        props.put( "key", "value" );
        index.add( id, props );
        index.updateOrAdd( id, props );
        assertEquals( 1, index.get( "key", "value" ).size() );
        index.flush();
        props.put( "key", "value2" );
        index.updateOrAdd( id, props );
        index.flush();
        assertEquals( 1, index.get( "key", "value2" ).size() );
        assertEquals( 0, index.get( "key", "value" ).size() );
        props.put( "key2", "value2" );
        props.put( "key", "value" );
        index.updateOrAdd( id, props );
        assertEquals( 0, index.get( "key2", "value2" ).size() );
        index.flush();
        assertEquals( 1, index.get( "key2", "value2" ).size() );
        assertEquals( 1, index.get( "key", "value" ).size() );
        
        long id2 = inserter.createNode( null );
        props = new HashMap<String, Object>();
        props.put("2key","value");
        index.updateOrAdd( id2, props );
        props.put("2key","value2");
        props.put("2key2","value3");
        index.updateOrAdd( id2, props );
        index.flush();
        assertEquals( 1, index.get( "2key", "value2" ).size() );
        provider.shutdown();
        inserter.shutdown();
    }

    @Test
    public void useStandardAnalyzer() throws Exception
    {
        BatchInserter inserter = new BatchInserterImpl( PATH );
        BatchInserterIndexProvider provider = new LuceneBatchInserterIndexProvider( inserter );
        BatchInserterIndex index = provider.nodeIndex( "myindex", stringMap( "analyzer", MyStandardAnalyzer.class.getName() ) );
        index.add( 0, map( "name", "Mattias" ) );
        provider.shutdown();
        inserter.shutdown();
    }
    
    @Test
    public void cachesShouldBeFilledWhenAddToMultipleIndexesCreatedNow() throws Exception
    {
        BatchInserter inserter = new BatchInserterImpl( PATH );
        BatchInserterIndexProvider provider = new LuceneBatchInserterIndexProvider( inserter );
        BatchInserterIndex index = provider.nodeIndex( "index1", LuceneIndexImplementation.EXACT_CONFIG );
        index.setCacheCapacity( "name", 100000 );
        String nameKey = "name";
        String titleKey = "title";
        
        assertCacheIsEmpty( index, nameKey, titleKey );
        index.add( 0, map( "name", "Neo", "title", "Matrix" ) );
        assertCacheContainsSomething( index, nameKey );
        assertCacheIsEmpty( index, titleKey );
        
        BatchInserterIndex index2 = provider.nodeIndex( "index2", LuceneIndexImplementation.EXACT_CONFIG );
        index2.setCacheCapacity( "title", 100000 );
        assertCacheIsEmpty( index2, nameKey, titleKey );
        index2.add( 0, map( "name", "Neo", "title", "Matrix" ) );
        assertCacheContainsSomething( index2, titleKey );
        assertCacheIsEmpty( index2, nameKey );
        
        provider.shutdown();
        inserter.shutdown();
    }
    
    @Test
    public void cachesDoesntGetFilledWhenAddingForAnExistingIndex() throws Exception
    {
        // Prepare the test case, i.e. create a store with a populated index.
        BatchInserter inserter = new BatchInserterImpl( PATH );
        BatchInserterIndexProvider provider = new LuceneBatchInserterIndexProvider( inserter );
        String indexName = "index";
        BatchInserterIndex index = provider.nodeIndex( indexName, LuceneIndexImplementation.EXACT_CONFIG );
        String key = "name";
        index.add( 0, map( key, "Mattias" ) );
        provider.shutdown();
        inserter.shutdown();
        
        // Test so that the next run doesn't start caching inserted stuff right away,
        // because that would lead to invalid results being returned.
        inserter = new BatchInserterImpl( PATH );
        provider = new LuceneBatchInserterIndexProvider( inserter );
        index = provider.nodeIndex( indexName, LuceneIndexImplementation.EXACT_CONFIG );
        index.setCacheCapacity( key, 100000 );
        assertCacheIsEmpty( index, key );
        index.add( 1, map( key, "Persson" ) );
        assertCacheIsEmpty( index, key );
        assertEquals( 1, index.get( key, "Persson" ).getSingle().intValue() );
        provider.shutdown();
        inserter.shutdown();
    }
    
    private void assertCacheContainsSomething( BatchInserterIndex index, String... keys )
    {
        Map<String, LruCache<String, Collection<Long>>> cache = getIndexCache( index );
        for ( String key : keys )
            assertTrue( cache.get( key ).size() > 0 );
    }

    private void assertCacheIsEmpty( BatchInserterIndex index, String... keys )
    {
        Map<String, LruCache<String, Collection<Long>>> cache = getIndexCache( index );
        for ( String key : keys )
        {
            LruCache<String, Collection<Long>> keyCache = cache.get( key );
            assertTrue( keyCache == null || keyCache.size() == 0 );
        }
    }

    private Map<String, LruCache<String, Collection<Long>>> getIndexCache( BatchInserterIndex index )
    {
        try
        {
            Field field = index.getClass().getDeclaredField( "cache" );
            field.setAccessible( true );
            return (Map<String, LruCache<String, Collection<Long>>>) field.get( index );
        }
        catch ( Exception e )
        {
            throw launderedException( e );
        }
    }

    private enum EdgeType implements RelationshipType
    {
        KNOWS
    }
}
