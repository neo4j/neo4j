/**
 * Copyright (c) 2002-2011 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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

import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.index.Neo4jTestCase.assertContains;
import static org.neo4j.index.Neo4jTestCase.assertContainsInOrder;
import static org.neo4j.index.impl.lucene.Contains.contains;
import static org.neo4j.index.impl.lucene.IsEmpty.isEmpty;
import static org.neo4j.index.impl.lucene.ValueContext.numeric;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.CountDownLatch;

import org.apache.lucene.index.Term;
import org.apache.lucene.queryParser.QueryParser.Operator;
import org.apache.lucene.search.DefaultSimilarity;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TermQuery;
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
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.graphdb.index.RelationshipIndex;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.index.Neo4jTestCase;
import org.neo4j.kernel.EmbeddedGraphDatabase;

public class TestLuceneIndex
{
    private static GraphDatabaseService graphDb;
    private Transaction tx;

    @BeforeClass
    public static void setUpStuff()
    {
        String storeDir = "target/var/freshindex";
        Neo4jTestCase.deleteFileOrDirectory( new File( storeDir ) );
        graphDb = new EmbeddedGraphDatabase( storeDir );
    }

    @AfterClass
    public static void tearDownStuff()
    {
        graphDb.shutdown();
    }
    
    @After
    public void commitTx()
    {
        finishTx( true );
    }
    
    private void rollbackTx()
    {
        finishTx( false );
    }

    public void finishTx( boolean success )
    {
        if ( tx != null )
        {
            if ( success )
            {
                tx.success();
            }
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
        T create( Object... properties );
        
        void delete( T entity );
    }

    private static final RelationshipType TEST_TYPE =
            DynamicRelationshipType.withName( "TEST_TYPE" );
    private static final EntityCreator<Node> NODE_CREATOR = new EntityCreator<Node>()
    {
        public Node create( Object... properties )
        {
            Node node = graphDb.createNode();
            setProperties( node, properties );
            return node;
        }
        
        public void delete( Node entity )
        {
            entity.delete();
        }
    };
    private static final EntityCreator<Relationship> RELATIONSHIP_CREATOR =
            new EntityCreator<Relationship>()
            {
                public Relationship create( Object... properties )
                {
                    Relationship rel = graphDb.createNode().createRelationshipTo( graphDb.createNode(), TEST_TYPE );
                    setProperties( rel, properties );
                    return rel;
                }
                
                public void delete( Relationship entity )
                {
                    entity.delete();
                }
            };

    static class FastRelationshipCreator implements EntityCreator<Relationship>
    {
        private Node node, otherNode;

        public Relationship create( Object... properties )
        {
            if ( node == null )
            {
                node = graphDb.createNode();
                otherNode = graphDb.createNode();
            }
            Relationship rel = node.createRelationshipTo( otherNode, TEST_TYPE );
            setProperties( rel, properties );
            return rel;
        }
        
        public void delete( Relationship entity )
        {
            entity.delete();
        }
    }
    
    private static void setProperties( PropertyContainer entity, Object... properties )
    {
        for ( Map.Entry<String, Object> entry : MapUtil.map( properties ).entrySet() )
        {
            entity.setProperty( entry.getKey(), entry.getValue() );
        }
    }
    
    private Index<Node> nodeIndex( String name, Map<String, String> config )
    {
        return graphDb.index().forNodes( name, config );
    }
    
    private RelationshipIndex relationshipIndex( String name, Map<String, String> config )
    {
        return graphDb.index().forRelationships( name, config );
    }
    
    private <T extends PropertyContainer> void makeSureAdditionsCanBeRead(
            Index<T> index, EntityCreator<T> entityCreator )
    {
        String key = "name";
        String value = "Mattias";
        assertThat( index.get( key, value ).getSingle(), is( nullValue() ) );
        assertThat( index.get( key, value ), isEmpty() );

        assertThat( index.query( key, "*" ), isEmpty() );

        T entity1 = entityCreator.create();
        T entity2 = entityCreator.create();
        index.add( entity1, key, value );
        assertThat( index.get( key, value ), contains( entity1 ) );
        assertThat( index.query( key, "*" ), contains( entity1 ) );
        assertThat( index.get( key, value ), contains( entity1 ) );

        restartTx();
        assertThat( index.get( key, value ), contains( entity1 ) );
        assertThat( index.query( key, "*" ), contains( entity1 ) );
        assertThat( index.get( key, value ), contains( entity1 ) );

        index.add( entity2, key, value );
        assertThat( index.get( key, value ), contains( entity1, entity2 ) );

        restartTx();
        assertThat( index.get( key, value ), contains( entity1, entity2 ) );
        index.delete();
    }

//    private <T extends PropertyContainer> void assertQueryNotPossible(
//            Index<T> index )
//    {
//        try
//        {
//            index.query( "somekey:somevalue" );
//            fail( "Querying shouldn't be possible" );
//        }
//        catch ( QueryNotPossibleException e )
//        {
//            // Good
//        }
//    }

    @Test
    public void makeSureYouGetLatestTxModificationsInQueryByDefault()
    {
        Index<Node> index = nodeIndex( "failing-index", LuceneIndexProvider.FULLTEXT_CONFIG );
        Node node = graphDb.createNode();
        index.add( node, "key", "value" );
        assertThat( index.query( "key:value" ), contains( node ) );
    }
    
    
    @Test
    public void testStartupInExistingDirectory() {
        File dir = new File( "target/temp/" );
        Neo4jTestCase.deleteFileOrDirectory( dir );
        dir.mkdir();
        EmbeddedGraphDatabase graphDatabase = new EmbeddedGraphDatabase( dir.getAbsolutePath() );
        Index<Node> index = graphDatabase.index().forNodes("nodes");
        assertNotNull(index);
    }

    @Test
    public void makeSureAdditionsCanBeReadNodeExact()
    {
        makeSureAdditionsCanBeRead( nodeIndex( "exact", LuceneIndexProvider.EXACT_CONFIG ),
                NODE_CREATOR );
    }

    @Test
    public void makeSureAdditionsCanBeReadNodeFulltext()
    {
        makeSureAdditionsCanBeRead( nodeIndex( "fulltext",
                LuceneIndexProvider.FULLTEXT_CONFIG ), NODE_CREATOR );
    }

    @Test
    public void makeSureAdditionsCanBeReadRelationshipExact()
    {
        makeSureAdditionsCanBeRead( relationshipIndex( "exact",
                LuceneIndexProvider.EXACT_CONFIG ), RELATIONSHIP_CREATOR );
    }

    @Test
    public void makeSureAdditionsCanBeReadRelationshipFulltext()
    {
        makeSureAdditionsCanBeRead( relationshipIndex( "fulltext",
                LuceneIndexProvider.FULLTEXT_CONFIG ), RELATIONSHIP_CREATOR );
    }

    @Test
    public void makeSureAdditionsCanBeRemovedInSameTx()
    {
        makeSureAdditionsCanBeRemoved( false );
    }
    
    @Test
    public void makeSureYouCanAskIfAnIndexExistsOrNot()
    {
        String name = "index-that-may-exist";
        assertFalse( graphDb.index().existsForNodes( name ) );
        graphDb.index().forNodes( name );
        assertTrue( graphDb.index().existsForNodes( name ) );

        assertFalse( graphDb.index().existsForRelationships( name ) );
        graphDb.index().forRelationships( name );
        assertTrue( graphDb.index().existsForRelationships( name ) );
    }

    private void makeSureAdditionsCanBeRemoved( boolean restartTx )
    {
        Index<Node> index = nodeIndex( "index", LuceneIndexProvider.EXACT_CONFIG );
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
        index.delete();
    }

    @Test
    public void makeSureAdditionsCanBeRemoved()
    {
        makeSureAdditionsCanBeRemoved( true );
    }

    private void makeSureSomeAdditionsCanBeRemoved( boolean restartTx )
    {
        Index<Node> index = nodeIndex( "index", LuceneIndexProvider.EXACT_CONFIG );
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
        index.delete();
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

    private void makeSureThereCanBeMoreThanOneValueForAKeyAndEntity(
            boolean restartTx )
    {
        Index<Node> index = nodeIndex( "index", LuceneIndexProvider.EXACT_CONFIG );
        String key = "name";
        String value1 = "Lucene";
        String value2 = "Index";
        String value3 = "Rules";
        assertThat( index.query( key, "*" ), isEmpty() );
        Node node = graphDb.createNode();
        index.add( node, key, value1 );
        index.add( node, key, value2 );
        if ( restartTx )
        {
            restartTx();
        }
        index.add( node, key, value3 );
        assertThat( index.get( key, value1 ), contains( node ) );
        assertThat( index.get( key, value2 ), contains( node ) );
        assertThat( index.get( key, value3 ), contains( node ) );
        assertThat( index.get( key, "whatever" ), isEmpty() );
        restartTx();
        assertThat( index.get( key, value1 ), contains( node ) );
        assertThat( index.get( key, value2 ), contains( node ) );
        assertThat( index.get( key, value3 ), contains( node ) );
        assertThat( index.get( key, "whatever" ), isEmpty() );
        index.delete();
    }

    @Test
    public void shouldNotGetLatestTxModificationsWhenChoosingSpeedQueries()
    {
        Index<Node> index = nodeIndex( "indexFooBar", LuceneIndexProvider.EXACT_CONFIG );
        Node node = graphDb.createNode();
        index.add( node, "key", "value" );
        QueryContext queryContext = new QueryContext( "value" ).tradeCorrectnessForSpeed();
        assertThat( index.query( "key", queryContext ), isEmpty() );
        assertThat( index.query( "key", "value" ), contains( node ) );
    }

    @Test
    public void makeSureArrayValuesAreSupported()
    {
        Index<Node> index = nodeIndex( "index", LuceneIndexProvider.EXACT_CONFIG );
        String key = "name";
        String value1 = "Lucene";
        String value2 = "Index";
        String value3 = "Rules";
        assertThat( index.query( key, "*" ), isEmpty() );
        Node node = graphDb.createNode();
        index.add( node, key, new String[]{value1, value2, value3} );
        assertThat( index.get( key, value1 ), contains( node ) );
        assertThat( index.get( key, value2 ), contains( node ) );
        assertThat( index.get( key, value3 ), contains( node ) );
        assertThat( index.get( key, "whatever" ), isEmpty() );
        restartTx();
        assertThat( index.get( key, value1 ), contains( node ) );
        assertThat( index.get( key, value2 ), contains( node ) );
        assertThat( index.get( key, value3 ), contains( node ) );
        assertThat( index.get( key, "whatever" ), isEmpty() );

        index.remove( node, key, new String[]{value2, value3} );
        assertThat( index.get( key, value1 ), contains( node ) );
        assertThat( index.get( key, value2 ), isEmpty() );
        assertThat( index.get( key, value3 ), isEmpty() );
        restartTx();
        assertThat( index.get( key, value1 ), contains( node ) );
        assertThat( index.get( key, value2 ), isEmpty() );
        assertThat( index.get( key, value3 ), isEmpty() );
        index.delete();
    }

    @Test
    public void makeSureWildcardQueriesCanBeAsked()
    {
        Index<Node> index = nodeIndex( "index", LuceneIndexProvider.EXACT_CONFIG );
        String key = "name";
        String value1 = "neo4j";
        String value2 = "nescafe";
        Node node1 = graphDb.createNode();
        Node node2 = graphDb.createNode();
        index.add( node1, key, value1 );
        index.add( node2, key, value2 );

        assertThat( index.query( key, "neo*" ), contains( node1 ) );
        assertThat( index.query( key, "n?o4j" ), contains( node1 ) );
        assertThat( index.query( key, "ne*" ), contains( node1, node2 ) );
        assertThat( index.query( key + ":neo4j" ), contains( node1 ) );
        assertThat( index.query( key + ":neo*" ), contains( node1 ) );
        assertThat( index.query( key + ":n?o4j" ), contains( node1 ) );
        assertThat( index.query( key + ":ne*" ), contains( node1, node2 ) );

        restartTx();
        
        assertThat( index.query( key, "neo*" ), contains( node1 ) );
        assertThat( index.query( key, "n?o4j" ), contains( node1 ) );
        assertThat( index.query( key, "ne*" ), contains( node1, node2 ) );
        assertThat( index.query( key + ":neo4j" ), contains( node1 ) );
        assertThat( index.query( key + ":neo*" ), contains( node1 ) );
        assertThat( index.query( key + ":n?o4j" ), contains( node1 ) );
        assertThat( index.query( key + ":ne*" ), contains( node1, node2 ) );
        index.delete();
    }

    @Test
    public void makeSureCompositeQueriesCanBeAsked()
    {
        Index<Node> index = nodeIndex( "index", LuceneIndexProvider.EXACT_CONFIG );
        Node neo = graphDb.createNode();
        Node trinity = graphDb.createNode();
        index.add( neo, "username", "neo@matrix" );
        index.add( neo, "sex", "male" );
        index.add( trinity, "username", "trinity@matrix" );
        index.add( trinity, "sex", "female" );

        assertThat( index.query( "username:*@matrix AND sex:male" ), contains( neo ) );
        assertThat( index.query( new QueryContext( "username:*@matrix sex:male" ).defaultOperator( Operator.AND ) ), contains( neo ) );
        assertThat( index.query( "username:*@matrix OR sex:male" ), contains( neo, trinity ) );
        assertThat( index.query( new QueryContext( "username:*@matrix sex:male" ).defaultOperator( Operator.OR ) ), contains( neo, trinity ) );

        restartTx();
        
        assertThat( index.query( "username:*@matrix AND sex:male" ), contains( neo ) );
        assertThat( index.query( new QueryContext( "username:*@matrix sex:male" ).defaultOperator( Operator.AND ) ), contains( neo ) );
        assertThat( index.query( "username:*@matrix OR sex:male" ), contains( neo, trinity ) );
        assertThat( index.query( new QueryContext( "username:*@matrix sex:male" ).defaultOperator( Operator.OR ) ), contains( neo, trinity ) );
        index.delete();
    }

    private <T extends PropertyContainer> void doSomeRandomUseCaseTestingWithExactIndex(
            Index<T> index, EntityCreator<T> creator )
    {
        String name = "name";
        String mattias = "Mattias Persson";
        String title = "title";
        String hacker = "Hacker";

        assertThat( index.get( name, mattias ), isEmpty() );

        T entity1 = creator.create();
        T entity2 = creator.create();

        assertNull( index.get( name, mattias ).getSingle() );
        index.add( entity1, name, mattias );
        assertThat( index.get( name, mattias ), contains( entity1 ) );

        assertContains( index.query( name, "\"" + mattias + "\"" ), entity1 );
        assertContains( index.query( "name:\"" + mattias + "\"" ), entity1 );

        assertEquals( entity1, index.get( name, mattias ).getSingle() );

        assertContains( index.query( "name", "Mattias*" ), entity1 );

        commitTx();
        assertThat( index.get( name, mattias ), contains( entity1 ) );
        assertThat( index.query( name, "\"" + mattias + "\"" ), contains( entity1 ) );
        assertThat( index.query( "name:\"" + mattias + "\"" ), contains( entity1 ) );
        assertEquals( entity1, index.get( name, mattias ).getSingle() );
        assertThat( index.query( "name", "Mattias*" ), contains( entity1 ) );

        beginTx();
        index.add( entity2, title, hacker );
        index.add( entity1, title, hacker );
        assertThat( index.get( name, mattias ), contains( entity1 ) );
        assertThat( index.get( title, hacker ), contains( entity1, entity2 ) );

        assertContains( index.query( "name:\"" + mattias + "\" OR title:\"" +
                hacker + "\"" ), entity1, entity2 );

        commitTx();
        assertThat( index.get( name, mattias ), contains( entity1 ) );
        assertThat( index.get( title, hacker ), contains( entity1, entity2 ) );
        assertThat( index.query( "name:\"" + mattias + "\" OR title:\"" + hacker + "\"" ), contains( entity1, entity2 ) );
        assertThat( index.query( "name:\"" + mattias + "\" AND title:\"" +
                hacker + "\"" ), contains( entity1 ) );

        beginTx();
        index.remove( entity2, title, hacker );
        assertThat( index.get( name, mattias ), contains( entity1 ) );
        assertThat( index.get( title, hacker ), contains( entity1 ) );

        assertContains( index.query( "name:\"" + mattias + "\" OR title:\"" +
                hacker + "\"" ), entity1 );

        commitTx();
        assertThat( index.get( name, mattias ), contains( entity1 ) );
        assertThat( index.get( title, hacker ), contains( entity1 ) );
        assertThat( index.query( "name:\"" + mattias + "\" OR title:\"" +
                hacker + "\"" ), contains( entity1 ) );

        beginTx();
        index.remove( entity1, title, hacker );
        index.remove( entity1, name, mattias );
        index.delete();
        commitTx();
    }

    @Test
    public void doSomeRandomUseCaseTestingWithExactNodeIndex()
    {
        doSomeRandomUseCaseTestingWithExactIndex( nodeIndex( "index",
                LuceneIndexProvider.EXACT_CONFIG ), NODE_CREATOR );
    }

    @Test
    public void doSomeRandomUseCaseTestingWithExactRelationshipIndex()
    {
        doSomeRandomUseCaseTestingWithExactIndex( relationshipIndex( "index",
                LuceneIndexProvider.EXACT_CONFIG ), RELATIONSHIP_CREATOR );
    }

    private <T extends PropertyContainer> void doSomeRandomTestingWithFulltextIndex(
            Index<T> index,
            EntityCreator<T> creator )
    {
        T entity1 = creator.create();
        T entity2 = creator.create();

        String key = "name";
        index.add( entity1, key, "The quick brown fox" );
        index.add( entity2, key, "brown fox jumped over" );

        assertThat( index.get( key, "The quick brown fox" ), contains( entity1 ) );
        assertThat( index.get( key, "brown fox jumped over" ), contains( entity2 ) );
        assertContains( index.query( key, "quick" ), entity1 );
        assertContains( index.query( key, "brown" ), entity1, entity2 );
        assertContains( index.query( key, "quick OR jumped" ), entity1, entity2 );
        assertContains( index.query( key, "brown AND fox" ), entity1, entity2 );

        restartTx();
        
        assertThat( index.get( key, "The quick brown fox" ), contains( entity1 ) );
        assertThat( index.get( key, "brown fox jumped over" ), contains( entity2 ) );
        assertThat( index.query( key, "quick" ), contains( entity1 ) );
        assertThat( index.query( key, "brown" ), contains( entity1, entity2 ) );
        assertThat( index.query( key, "quick OR jumped" ), contains( entity1, entity2 ) );
        assertThat( index.query( key, "brown AND fox" ), contains( entity1, entity2 ) );

        index.delete();
    }

    @Test
    public void doSomeRandomTestingWithNodeFulltextInde()
    {
        doSomeRandomTestingWithFulltextIndex( nodeIndex( "fulltext",
                LuceneIndexProvider.FULLTEXT_CONFIG ), NODE_CREATOR );
    }

    @Test
    public void doSomeRandomTestingWithRelationshipFulltextInde()
    {
        doSomeRandomTestingWithFulltextIndex( relationshipIndex( "fulltext",
                LuceneIndexProvider.FULLTEXT_CONFIG ), RELATIONSHIP_CREATOR );
    }

    @Test
    public void testNodeLocalRelationshipIndex()
    {
        RelationshipIndex index = relationshipIndex( "locality",
                LuceneIndexProvider.EXACT_CONFIG );

        RelationshipType type = DynamicRelationshipType.withName( "YO" );
        Node startNode = graphDb.createNode();
        Node endNode1 = graphDb.createNode();
        Node endNode2 = graphDb.createNode();
        Relationship rel1 = startNode.createRelationshipTo( endNode1, type );
        Relationship rel2 = startNode.createRelationshipTo( endNode2, type );
        index.add( rel1, "name", "something" );
        index.add( rel2, "name", "something" );
        restartTx();
        assertThat( index.query( "name:something" ), contains( rel1, rel2 ) );
        assertThat( index.query( "name:something", null, endNode1 ), contains( rel1 ) );
        assertThat( index.query( "name:something", startNode, endNode2 ), contains( rel2 ) );
        assertThat( index.query( null, startNode, endNode1 ), contains( rel1 ) );
        assertThat( index.get( "name", "something", null, endNode1 ), contains( rel1 ) );
        assertThat( index.get( "name", "something", startNode, endNode2 ), contains( rel2 ) );
        assertThat( index.get( null, null, startNode, endNode1 ), contains( rel1 ) );
        rel2.delete();
        rel1.delete();
        startNode.delete();
        endNode1.delete();
        endNode2.delete();
        index.delete();
    }

    @Ignore( "This breaks on automated build system - but nohwere else" )
    @Test
    public void testSortByRelevance()
    {
        Index<Node> index = nodeIndex( "relevance", LuceneIndexProvider.EXACT_CONFIG );

        Node node1 = graphDb.createNode();
        Node node2 = graphDb.createNode();
        Node node3 = graphDb.createNode();
        index.add( node1, "name", "something" );
        index.add( node2, "name", "something" );
        index.add( node2, "foo", "yes" );
        index.add( node3, "name", "something" );
        index.add( node3, "foo", "yes" );
        index.add( node3, "bar", "yes" );
        restartTx();

        // This section fails in hudson - does INDEXORDER *really* mean "insertion order"?
        IndexHits<Node> hits = index.query(
                new QueryContext( "+name:something foo:yes bar:yes" ).sort( Sort.INDEXORDER ) );
        assertEquals( node1, hits.next() );
        assertEquals( node2, hits.next() );
        assertEquals( node3, hits.next() );
        assertFalse( hits.hasNext() );

        hits = index.query(
                new QueryContext( "+name:something foo:yes bar:yes" ).sort( Sort.RELEVANCE ) );
        assertEquals( node3, hits.next() );
        assertEquals( node2, hits.next() );
        assertEquals( node1, hits.next() );
        assertFalse( hits.hasNext() );
        index.delete();
        node1.delete();
        node2.delete();
        node3.delete();
    }

    @Test
    public void testSorting()
    {
        Index<Node> index = nodeIndex( "sort", LuceneIndexProvider.EXACT_CONFIG );
        String name = "name";
        String title = "title";
        String other = "other";
        String sex = "sex";
        Node adam = graphDb.createNode();
        Node adam2 = graphDb.createNode();
        Node jack = graphDb.createNode();
        Node eva = graphDb.createNode();

        index.add( adam, name, "Adam" );
        index.add( adam, title, "Software developer" );
        index.add( adam, sex, "male" );
        index.add( adam, other, "aaa" );
        index.add( adam2, name, "Adam" );
        index.add( adam2, title, "Blabla" );
        index.add( adam2, sex, "male" );
        index.add( adam2, other, "bbb" );
        index.add( jack, name, "Jack" );
        index.add( jack, title, "Apple sales guy" );
        index.add( jack, sex, "male" );
        index.add( jack, other, "ccc" );
        index.add( eva, name, "Eva" );
        index.add( eva, title, "Secretary" );
        index.add( eva, sex, "female" );
        index.add( eva, other, "ddd" );

        assertContainsInOrder( index.query( new QueryContext( "name:*" ).sort( name, title ) ), adam2, adam, eva, jack );
        assertContainsInOrder( index.query( new QueryContext( "name:*" ).sort( name, other ) ), adam, adam2, eva, jack );
        assertContainsInOrder( index.query( new QueryContext( "name:*" ).sort( sex, title ) ), eva, jack, adam2, adam );
        assertContainsInOrder( index.query( name, new QueryContext( "*" ).sort( sex, title ) ), eva, jack, adam2, adam );

        restartTx();

        assertContainsInOrder( index.query( new QueryContext( "name:*" ).sort( name, title ) ), adam2, adam, eva, jack );
        assertContainsInOrder( index.query( new QueryContext( "name:*" ).sort( name, other ) ), adam, adam2, eva, jack );
        assertContainsInOrder( index.query( new QueryContext( "name:*" ).sort( sex, title ) ), eva, jack, adam2, adam );
        assertContainsInOrder( index.query( name, new QueryContext( "*" ).sort( sex, title ) ), eva, jack, adam2, adam );
        assertContainsInOrder( index.query( new QueryContext( "name:*" ).sort( name, title ).topDocs( 2 ) ), adam2, adam );
    }

    @Test
    public void testNumericValues()
    {
        Index<Node> index = nodeIndex( "numeric", LuceneIndexProvider.EXACT_CONFIG );

        Node node10 = graphDb.createNode();
        Node node6 = graphDb.createNode();
        Node node31 = graphDb.createNode();

        String key = "key";
        index.add( node10, key, numeric( 10 ) );
        index.add( node6, key, numeric( 6 ) );
        index.add( node31, key, numeric( 31 ) );

        assertThat( index.query( NumericRangeQuery.newIntRange( key, 4, 40, true, true ) ), contains( node10, node6, node31 ) );
        assertThat( index.query( NumericRangeQuery.newIntRange( key, 6, 15, true, true ) ), contains( node10, node6 ) );
        assertThat( index.query( NumericRangeQuery.newIntRange( key, 6, 15, false, true ) ), contains( node10 ) );

        restartTx();
        
        assertThat( index.query( NumericRangeQuery.newIntRange( key, 4, 40, true, true ) ), contains( node10, node6, node31 ) );
        assertThat( index.query( NumericRangeQuery.newIntRange( key, 6, 15, true, true ) ), contains( node10, node6 ) );
        assertThat( index.query( NumericRangeQuery.newIntRange( key, 6, 15, false, true ) ), contains( node10 ) );
    }

    @Test
    public void testRemoveNumericValues()
    {
        Index<Node> index = nodeIndex( "numeric2", LuceneIndexProvider.EXACT_CONFIG );
        Node node1 = graphDb.createNode();
        Node node2 = graphDb.createNode();
        String key = "key";
        index.add( node1, key, new ValueContext( 15 ).indexNumeric() );
        index.add( node2, key, new ValueContext( 5 ).indexNumeric() );
        index.remove( node1, key, new ValueContext( 15 ).indexNumeric() );

        assertThat( index.query( NumericRangeQuery.newIntRange( key, 0, 20, false, false ) ), contains( node2 ) );

        index.remove( node2, key, new ValueContext( 5 ).indexNumeric() );

        assertThat( index.query( NumericRangeQuery.newIntRange( key, 0, 20, false, false ) ), isEmpty() );

        restartTx();
        assertThat( index.query( NumericRangeQuery.newIntRange( key, 0, 20, false, false ) ), isEmpty() );

        index.add( node1, key, new ValueContext( 15 ).indexNumeric() );
        index.add( node2, key, new ValueContext( 5 ).indexNumeric() );
        restartTx();
        assertThat( index.query( NumericRangeQuery.newIntRange( key, 0, 20, false, false ) ), contains( node1, node2 ) );
        index.remove( node1, key, new ValueContext( 15 ).indexNumeric() );

        assertThat( index.query( NumericRangeQuery.newIntRange( key, 0, 20, false, false ) ), contains( node2 ) );

        restartTx();
        assertThat( index.query( NumericRangeQuery.newIntRange( key, 0, 20, false, false ) ), contains( node2 ) );
    }

    @Test
    public void testIndexNumberAsString()
    {
        Index<Node> index = nodeIndex( "nums", LuceneIndexProvider.EXACT_CONFIG );
        Node node1 = graphDb.createNode();
        index.add( node1, "key", 10 );
        assertEquals( node1, index.get( "key", 10 ).getSingle() );
        assertEquals( node1, index.get( "key", "10" ).getSingle() );
        assertEquals( node1, index.query( "key", 10 ).getSingle() );
        assertEquals( node1, index.query( "key", "10" ).getSingle() );

        restartTx();
        assertEquals( node1, index.get( "key", 10 ).getSingle() );
        assertEquals( node1, index.get( "key", "10" ).getSingle() );
        assertEquals( node1, index.query( "key", 10 ).getSingle() );
        assertEquals( node1, index.query( "key", "10" ).getSingle() );
    }

    private <T extends PropertyContainer> void testInsertionSpeed(
            Index<T> index,
            EntityCreator<T> creator )
    {
        long t = System.currentTimeMillis();
        for ( int i = 0; i < 300000; i++ )
        {
            T entity = creator.create();
            if ( i % 5000 == 5 )
            {
                index.query( new TermQuery( new Term( "name", "The name " + i ) ) );
            }
            IteratorUtil.lastOrNull( (Iterable<T>) index.query( new QueryContext( new TermQuery( new Term( "name", "The name " + i ) ) ).tradeCorrectnessForSpeed() ) );
            IteratorUtil.lastOrNull( (Iterable<T>) index.get( "name", "The name " + i ) );
            index.add( entity, "name", "The name " + i );
            index.add( entity, "title", "Some title " + i );
            index.add( entity, "something", i + "Nothing" );
            index.add( entity, "else", i + "kdfjkdjf" + i );
            if ( i % 10000 == 0 )
            {
                restartTx();
                System.out.println( i );
            }
        }
        System.out.println( "insert:" + ( System.currentTimeMillis() - t ) );

        t = System.currentTimeMillis();
        int count = 1000;
        int resultCount = 0;
        for ( int i = 0; i < count; i++ )
        {
            for ( T entity : index.get( "name", "The name " + i*900 ) )
            {
                resultCount++;
            }
        }
        System.out.println( "get(" + resultCount + "):" + (double)( System.currentTimeMillis() - t ) / (double)count );

        t = System.currentTimeMillis();
        resultCount = 0;
        for ( int i = 0; i < count; i++ )
        {
            for ( T entity : index.get( "something", i*900 + "Nothing" ) )
            {
                resultCount++;
            }
        }
        System.out.println( "get(" + resultCount + "):" + (double)( System.currentTimeMillis() - t ) / (double)count );
    }
    
    @Ignore
    @Test
    public void testNodeInsertionSpeed()
    {
        testInsertionSpeed( nodeIndex( "insertion-speed",
                LuceneIndexProvider.EXACT_CONFIG ), NODE_CREATOR );
    }

    @Ignore
    @Test
    public void testNodeFulltextInsertionSpeed()
    {
        testInsertionSpeed( nodeIndex( "insertion-speed-full",
                LuceneIndexProvider.FULLTEXT_CONFIG ), NODE_CREATOR );
    }

    @Ignore
    @Test
    public void testRelationshipInsertionSpeed()
    {
        testInsertionSpeed( relationshipIndex( "insertion-speed",
                LuceneIndexProvider.EXACT_CONFIG ), new FastRelationshipCreator() );
    }
    
    @Test( expected = IllegalArgumentException.class )
    public void makeSureIndexGetsCreatedImmediately()
    {
        // Since index creation is done outside of the normal transactions,
        // a rollback will not roll back index creation.
        
        nodeIndex( "immediate-index", LuceneIndexProvider.FULLTEXT_CONFIG );
        assertTrue( graphDb.index().existsForNodes( "immediate-index" ) );
        rollbackTx();
        assertTrue( graphDb.index().existsForNodes( "immediate-index" ) );
        nodeIndex( "immediate-index", LuceneIndexProvider.EXACT_CONFIG );
    }
    
    @Test
    public void makeSureFulltextConfigIsCaseInsensitiveByDefault()
    {
        Index<Node> index = nodeIndex( "ft-case-sensitive", LuceneIndexProvider.FULLTEXT_CONFIG );
        Node node = graphDb.createNode();
        String key = "name";
        String value = "Mattias Persson";
        index.add( node, key, value );
        for ( int i = 0; i < 2; i++ )
        {
            assertThat( index.query( "name", "[A TO Z]" ), contains( node ) );
            assertThat( index.query( "name", "[a TO z]" ), contains( node ) );
            assertThat( index.query( "name", "Mattias" ), contains( node ) );
            assertThat( index.query( "name", "mattias" ), contains( node ) );
            assertThat( index.query( "name", "Matt*" ), contains( node ) );
            assertThat( index.query( "name", "matt*" ), contains( node ) );
            restartTx();
        }
    }
    
    @Test
    public void makeSureFulltextIndexCanBeCaseSensitive()
    {
        Index<Node> index = nodeIndex( "ft-case-insensitive", MapUtil.stringMap(
                new HashMap<String, String>( LuceneIndexProvider.FULLTEXT_CONFIG ),
                        "to_lower_case", "false" ) );
        Node node = graphDb.createNode();
        String key = "name";
        String value = "Mattias Persson";
        index.add( node, key, value );
        for ( int i = 0; i < 2; i++ )
        {
            assertThat( index.query( "name", "[A TO Z]" ), contains( node ) );
            assertThat( index.query( "name", "[a TO z]" ), isEmpty() );
            assertThat( index.query( "name", "Matt*" ), contains( node ) );
            assertThat( index.query( "name", "matt*" ), isEmpty() );
            assertThat( index.query( "name", "Persson" ), contains( node ) );
            assertThat( index.query( "name", "persson" ), isEmpty() );
            restartTx();
        }
    }
    
    @Test
    public void makeSureCustomAnalyzerCanBeUsed()
    {
        CustomAnalyzer.called = false;
        Index<Node> index = nodeIndex( "w-custom-analyzer", MapUtil.stringMap(
                "provider", "lucene", "analyzer", org.neo4j.index.impl.lucene.CustomAnalyzer.class.getName(),
                "to_lower_case", "true" ) );
        Node node = graphDb.createNode();
        String key = "name";
        String value = "The value";
        index.add( node, key, value );
        restartTx();
        assertTrue( CustomAnalyzer.called );
        assertThat( index.query( key, "[A TO Z]" ), contains( node ) );
    }
    
    @Test
    public void makeSureCustomAnalyzerCanBeUsed2()
    {
        CustomAnalyzer.called = false;
        Index<Node> index = nodeIndex( "w-custom-analyzer-2", MapUtil.stringMap(
                "provider", "lucene", "analyzer", org.neo4j.index.impl.lucene.CustomAnalyzer.class.getName(),
                "to_lower_case", "true", "type", "fulltext" ) );
        Node node = graphDb.createNode();
        String key = "name";
        String value = "The value";
        index.add( node, key, value );
        restartTx();
        assertTrue( CustomAnalyzer.called );
        assertThat( index.query( key, "[A TO Z]" ), contains( node ) );
    }
    
    @Ignore
    @Test
    public void makeSureFilesAreClosedProperly() throws Exception
    {
        commitTx();
        final Index<Node> index = nodeIndex( "open-files", LuceneIndexProvider.EXACT_CONFIG );
        final long time = System.currentTimeMillis();
        final CountDownLatch latch = new CountDownLatch( 30 );
        for ( int t = 0; t < latch.getCount(); t++ ) 
        {
            new Thread()
            {
                public void run()
                {
                    for ( int i = 0; System.currentTimeMillis() - time < 100*1000; i++ )
                    {
                        if ( i%10 == 0 )
                        {
                            if ( i%100 == 0 )
                            {
                                int size = 0;
                                int type = (int)(System.currentTimeMillis()%3);
                                if ( type == 0 )
                                {
                                    IndexHits<Node> itr = index.get( "key", "value5" );
                                    try
                                    {
                                        itr.getSingle();
                                    }
                                    catch ( NoSuchElementException e )
                                    {
            
                                    }
                                    size = 99;
                                }
                                else if ( type == 1 )
                                {
                                    IndexHits<Node> itr = index.get( "key", "value5" );
                                    for ( ;itr.hasNext() && size < 5; size++ )
                                    {
                                        itr.next();
                                    }
                                    itr.close();
                                }
                                else
                                {
                                    IndexHits<Node> itr = index.get( "key", "crap value" ); /* Will return 0 hits */
                                    // Iterate over the hits sometimes (it's always gonna be 0 sized)
                                    if ( System.currentTimeMillis()%10 > 5 )
                                    {
                                        IteratorUtil.count( (Iterator<Node>) itr );
                                    }
                                }
                                
                                System.out.println( "C iterated " + size + " only" );
                            }
                            else
                            {
                                int size = IteratorUtil.count( (Iterator<Node>) index.get( "key", "value5" ) );
                                System.out.println( "hit size:" + size );
                            }
                        }
                        else
                        {
                            Transaction tx = graphDb.beginTx();
                            try
                            {
                                for ( int ii = 0; ii < 20; ii++ )
                                {
                                    Node node = graphDb.createNode();
                                    index.add( node, "key", "value" + ii );
                                }
                                tx.success();
                            }
                            finally
                            {
                                tx.finish();
                            }
                        }
                    }
                    latch.countDown();
                }
            }.start();
        }
        latch.await();
    }
    
    @Test
    public void makeSureIndexNameAndConfigCanBeReachedFromIndex()
    {
        String indexName = "my-index-1";
        Index<Node> nodeIndex = nodeIndex( indexName, LuceneIndexProvider.EXACT_CONFIG );
        assertEquals( indexName, nodeIndex.getName() );
        assertEquals( LuceneIndexProvider.EXACT_CONFIG, graphDb.index().getConfiguration( nodeIndex ) );
        
        String indexName2 = "my-index-2";
        Index<Relationship> relIndex = relationshipIndex( indexName2, LuceneIndexProvider.FULLTEXT_CONFIG );
        assertEquals( indexName2, relIndex.getName() );
        assertEquals( LuceneIndexProvider.FULLTEXT_CONFIG, graphDb.index().getConfiguration( relIndex ) );
    }
    
    @Test
    public void testStringQueryVsQueryObject() throws IOException
    {
        Index<Node> index = nodeIndex( "query-diff", LuceneIndexProvider.FULLTEXT_CONFIG );
        Node node = graphDb.createNode();
        index.add( node, "name", "Mattias Persson" );
        restartTx();
        assertContains( index.query( "name:Mattias AND name:Per*" ), node );
        assertContains( index.query( "name:mattias" ), node );
        assertContains( index.query( new TermQuery( new Term( "name", "mattias" ) ) ), node );
        assertNull( index.query( new TermQuery( new Term( "name", "Mattias" ) ) ).getSingle() );
    }
    
    private <T extends PropertyContainer> void testAbandonedIds( EntityCreator<T> creator,
            Index<T> index )
    {
        // TODO This doesn't actually test that they are deleted, it just triggers it
        // so that you manually can inspect what's going on
        T a = creator.create();
        T b = creator.create();
        T c = creator.create();
        String key = "name";
        String value = "value";
        index.add( a, key, value );
        index.add( b, key, value );
        index.add( c, key, value );
        restartTx();
        
        creator.delete( b );
        restartTx();
        
        IteratorUtil.count( (Iterator<Node>) index.get( key, value ) );
        rollbackTx();
        beginTx();
        
        IteratorUtil.count( (Iterator<Node>) index.get( key, value ) );
        index.add( c, "something", "whatever" );
        restartTx();
        
        IteratorUtil.count( (Iterator<Node>) index.get( key, value ) );
    }
    
    @Test
    public void testAbandonedNodeIds()
    {
        testAbandonedIds( NODE_CREATOR, nodeIndex( "abandoned", LuceneIndexProvider.EXACT_CONFIG ) );
    }
    
    @Test
    public void testAbandonedRelIds()
    {
        testAbandonedIds( RELATIONSHIP_CREATOR, relationshipIndex( "abandoned", LuceneIndexProvider.EXACT_CONFIG ) );
    }
    
    @Test
    public void makeSureYouCanRemoveFromRelationshipIndex()
    {
        Node n1 = graphDb.createNode();
        Node n2 = graphDb.createNode();
        Relationship r = n1.createRelationshipTo( n2, DynamicRelationshipType.withName( "foo" ) );
        RelationshipIndex index = graphDb.index().forRelationships( "rel-index" );
        String key = "bar";
        index.remove( r, key, "value" );
        index.add( r, key, "otherValue" );
        assertThat( index.get( key, "value" ), isEmpty() );
        assertThat( index.get( key, "otherValue" ), contains( r ) );
        restartTx();
        assertThat( index.get( key, "value" ), isEmpty() );
        assertThat( index.get( key, "otherValue" ), contains( r ) );
    }
    
    @Test
    public void makeSureYouCanGetEntityTypeFromIndex()
    {
        Index<Node> nodeIndex = nodeIndex( "type-test", MapUtil.stringMap( "provider", "lucene", "type", "exact" ) );
        Index<Relationship> relIndex = relationshipIndex( "type-test", MapUtil.stringMap( "provider", "lucene", "type", "exact" ) );
        assertEquals( Node.class, nodeIndex.getEntityType() );
        assertEquals( Relationship.class, relIndex.getEntityType() );
    }
    
    @Test
    public void makeSureConfigurationCanBeModified()
    {
        Index<Node> index = nodeIndex( "conf-index", LuceneIndexProvider.EXACT_CONFIG );
        try
        {
            graphDb.index().setConfiguration( index, "provider", "something" );
            fail( "Shouldn't be able to modify provider" );
        }
        catch ( IllegalArgumentException e ) { /* Good*/ }
        try
        {
            graphDb.index().removeConfiguration( index, "provider" );
            fail( "Shouldn't be able to modify provider" );
        }
        catch ( IllegalArgumentException e ) { /* Good*/ }

        String key = "my-key";
        String value = "my-value";
        String newValue = "my-new-value";
        assertNull( graphDb.index().setConfiguration( index, key, value ) );
        assertEquals( value, graphDb.index().getConfiguration( index ).get( key ) );
        assertEquals( value, graphDb.index().setConfiguration( index, key, newValue ) );
        assertEquals( newValue, graphDb.index().getConfiguration( index ).get( key ) );
        assertEquals( newValue, graphDb.index().removeConfiguration( index, key ) );
        assertNull( graphDb.index().getConfiguration( index ).get( key ) );
    }
    
    @Test
    public void makeSureSlightDifferencesInIndexConfigCanBeSupplied()
    {
        Map<String, String> config = MapUtil.stringMap( "provider", "lucene", "type", "fulltext" );
        String name = "the-name";
        nodeIndex( name, config );
        nodeIndex( name, MapUtil.stringMap( new HashMap<String, String>( config ), "to_lower_case", "true" ) );
        try
        {
            nodeIndex( name, MapUtil.stringMap( new HashMap<String, String>( config ), "to_lower_case", "false" ) );
            fail( "Shouldn't be able to get index with these kinds of differences in config" );
        }
        catch ( IllegalArgumentException e ) { /* */ }
        nodeIndex( name, MapUtil.stringMap( new HashMap<String, String>( config ), "whatever", "something" ) );
    }
    
    @Test
    public void testScoring()
    {
        Index<Node> index = nodeIndex( "score-index", LuceneIndexProvider.FULLTEXT_CONFIG );
        Node node1 = graphDb.createNode();
        Node node2 = graphDb.createNode();
        String key = "text";
        // Where the heck did I get this sentence from?
        index.add( node1, key, "a time where no one was really awake" ); 
        index.add( node2, key, "once upon a time there was" );
        restartTx();
        
        IndexHits<Node> hits = index.query( key, new QueryContext( "once upon a time was" ).sort( Sort.RELEVANCE ) );
        Node hit1 = hits.next();
        float score1 = hits.currentScore();
        Node hit2 = hits.next();
        float score2 = hits.currentScore();
        assertEquals( node2, hit1 );
        assertEquals( node1, hit2 );
        assertTrue( score1 > score2 );
    }
    
    @Test
    public void testTopHits()
    {
        Index<Relationship> index = relationshipIndex( "topdocs", LuceneIndexProvider.FULLTEXT_CONFIG );
        EntityCreator<Relationship> creator = RELATIONSHIP_CREATOR;
        String key = "text";
        Relationship rel1 = creator.create( key, "one two three four five six seven eight nine ten" );
        Relationship rel2 = creator.create( key, "one two three four five six seven eight other things" );
        Relationship rel3 = creator.create( key, "one two three four five six some thing else" );
        Relationship rel4 = creator.create( key, "one two three four five what ever" );
        Relationship rel5 = creator.create( key, "one two three four all that is good and bad" );
        Relationship rel6 = creator.create( key, "one two three hill or something" );
        Relationship rel7 = creator.create( key, "one two other time than this" );
        index.add( rel2, key, rel2.getProperty( key ) );
        index.add( rel1, key, rel1.getProperty( key ) );
        index.add( rel3, key, rel3.getProperty( key ) );
        index.add( rel7, key, rel7.getProperty( key ) );
        index.add( rel5, key, rel5.getProperty( key ) );
        index.add( rel4, key, rel4.getProperty( key ) );
        index.add( rel6, key, rel6.getProperty( key ) );
        restartTx();
        
        String query = "one two three four five six seven";
        assertContainsInOrder( index.query( key, new QueryContext( query ).topDocs( 3 ).sort(
                Sort.RELEVANCE ) ), rel1, rel2, rel3 );
    }
    
    @Test
    public void testSimilarity()
    {
        Index<Node> index = nodeIndex( "similarity", MapUtil.stringMap( "provider", "lucene",
                "type", "fulltext", "similarity", DefaultSimilarity.class.getName() ) );
        Node node = graphDb.createNode();
        index.add( node, "key", "value" );
        restartTx();
        assertContains( index.get( "key", "value" ), node );
    }
    
    @Test
    public void testCombinedHitsSizeProblem()
    {
        Index<Node> index = nodeIndex( "size-npe", LuceneIndexProvider.EXACT_CONFIG );
        Node node1 = graphDb.createNode();
        Node node2 = graphDb.createNode();
        Node node3 = graphDb.createNode();
        String key = "key";
        String value = "value";
        index.add( node1, key, value );
        index.add( node2, key, value );
        restartTx();
        index.add( node3, key, value );
        IndexHits<Node> hits = index.get( key, value );
        assertEquals( 3, hits.size() );
    }
}
