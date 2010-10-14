/**
 * Copyright (c) 2002-2010 "Neo Technology,"
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

import org.apache.lucene.queryParser.QueryParser.Operator;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Sort;
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
import org.neo4j.index.Neo4jTestCase;
import org.neo4j.kernel.EmbeddedGraphDatabase;

import java.io.File;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.neo4j.index.Neo4jTestCase.assertOrderedCollection;
import static org.neo4j.index.impl.lucene.Contains.contains;
import static org.neo4j.index.impl.lucene.IsEmpty.isEmpty;
import static org.neo4j.index.impl.lucene.ValueContext.numeric;

public class TestLuceneIndex
{
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

    private static final RelationshipType TEST_TYPE =
            DynamicRelationshipType.withName( "TEST_TYPE" );
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
                    return graphDb.createNode().createRelationshipTo( graphDb.createNode(), TEST_TYPE );
                }
            };

    static class FastRelationshipCreator implements EntityCreator<Relationship>
    {
        private Node node, otherNode;

        public Relationship create()
        {
            if ( node == null )
            {
                node = graphDb.createNode();
                otherNode = graphDb.createNode();
            }
            return node.createRelationshipTo( otherNode, TEST_TYPE );
        }
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

        assertQueryNotPossible( index );

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

    private <T extends PropertyContainer> void assertQueryNotPossible(
            Index<T> index )
    {
        try
        {
            index.query( "somekey:somevalue" );
            fail( "Querying shouldn't be possible" );
        }
        catch ( QueryNotPossibleException e )
        {
            // Good
        }
    }

    @Test( expected = QueryNotPossibleException.class )
    public void makeSureYouCantQueryModifiedIndexInTx()
    {
        Index<Node> index = provider.nodeIndex( "failing-index", LuceneIndexProvider.FULLTEXT_CONFIG );
        Node node = graphDb.createNode();
        index.add( node, "key", "value" );
        index.query( "key:value" );
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
        index.delete();
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
        Index<Node> index = provider.nodeIndex( "index", LuceneIndexProvider.EXACT_CONFIG );
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
    public void shouldNotFailQueryFromIndexInTx()
    {
        Index<Node> index = provider.nodeIndex( "indexFooBar", LuceneIndexProvider.EXACT_CONFIG );
        Node node = graphDb.createNode();
        index.add( node, "key", "value" );

        QueryContext queryContext = new QueryContext( "value" ).allowQueryingModifications();
        assertThat( index.query( "key", queryContext ), contains( node ) );
    }

    @Test
    public void makeSureArrayValuesAreSupported()
    {
        Index<Node> index = provider.nodeIndex( "index", LuceneIndexProvider.EXACT_CONFIG );
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
        Index<Node> index = provider.nodeIndex( "index", LuceneIndexProvider.EXACT_CONFIG );
        String key = "name";
        String value1 = "neo4j";
        String value2 = "nescafe";
        Node node1 = graphDb.createNode();
        Node node2 = graphDb.createNode();
        index.add( node1, key, value1 );
        index.add( node2, key, value2 );

        assertQueryNotPossible( index );

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
        Index<Node> index = provider.nodeIndex( "index", LuceneIndexProvider.EXACT_CONFIG );
        Node neo = graphDb.createNode();
        Node trinity = graphDb.createNode();
        index.add( neo, "username", "neo@matrix" );
        index.add( neo, "sex", "male" );
        index.add( trinity, "username", "trinity@matrix" );
        index.add( trinity, "sex", "female" );

        assertQueryNotPossible( index );

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

        assertQueryNotPossible( index );
//        assertCollection( index.query( name, "\"" + mattias + "\"" ), entity1 );
//        assertCollection( index.query( "name:\"" + mattias + "\"" ), entity1 );

        assertEquals( entity1, index.get( name, mattias ).getSingle() );

        assertQueryNotPossible( index );
//        assertCollection( index.query( "name", "Mattias*" ), entity1 );

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

        assertQueryNotPossible( index );
//        assertCollection( index.query( "name:\"" + mattias + "\" OR title:\"" +
//                hacker + "\"" ), entity1, entity2 );

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

        assertQueryNotPossible( index );
//        assertCollection( index.query( "name:\"" + mattias + "\" OR title:\"" +
//                hacker + "\"" ), entity1 );

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
        doSomeRandomUseCaseTestingWithExactIndex( provider.nodeIndex( "index",
                LuceneIndexProvider.EXACT_CONFIG ), NODE_CREATOR );
    }

    @Test
    public void doSomeRandomUseCaseTestingWithExactRelationshipIndex()
    {
        doSomeRandomUseCaseTestingWithExactIndex( provider.relationshipIndex( "index",
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

        assertQueryNotPossible( index );
//        assertCollection( index.query( key, "quick" ), entity1 );
//        assertCollection( index.query( key, "brown" ), entity1, entity2 );
//        assertCollection( index.query( key, "quick OR jumped" ), entity1, entity2 );
//        assertCollection( index.query( key, "brown AND fox" ), entity1, entity2 );

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
        doSomeRandomTestingWithFulltextIndex( provider.nodeIndex( "fulltext",
                LuceneIndexProvider.FULLTEXT_CONFIG ), NODE_CREATOR );
    }

    @Test
    public void doSomeRandomTestingWithRelationshipFulltextInde()
    {
        doSomeRandomTestingWithFulltextIndex( provider.relationshipIndex( "fulltext",
                LuceneIndexProvider.FULLTEXT_CONFIG ), RELATIONSHIP_CREATOR );
    }

    @Test
    public void testNodeLocalRelationshipIndex()
    {
        RelationshipIndex index = provider.relationshipIndex( "locality",
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
        Index<Node> index = provider.nodeIndex( "relevance", LuceneIndexProvider.EXACT_CONFIG );

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

    private <T extends PropertyContainer> void testInsertionSpeed(
            Index<T> index,
            EntityCreator<T> creator )
    {
        long t = System.currentTimeMillis();
        for ( int i = 0; i < 30000; i++ )
        {
            T entity = creator.create();
//            index.query( new TermQuery( new Term( "name", "The name " + i ) ) );
            index.get( "name", "The name " + i );
            index.add( entity, "name", "The name " + i );
            index.add( entity, "title", "Some title " + i );
            index.add( entity, "something", i + "Nothing" );
            index.add( entity, "else", i + "kdfjkdjf" + i );
            if ( i % 5000 == 0 )
            {
                restartTx();
//                System.out.print( "." );
            }
        }
        System.out.println( "insert:" + ( System.currentTimeMillis() - t ) );

        t = System.currentTimeMillis();
        int count = 100;
        for ( int i = 0; i < count; i++ )
        {
            for ( T entity : index.get( "name", "The name " + i ) )
            {
            }
        }
        System.out.println( "get:" + (double)( System.currentTimeMillis() - t ) / (double)count );
    }

    @Test
    public void testSorting()
    {
        Index<Node> index = provider.nodeIndex( "sort", LuceneIndexProvider.EXACT_CONFIG );
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

        assertQueryNotPossible( index );
//        assertOrderedCollection( index.query( new QueryContext( "name:*" ).sort( name, title ) ), adam2, adam, eva, jack );
//        assertOrderedCollection( index.query( new QueryContext( "name:*" ).sort( name, other ) ), adam, adam2, eva, jack );
//        assertOrderedCollection( index.query( new QueryContext( "name:*" ).sort( sex, title ) ), eva, jack, adam2, adam );

        restartTx();

        assertOrderedCollection( index.query( new QueryContext( "name:*" ).sort( name, title ) ), adam2, adam, eva, jack );
        assertOrderedCollection( index.query( new QueryContext( "name:*" ).sort( name, other ) ), adam, adam2, eva, jack );
        assertOrderedCollection( index.query( new QueryContext( "name:*" ).sort( sex, title ) ), eva, jack, adam2, adam );
    }

    @Test
    public void testNumericValues()
    {
        Index<Node> index = provider.nodeIndex( "numeric", LuceneIndexProvider.EXACT_CONFIG );

        Node node10 = graphDb.createNode();
        Node node6 = graphDb.createNode();
        Node node31 = graphDb.createNode();

        String key = "key";
        index.add( node10, key, numeric( 10 ) );
        index.add( node6, key, numeric( 6 ) );
        index.add( node31, key, numeric( 31 ) );

        assertQueryNotPossible( index );

        restartTx();
        assertThat( index.query( NumericRangeQuery.newIntRange( key, 4, 40, true, true ) ), contains( node10, node6, node31 ) );
        assertThat( index.query( NumericRangeQuery.newIntRange( key, 6, 15, true, true ) ), contains( node10, node6 ) );
        assertThat( index.query( NumericRangeQuery.newIntRange( key, 6, 15, false, true ) ), contains( node10 ) );
    }

    @Test
    public void testRemoveNumericValues()
    {
        Index<Node> index = provider.nodeIndex( "numeric2", LuceneIndexProvider.EXACT_CONFIG );
        Node node1 = graphDb.createNode();
        Node node2 = graphDb.createNode();
        String key = "key";
        index.add( node1, key, new ValueContext( 15 ).indexNumeric() );
        index.add( node2, key, new ValueContext( 5 ).indexNumeric() );
        index.remove( node1, key, new ValueContext( 15 ).indexNumeric() );

        assertQueryNotPossible( index );

        index.remove( node2, key, new ValueContext( 5 ).indexNumeric() );

        assertQueryNotPossible( index );

        restartTx();
        assertThat( index.query( NumericRangeQuery.newIntRange( key, 0, 20, false, false ) ), isEmpty() );

        index.add( node1, key, new ValueContext( 15 ).indexNumeric() );
        index.add( node2, key, new ValueContext( 5 ).indexNumeric() );
        restartTx();
        assertThat( index.query( NumericRangeQuery.newIntRange( key, 0, 20, false, false ) ), contains( node1, node2 ) );
        index.remove( node1, key, new ValueContext( 15 ).indexNumeric() );

        assertQueryNotPossible( index );

        restartTx();
        assertThat( index.query( NumericRangeQuery.newIntRange( key, 0, 20, false, false ) ), contains( node2 ) );
    }

    @Test
    public void testIndexNumberAsString()
    {
        Index<Node> index = provider.nodeIndex( "nums", LuceneIndexProvider.EXACT_CONFIG );
        Node node1 = graphDb.createNode();
        index.add( node1, "key", 10 );
        assertEquals( node1, index.get( "key", 10 ).getSingle() );
        assertEquals( node1, index.get( "key", "10" ).getSingle() );

        assertQueryNotPossible( index );
//        assertEquals( node1, index.query( "key", 10 ).getSingle() );
//        assertEquals( node1, index.query( "key", "10" ).getSingle() );

        restartTx();
        assertEquals( node1, index.get( "key", 10 ).getSingle() );
        assertEquals( node1, index.get( "key", "10" ).getSingle() );
        assertEquals( node1, index.query( "key", 10 ).getSingle() );
        assertEquals( node1, index.query( "key", "10" ).getSingle() );
    }

    @Ignore
    @Test
    public void testNodeInsertionSpeed()
    {
        testInsertionSpeed( provider.nodeIndex( "insertion-speed",
                LuceneIndexProvider.EXACT_CONFIG ), NODE_CREATOR );
    }

    @Ignore
    @Test
    public void testNodeFulltextInsertionSpeed()
    {
        testInsertionSpeed( provider.nodeIndex( "insertion-speed-full",
                LuceneIndexProvider.FULLTEXT_CONFIG ), NODE_CREATOR );
    }

    @Ignore
    @Test
    public void testRelationshipInsertionSpeed()
    {
        testInsertionSpeed( provider.relationshipIndex( "insertion-speed",
                LuceneIndexProvider.EXACT_CONFIG ), new FastRelationshipCreator() );
    }
}
