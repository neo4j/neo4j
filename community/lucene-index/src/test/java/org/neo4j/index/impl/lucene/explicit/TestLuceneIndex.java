/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.index.impl.lucene.explicit;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.QueryParser.Operator;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.search.SortedSetSortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.similarities.DefaultSimilarity;
import org.hamcrest.CoreMatchers;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntFunction;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.graphdb.index.RelationshipIndex;
import org.neo4j.graphdb.index.UniqueFactory;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.index.lucene.QueryContext;
import org.neo4j.index.lucene.ValueContext;
import org.neo4j.kernel.impl.MyRelTypes;
import org.neo4j.kernel.impl.index.IndexConfigStore;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import static org.apache.lucene.search.NumericRangeQuery.newIntRange;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.isOneOf;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.graphdb.RelationshipType.withName;
import static org.neo4j.helpers.collection.Iterators.asSet;
import static org.neo4j.helpers.collection.Iterators.count;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.index.Neo4jTestCase.assertContains;
import static org.neo4j.index.Neo4jTestCase.assertContainsInOrder;
import static org.neo4j.index.impl.lucene.explicit.LuceneIndexImplementation.EXACT_CONFIG;
import static org.neo4j.index.impl.lucene.explicit.LuceneIndexImplementation.FULLTEXT_CONFIG;
import static org.neo4j.index.lucene.QueryContext.numericRange;
import static org.neo4j.index.lucene.ValueContext.numeric;

public class TestLuceneIndex extends AbstractLuceneIndexTest
{
    @SuppressWarnings( "unchecked" )
    private <T extends PropertyContainer> void makeSureAdditionsCanBeRead(
            Index<T> index, EntityCreator<T> entityCreator )
    {
        String key = "name";
        String value = "Mattias";
        assertThat( index.get( key, value ).getSingle(), is( nullValue() ) );
        assertThat( index.get( key, value ), emptyIterable() );

        assertThat( index.query( key, "*" ), emptyIterable() );

        T entity1 = entityCreator.create();
        T entity2 = entityCreator.create();
        index.add( entity1, key, value );
        for ( int i = 0; i < 2; i++ )
        {
            assertThat( index.get( key, value ), Contains.contains( entity1 ) );
            assertThat( index.query( key, "*" ), Contains.contains( entity1 ) );
            assertThat( index.get( key, value ), Contains.contains( entity1 ) );

            restartTx();
        }

        index.add( entity2, key, value );
        assertThat( index.get( key, value ), Contains.contains( entity1, entity2 ) );

        restartTx();
        assertThat( index.get( key, value ), Contains.contains( entity1, entity2 ) );
        index.delete();
    }

    @Test
    public void queryIndexWithSortByNumeric()
    {
        Index<Node> index = nodeIndex();
        commitTx();

        String numericProperty = "NODE_ID";

        try ( Transaction transaction = graphDb.beginTx() )
        {
            for ( int i = 0; i < 15; i++ )
            {
                Node node = graphDb.createNode();
                node.setProperty( numericProperty, i );
                index.add( node, numericProperty, new ValueContext( i ).indexNumeric() );
            }
            transaction.success();
        }

        queryAndSortNodesByNumericProperty( index, numericProperty );
    }

    @Test
    public void queryIndexWithSortByString()
    {
        Index<Node> index = nodeIndex();
        commitTx();

        String stringProperty = "NODE_NAME";

        String[] names = new String[]{"Fry", "Leela", "Bender", "Amy", "Hubert", "Calculon"};
        try ( Transaction transaction = graphDb.beginTx() )
        {
            for ( String name : names )
            {
                Node node = graphDb.createNode();
                node.setProperty( stringProperty, name );
                index.add( node, stringProperty, name );
            }
            transaction.success();
        }

        String[] sortedNames = new String[]{"Leela", "Hubert", "Fry", "Calculon", "Bender", "Amy"};
        queryAndSortNodesByStringProperty(index, stringProperty, sortedNames );
    }

    @Test
    public void queryIndexWithSortByNumericAfterOtherPropertyUpdate()
    {
        Index<Node> index = nodeIndex();
        commitTx();

        String yearProperty = "year";
        String priceProperty = "price";

        Label nodeLabel = Label.label( "priceyNodes" );

        try ( Transaction transaction = graphDb.beginTx() )
        {
            for ( int i = 0; i < 15; i++ )
            {
                Node node = graphDb.createNode(nodeLabel);
                node.setProperty( yearProperty, i );
                node.setProperty( priceProperty, i );
                index.add( node, yearProperty, new ValueContext( i ).indexNumeric() );
                index.add( node, priceProperty, new ValueContext( i ).indexNumeric() );
            }
            transaction.success();
        }

        doubleNumericPropertyValueForAllNodesWithLabel( index, priceProperty, nodeLabel );

        queryAndSortNodesByNumericProperty( index, yearProperty );
        queryAndSortNodesByNumericProperty( index, priceProperty, value -> value * 2 );
    }

    @Test
    public void queryIndexWithSortByNumericAfterSamePropertyUpdate()
    {
        Index<Node> index = nodeIndex( stringMap() );
        commitTx();
        String numericProperty = "PRODUCT_ID";

        Label updatableIndexesProperty = Label.label( "updatableIndexes" );

        try ( Transaction transaction = graphDb.beginTx() )
        {
            for ( int i = 0; i < 15; i++ )
            {
                Node node = graphDb.createNode(updatableIndexesProperty);
                node.setProperty( numericProperty, i );
                index.add( node, numericProperty, new ValueContext( i ).indexNumeric() );
            }
            transaction.success();
        }

        doubleNumericPropertyValueForAllNodesWithLabel( index, numericProperty, updatableIndexesProperty );
        queryAndSortNodesByNumericProperty( index, numericProperty, i -> i * 2 );
    }

    @Test
    public void queryIndexWithSortByStringAfterOtherPropertyUpdate()
    {

        Index<Node> index = nodeIndex( stringMap() );
        commitTx();

        String nameProperty = "NODE_NAME";
        String jobNameProperty = "NODE_JOB_NAME";

        String[] names = new String[]{"Fry", "Leela", "Bender", "Amy", "Hubert", "Calculon"};
        String[] jobs = new String[]{"delivery boy", "pilot", "gambler", "intern", "professor", "actor"};
        Label characters = Label.label( "characters" );
        setPropertiesAndUpdateToJunior( index, nameProperty, jobNameProperty, names, jobs, characters );

        String[] sortedNames = new String[]{"Leela", "Hubert", "Fry", "Calculon", "Bender", "Amy"};
        String[] sortedJobs = {"junior professor", "junior pilot", "junior intern", "junior gambler",
                "junior delivery boy", "junior actor"};
        queryAndSortNodesByStringProperty( index, nameProperty, sortedNames );
        queryAndSortNodesByStringProperty( index, jobNameProperty, sortedJobs );
    }

    @Test
    public void queryCustomIndexWithSortByStringAfterOtherPropertyUpdate()
    {

        Index<Node> index = nodeIndex( MapUtil.stringMap( LuceneIndexImplementation.KEY_TYPE, "exact",
                LuceneIndexImplementation.KEY_TO_LOWER_CASE, "true" ) );
        commitTx();

        String nameProperty = "NODE_NAME_CUSTOM";
        String jobNameProperty = "NODE_JOB_NAME_CUSTOM";

        String[] names = new String[]{"Fry", "Leela", "Bender", "Amy", "Hubert", "Calculon"};
        String[] jobs = new String[]{"delivery boy", "pilot", "gambler", "intern", "professor", "actor"};
        Label characters = Label.label( "characters_custom" );
        setPropertiesAndUpdateToJunior( index, nameProperty, jobNameProperty, names, jobs, characters );

        String[] sortedNames = new String[]{"Leela", "Hubert", "Fry", "Calculon", "Bender", "Amy"};
        String[] sortedJobs = {"junior professor", "junior pilot", "junior intern", "junior gambler",
                "junior delivery boy", "junior actor"};
        queryAndSortNodesByStringProperty( index, nameProperty, sortedNames );
        queryAndSortNodesByStringProperty( index, jobNameProperty, sortedJobs );
    }

    @Test
    public void queryIndexWithSortByStringAfterSamePropertyUpdate()
    {
        Index<Node> index = nodeIndex( stringMap() );
        commitTx();

        String nameProperty = "NODE_NAME";
        Label heroes = Label.label( "heroes" );

        String[] names = new String[]{"Fry", "Leela", "Bender", "Amy", "Hubert", "Calculon"};
        try ( Transaction transaction = graphDb.beginTx() )
        {
            for ( String name : names )
            {
                Node node = graphDb.createNode(heroes);
                node.setProperty( nameProperty, name );
                index.add( node, nameProperty, name );
            }
            transaction.success();
        }

        try ( Transaction transaction = graphDb.beginTx() )
        {
            ResourceIterator<Node> nodes = graphDb.findNodes( heroes );
            nodes.stream().forEach( node ->
            {
                node.setProperty( nameProperty, "junior " + node.getProperty( nameProperty )  );
                index.remove( node, nameProperty );
                index.add( node, nameProperty,  node.getProperty( nameProperty ));
            } );
            transaction.success();
        }

        String[] sortedNames = new String[]{"junior Leela", "junior Hubert", "junior Fry", "junior Calculon",
                "junior Bender", "junior Amy"};
        queryAndSortNodesByStringProperty( index, nameProperty, sortedNames );
    }

    @Test
    public void makeSureYouGetLatestTxModificationsInQueryByDefault()
    {
        Index<Node> index = nodeIndex( LuceneIndexImplementation.FULLTEXT_CONFIG );
        Node node = graphDb.createNode();
        index.add( node, "key", "value" );
        assertThat( index.query( "key:value" ), Contains.contains( node ) );
    }

    @Test
    public void makeSureLuceneIndexesReportAsWriteable()
    {
        Index<Node> index = nodeIndex( LuceneIndexImplementation.FULLTEXT_CONFIG );
        Node node = graphDb.createNode();
        index.add( node, "key", "value" );
        assertTrue( index.isWriteable() );
    }

    @Test
    public void makeSureAdditionsCanBeReadNodeExact()
    {
        makeSureAdditionsCanBeRead( nodeIndex( LuceneIndexImplementation.EXACT_CONFIG ),
                NODE_CREATOR );
    }

    @Test
    public void makeSureAdditionsCanBeReadNodeFulltext()
    {
        makeSureAdditionsCanBeRead( nodeIndex(
                LuceneIndexImplementation.FULLTEXT_CONFIG ), NODE_CREATOR );
    }

    @Test
    public void makeSureAdditionsCanBeReadRelationshipExact()
    {
        makeSureAdditionsCanBeRead( relationshipIndex(
                LuceneIndexImplementation.EXACT_CONFIG ), RELATIONSHIP_CREATOR );
    }

    @Test
    public void makeSureAdditionsCanBeReadRelationshipFulltext()
    {
        makeSureAdditionsCanBeRead( relationshipIndex(
                LuceneIndexImplementation.FULLTEXT_CONFIG ), RELATIONSHIP_CREATOR );
    }

    @Test
    public void makeSureAdditionsCanBeRemovedInSameTx()
    {
        makeSureAdditionsCanBeRemoved( false );
    }

    @Test
    public void removingAnIndexedNodeWillAlsoRemoveItFromTheIndex()
    {
        Index<Node> index = nodeIndex( LuceneIndexImplementation.EXACT_CONFIG );
        Node node = graphDb.createNode();
        node.setProperty( "poke", 1 );
        index.add( node, "key", "value" );
        commitTx();

        beginTx();
        node.delete();
        commitTx();

        beginTx();
        IndexHits<Node> nodes = index.get( "key", "value" );
        // IndexHits.size is allowed to be inaccurate in this case:
        assertThat( nodes.size(), isOneOf( 0, 1 ) );
        for ( Node n : nodes )
        {
            n.getProperty( "poke" );
            fail( "Found node " + n );
        }
        commitTx();

        beginTx();
        IndexHits<Node> nodesAgain = index.get( "key", "value" );
        // After a read, the index should be repaired:
        assertThat( nodesAgain.size(), is( 0 ) );
        for ( Node n : nodesAgain )
        {
            n.getProperty( "poke" );
            fail( "Found node " + n );
        }
    }

    @Test
    public void removingAnIndexedRelationshipWillAlsoRemoveItFromTheIndex()
    {
        Index<Relationship> index = relationshipIndex( LuceneIndexImplementation.EXACT_CONFIG );
        Node a = graphDb.createNode();
        Node b = graphDb.createNode();
        Relationship rel = a.createRelationshipTo( b, withName( "REL" ) );
        rel.setProperty( "poke", 1 );
        index.add( rel, "key", "value" );
        commitTx();

        beginTx();
        rel.delete();
        commitTx();

        beginTx();
        IndexHits<Relationship> rels = index.get( "key", "value" );
        // IndexHits.size is allowed to be inaccurate in this case:
        assertThat( rels.size(), isOneOf( 0, 1 ) );
        for ( Relationship r : rels )
        {
            r.getProperty( "poke" );
            fail( "Found relationship " + r );
        }
        commitTx();

        beginTx();
        IndexHits<Relationship> relsAgain = index.get( "key", "value" );
        // After a read, the index should be repaired:
        assertThat( relsAgain.size(), is( 0 ) );
        for ( Relationship r : relsAgain )
        {
            r.getProperty( "poke" );
            fail( "Found relationship " + r );
        }
    }

    @Test
    public void makeSureYouCanAskIfAnIndexExistsOrNot()
    {
        String name = currentIndexName();
        assertFalse( graphDb.index().existsForNodes( name ) );
        graphDb.index().forNodes( name );
        assertTrue( graphDb.index().existsForNodes( name ) );

        assertFalse( graphDb.index().existsForRelationships( name ) );
        graphDb.index().forRelationships( name );
        assertTrue( graphDb.index().existsForRelationships( name ) );
    }

    private void makeSureAdditionsCanBeRemoved( boolean restartTx )
    {
        Index<Node> index = nodeIndex( LuceneIndexImplementation.EXACT_CONFIG );
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
        Index<Node> index = nodeIndex( LuceneIndexImplementation.EXACT_CONFIG );
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
        Index<Node> index = nodeIndex( LuceneIndexImplementation.EXACT_CONFIG );
        String key = "name";
        String value1 = "Lucene";
        String value2 = "Index";
        String value3 = "Rules";
        assertThat( index.query( key, "*" ), emptyIterable() );
        Node node = graphDb.createNode();
        index.add( node, key, value1 );
        index.add( node, key, value2 );
        if ( restartTx )
        {
            restartTx();
        }
        index.add( node, key, value3 );
        for ( int i = 0; i < 2; i++ )
        {
            assertThat( index.get( key, value1 ), Contains.contains( node ) );
            assertThat( index.get( key, value2 ), Contains.contains( node ) );
            assertThat( index.get( key, value3 ), Contains.contains( node ) );
            assertThat( index.get( key, "whatever" ), emptyIterable() );
            restartTx();
        }
        index.delete();
    }

    @Test
    public void indexHitsFromQueryingRemovedDoesNotReturnNegativeCount()
    {
        Index<Node> index = nodeIndex( LuceneIndexImplementation.EXACT_CONFIG );
        Node theNode = graphDb.createNode();
        index.remove( theNode );
        try ( IndexHits<Node> hits = index.query( "someRandomKey", theNode.getId() ) )
        {
            assertTrue( hits.size() >= 0 );
        }
    }

    @Test
    public void shouldNotGetLatestTxModificationsWhenChoosingSpeedQueries()
    {
        Index<Node> index = nodeIndex( LuceneIndexImplementation.EXACT_CONFIG );
        Node node = graphDb.createNode();
        index.add( node, "key", "value" );
        QueryContext queryContext = new QueryContext( "value" ).tradeCorrectnessForSpeed();
        assertThat( index.query( "key", queryContext ), emptyIterable() );
        assertThat( index.query( "key", "value" ), Contains.contains( node ) );
    }

    @Test
    public void makeSureArrayValuesAreSupported()
    {
        Index<Node> index = nodeIndex( LuceneIndexImplementation.EXACT_CONFIG );
        String key = "name";
        String value1 = "Lucene";
        String value2 = "Index";
        String value3 = "Rules";
        assertThat( index.query( key, "*" ), emptyIterable() );
        Node node = graphDb.createNode();
        index.add( node, key, new String[]{value1, value2, value3} );
        for ( int i = 0; i < 2; i++ )
        {
            assertThat( index.get( key, value1 ), Contains.contains( node ) );
            assertThat( index.get( key, value2 ), Contains.contains( node ) );
            assertThat( index.get( key, value3 ), Contains.contains( node ) );
            assertThat( index.get( key, "whatever" ), emptyIterable() );
            restartTx();
        }

        index.remove( node, key, new String[]{value2, value3} );

        for ( int i = 0; i < 2; i++ )
        {
            assertThat( index.get( key, value1 ), Contains.contains( node ) );
            assertThat( index.get( key, value2 ), emptyIterable() );
            assertThat( index.get( key, value3 ), emptyIterable() );
            restartTx();
        }
        index.delete();
    }

    @Test
    public void makeSureWildcardQueriesCanBeAsked()
    {
        Index<Node> index = nodeIndex( LuceneIndexImplementation.EXACT_CONFIG );
        String key = "name";
        String value1 = "neo4j";
        String value2 = "nescafe";
        Node node1 = graphDb.createNode();
        Node node2 = graphDb.createNode();
        index.add( node1, key, value1 );
        index.add( node2, key, value2 );

        for ( int i = 0; i < 2; i++ )
        {
            assertThat( index.query( key, "neo*" ), Contains.contains( node1 ) );
            assertThat( index.query( key, "n?o4j" ), Contains.contains( node1 ) );
            assertThat( index.query( key, "ne*" ), Contains.contains( node1, node2 ) );
            assertThat( index.query( key + ":neo4j" ), Contains.contains( node1 ) );
            assertThat( index.query( key + ":neo*" ), Contains.contains( node1 ) );
            assertThat( index.query( key + ":n?o4j" ), Contains.contains( node1 ) );
            assertThat( index.query( key + ":ne*" ), Contains.contains( node1, node2 ) );

            restartTx();
        }
        index.delete();
    }

    @Test
    public void makeSureCompositeQueriesCanBeAsked()
    {
        Index<Node> index = nodeIndex( LuceneIndexImplementation.EXACT_CONFIG );
        Node neo = graphDb.createNode();
        Node trinity = graphDb.createNode();
        index.add( neo, "username", "neo@matrix" );
        index.add( neo, "sex", "male" );
        index.add( trinity, "username", "trinity@matrix" );
        index.add( trinity, "sex", "female" );

        for ( int i = 0; i < 2; i++ )
        {
            assertThat( index.query( "username:*@matrix AND sex:male" ), Contains.contains( neo ) );
            assertThat( index.query( new QueryContext( "username:*@matrix sex:male" ).defaultOperator( Operator.AND ) ), Contains

                    .contains( neo ) );
            assertThat( index.query( "username:*@matrix OR sex:male" ), Contains.contains( neo, trinity ) );
            assertThat( index.query( new QueryContext( "username:*@matrix sex:male" ).defaultOperator( Operator.OR ) ), Contains
                    .contains( neo, trinity ) );

            restartTx();
        }
        index.delete();
    }

    @SuppressWarnings( "unchecked" )
    private <T extends PropertyContainer> void doSomeRandomUseCaseTestingWithExactIndex(
            Index<T> index, EntityCreator<T> creator )
    {
        String name = "name";
        String mattias = "Mattias Persson";
        String title = "title";
        String hacker = "Hacker";

        assertThat( index.get( name, mattias ), emptyIterable() );

        T entity1 = creator.create();
        T entity2 = creator.create();

        assertNull( index.get( name, mattias ).getSingle() );
        index.add( entity1, name, mattias );
        assertThat( index.get( name, mattias ), Contains.contains( entity1 ) );

        assertContains( index.query( name, "\"" + mattias + "\"" ), entity1 );
        assertContains( index.query( "name:\"" + mattias + "\"" ), entity1 );

        assertEquals( entity1, index.get( name, mattias ).getSingle() );

        assertContains( index.query( "name", "Mattias*" ), entity1 );

        commitTx();

        beginTx();
        assertThat( index.get( name, mattias ), Contains.contains( entity1 ) );
        assertThat( index.query( name, "\"" + mattias + "\"" ), Contains.contains( entity1 ) );
        assertThat( index.query( "name:\"" + mattias + "\"" ), Contains.contains( entity1 ) );
        assertEquals( entity1, index.get( name, mattias ).getSingle() );
        assertThat( index.query( "name", "Mattias*" ), Contains.contains( entity1 ) );
        commitTx();

        beginTx();
        index.add( entity2, title, hacker );
        index.add( entity1, title, hacker );
        assertThat( index.get( name, mattias ), Contains.contains( entity1 ) );
        assertThat( index.get( title, hacker ), Contains.contains( entity1, entity2 ) );

        assertContains( index.query( "name:\"" + mattias + "\" OR title:\"" +
                hacker + "\"" ), entity1, entity2 );

        commitTx();

        beginTx();
        assertThat( index.get( name, mattias ), Contains.contains( entity1 ) );
        assertThat( index.get( title, hacker ), Contains.contains( entity1, entity2 ) );
        assertThat( index.query( "name:\"" + mattias + "\" OR title:\"" + hacker + "\"" ), Contains
                .contains( entity1, entity2 ) );
        assertThat( index.query( "name:\"" + mattias + "\" AND title:\"" +
                hacker + "\"" ), Contains.contains( entity1 ) );
        commitTx();

        beginTx();
        index.remove( entity2, title, hacker );
        assertThat( index.get( name, mattias ), Contains.contains( entity1 ) );
        assertThat( index.get( title, hacker ), Contains.contains( entity1 ) );

        assertContains( index.query( "name:\"" + mattias + "\" OR title:\"" +
                hacker + "\"" ), entity1 );

        commitTx();

        beginTx();
        assertThat( index.get( name, mattias ), Contains.contains( entity1 ) );
        assertThat( index.get( title, hacker ), Contains.contains( entity1 ) );
        assertThat( index.query( "name:\"" + mattias + "\" OR title:\"" +
                hacker + "\"" ), Contains.contains( entity1 ) );
        commitTx();

        beginTx();
        index.remove( entity1, title, hacker );
        index.remove( entity1, name, mattias );
        index.delete();
        commitTx();
    }

    @Test
    public void doSomeRandomUseCaseTestingWithExactNodeIndex()
    {
        doSomeRandomUseCaseTestingWithExactIndex( nodeIndex(
                LuceneIndexImplementation.EXACT_CONFIG ), NODE_CREATOR );
    }

    @Test
    public void doSomeRandomUseCaseTestingWithExactRelationshipIndex()
    {
        doSomeRandomUseCaseTestingWithExactIndex( relationshipIndex(
                LuceneIndexImplementation.EXACT_CONFIG ), RELATIONSHIP_CREATOR );
    }

    @SuppressWarnings( "unchecked" )
    private <T extends PropertyContainer> void doSomeRandomTestingWithFulltextIndex(
            Index<T> index,
            EntityCreator<T> creator )
    {
        T entity1 = creator.create();
        T entity2 = creator.create();

        String key = "name";
        index.add( entity1, key, "The quick brown fox" );
        index.add( entity2, key, "brown fox jumped over" );

        for ( int i = 0; i < 2; i++ )
        {
            assertThat( index.get( key, "The quick brown fox" ), Contains.contains( entity1 ) );
            assertThat( index.get( key, "brown fox jumped over" ), Contains.contains( entity2 ) );
            assertThat( index.query( key, "quick" ), Contains.contains( entity1 ) );
            assertThat( index.query( key, "brown" ), Contains.contains( entity1, entity2 ) );
            assertThat( index.query( key, "quick OR jumped" ), Contains.contains( entity1, entity2 ) );
            assertThat( index.query( key, "brown AND fox" ), Contains.contains( entity1, entity2 ) );

            restartTx();
        }

        index.delete();
    }

    @Test
    public void doSomeRandomTestingWithNodeFulltextInde()
    {
        doSomeRandomTestingWithFulltextIndex( nodeIndex(
                LuceneIndexImplementation.FULLTEXT_CONFIG ), NODE_CREATOR );
    }

    @Test
    public void doSomeRandomTestingWithRelationshipFulltextInde()
    {
        doSomeRandomTestingWithFulltextIndex( relationshipIndex(
                LuceneIndexImplementation.FULLTEXT_CONFIG ), RELATIONSHIP_CREATOR );
    }

    @Test
    public void testNodeLocalRelationshipIndex()
    {
        RelationshipIndex index = relationshipIndex(
                LuceneIndexImplementation.EXACT_CONFIG );

        RelationshipType type = withName( "YO" );
        Node startNode = graphDb.createNode();
        Node endNode1 = graphDb.createNode();
        Node endNode2 = graphDb.createNode();
        Relationship rel1 = startNode.createRelationshipTo( endNode1, type );
        Relationship rel2 = startNode.createRelationshipTo( endNode2, type );
        index.add( rel1, "name", "something" );
        index.add( rel2, "name", "something" );

        for ( int i = 0; i < 2; i++ )
        {
            assertThat( index.query( "name:something" ), Contains.contains( rel1, rel2 ) );
            assertThat( index.query( "name:something", null, endNode1 ), Contains.contains( rel1 ) );
            assertThat( index.query( "name:something", startNode, endNode2 ), Contains.contains( rel2 ) );
            assertThat( index.query( null, startNode, endNode1 ), Contains.contains( rel1 ) );
            assertThat( index.get( "name", "something", null, endNode1 ), Contains.contains( rel1 ) );
            assertThat( index.get( "name", "something", startNode, endNode2 ), Contains.contains( rel2 ) );
            assertThat( index.get( null, null, startNode, endNode1 ), Contains.contains( rel1 ) );

            restartTx();
        }

        rel2.delete();
        rel1.delete();
        startNode.delete();
        endNode1.delete();
        endNode2.delete();
        index.delete();
    }

    @Test
    public void testSortByRelevance()
    {
        Index<Node> index = nodeIndex( LuceneIndexImplementation.EXACT_CONFIG );

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

        IndexHits<Node> hits = index.query(
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
        Index<Node> index = nodeIndex( LuceneIndexImplementation.EXACT_CONFIG );
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

        for ( int i = 0; i < 2; i++ )
        {
            assertContainsInOrder( index.query( new QueryContext( "name:*" ).sort( name, title ) ), adam2, adam, eva, jack );
            assertContainsInOrder( index.query( new QueryContext( "name:*" ).sort( name, other ) ), adam, adam2, eva, jack );
            assertContainsInOrder( index.query( new QueryContext( "name:*" ).sort( sex, title ) ), eva, jack, adam2, adam );
            assertContainsInOrder( index.query( name, new QueryContext( "*" ).sort( sex, title ) ), eva, jack, adam2, adam );
            assertContainsInOrder( index.query( new QueryContext( "name:*" ).sort( name, title ).top( 2 ) ), adam2, adam );

            restartTx();
        }
    }

    @Test
    public void testNumericValuesExactIndex()
    {
        testNumericValues( nodeIndex( LuceneIndexImplementation.EXACT_CONFIG ) );
    }

    @Test
    public void testNumericValuesFulltextIndex()
    {
        testNumericValues( nodeIndex( LuceneIndexImplementation.FULLTEXT_CONFIG ) );
    }

    private void testNumericValues( Index<Node> index )
    {
        Node node10 = graphDb.createNode();
        Node node6 = graphDb.createNode();
        Node node31 = graphDb.createNode();

        String key = "key";
        index.add( node10, key, numeric( 10 ) );
        index.add( node6, key, numeric( 6 ) );
        index.add( node31, key, numeric( 31 ) );

        for ( int i = 0; i < 2; i++ )
        {
            assertThat( index.query( NumericRangeQuery.newIntRange( key, 4, 40, true, true ) ), Contains
                    .contains( node10, node6, node31 ) );
            assertThat( index.query( NumericRangeQuery.newIntRange( key, 6, 15, true, true ) ), Contains
                    .contains( node10, node6 ) );
            assertThat( index.query( NumericRangeQuery.newIntRange( key, 6, 15, false, true ) ), Contains.contains( node10 ) );
            restartTx();
        }

        index.remove( node6, key, numeric( 6 ) );
        assertThat( index.query( NumericRangeQuery.newIntRange( key, 4, 40, true, true ) ), Contains
                .contains( node10, node31 ) );
        restartTx();
        assertThat( index.query( NumericRangeQuery.newIntRange( key, 4, 40, true, true ) ), Contains
                .contains( node10, node31 ) );
    }

    @Test
    public void testNumericValueArrays()
    {
        Index<Node> index = nodeIndex( LuceneIndexImplementation.EXACT_CONFIG );

        Node node1 = graphDb.createNode();
        index.add( node1, "number", new ValueContext[]{ numeric( 45 ), numeric( 98 ) } );
        Node node2 = graphDb.createNode();
        index.add( node2, "number", new ValueContext[]{ numeric( 47 ), numeric( 100 ) } );

        IndexHits<Node> indexResult1 = index.query( "number", newIntRange( "number", 47, 98, true, true ) );
        assertThat( indexResult1, Contains.contains( node1, node2 ) );
        assertThat( indexResult1.size(), is( 2 ));

        IndexHits<Node> indexResult2 = index.query( "number", newIntRange( "number", 44, 46, true, true ) );
        assertThat( indexResult2, Contains.contains( node1 ) );
        assertThat( indexResult2.size(), is( 1 ) );

        IndexHits<Node> indexResult3 = index.query( "number", newIntRange( "number", 99, 101, true, true ) );
        assertThat( indexResult3, Contains.contains( node2 ) );
        assertThat( indexResult3.size(), is( 1 ) );

        IndexHits<Node> indexResult4 = index.query( "number", newIntRange( "number", 47, 98, false, false ) );
        assertThat( indexResult4, emptyIterable() );

        IndexHits<Node> indexResult5 = index.query( "number", numericRange( "number", null, 98, true, true ) );
        assertContains( indexResult5, node1, node2 );

        IndexHits<Node> indexResult6 = index.query( "number", numericRange( "number", 47, null, true, true ) );
        assertContains( indexResult6, node1, node2 );
    }

    @Test
    public void testRemoveNumericValues()
    {
        Index<Node> index = nodeIndex( LuceneIndexImplementation.EXACT_CONFIG );
        Node node1 = graphDb.createNode();
        Node node2 = graphDb.createNode();
        String key = "key";
        index.add( node1, key, new ValueContext( 15 ).indexNumeric() );
        index.add( node2, key, new ValueContext( 5 ).indexNumeric() );
        index.remove( node1, key, new ValueContext( 15 ).indexNumeric() );

        assertThat( index.query( NumericRangeQuery.newIntRange( key, 0, 20, false, false ) ), Contains.contains( node2 ) );

        index.remove( node2, key, new ValueContext( 5 ).indexNumeric() );

        assertThat( index.query( NumericRangeQuery.newIntRange( key, 0, 20, false, false ) ), emptyIterable() );

        restartTx();
        assertThat( index.query( NumericRangeQuery.newIntRange( key, 0, 20, false, false ) ), emptyIterable() );

        index.add( node1, key, new ValueContext( 15 ).indexNumeric() );
        index.add( node2, key, new ValueContext( 5 ).indexNumeric() );
        restartTx();
        assertThat( index.query( NumericRangeQuery.newIntRange( key, 0, 20, false, false ) ), Contains
                .contains( node1, node2 ) );
        index.remove( node1, key, new ValueContext( 15 ).indexNumeric() );

        assertThat( index.query( NumericRangeQuery.newIntRange( key, 0, 20, false, false ) ), Contains.contains( node2 ) );

        restartTx();
        assertThat( index.query( NumericRangeQuery.newIntRange( key, 0, 20, false, false ) ), Contains.contains( node2 ) );
    }

    @Test
    public void sortNumericValues()
    {
        Index<Node> index = nodeIndex( LuceneIndexImplementation.EXACT_CONFIG );
        Node node1 = graphDb.createNode();
        Node node2 = graphDb.createNode();
        Node node3 = graphDb.createNode();
        String key = "key";
        index.add( node1, key, numeric( 5 ) );
        index.add( node2, key, numeric( 15 ) );
        index.add( node3, key, numeric( 10 ) );
        restartTx();

        assertContainsInOrder( index.query( numericRange( key, 5, 15 ).sortNumeric( key, false ) ), node1, node3, node2 );
    }

    @Test
    public void testIndexNumberAsString()
    {
        Index<Node> index = nodeIndex( LuceneIndexImplementation.EXACT_CONFIG );
        Node node1 = graphDb.createNode();
        index.add( node1, "key", 10 );

        for ( int i = 0; i < 2; i++ )
        {
            assertEquals( node1, index.get( "key", 10 ).getSingle() );
            assertEquals( node1, index.get( "key", "10" ).getSingle() );
            assertEquals( node1, index.query( "key", 10 ).getSingle() );
            assertEquals( node1, index.query( "key", "10" ).getSingle() );
            restartTx();
        }
    }

    @Test( expected = IllegalArgumentException.class )
    public void makeSureIndexGetsCreatedImmediately()
    {
        // Since index creation is done outside of the normal transactions,
        // a rollback will not roll back index creation.

        nodeIndex( LuceneIndexImplementation.FULLTEXT_CONFIG );
        assertTrue( graphDb.index().existsForNodes( currentIndexName() ) );
        rollbackTx();
        beginTx();
        assertTrue( graphDb.index().existsForNodes( currentIndexName() ) );
        nodeIndex( LuceneIndexImplementation.EXACT_CONFIG );
        rollbackTx();
    }

    @Test
    public void makeSureFulltextConfigIsCaseInsensitiveByDefault()
    {
        Index<Node> index = nodeIndex( LuceneIndexImplementation.FULLTEXT_CONFIG );
        Node node = graphDb.createNode();
        String key = "name";
        String value = "Mattias Persson";
        index.add( node, key, value );
        for ( int i = 0; i < 2; i++ )
        {
            assertThat( index.query( "name", "[A TO Z]" ), Contains.contains( node ) );
            assertThat( index.query( "name", "[a TO z]" ), Contains.contains( node ) );
            assertThat( index.query( "name", "Mattias" ), Contains.contains( node ) );
            assertThat( index.query( "name", "mattias" ), Contains.contains( node ) );
            assertThat( index.query( "name", "Matt*" ), Contains.contains( node ) );
            assertThat( index.query( "name", "matt*" ), Contains.contains( node ) );
            restartTx();
        }
    }

    @Test
    public void makeSureFulltextIndexCanBeCaseSensitive()
    {
        Index<Node> index = nodeIndex( MapUtil.stringMap(
                new HashMap<>( LuceneIndexImplementation.FULLTEXT_CONFIG ),
                "to_lower_case", "false" ) );
        Node node = graphDb.createNode();
        String key = "name";
        String value = "Mattias Persson";
        index.add( node, key, value );
        for ( int i = 0; i < 2; i++ )
        {
            assertThat( index.query( "name", "[A TO Z]" ), Contains.contains( node ) );
            assertThat( index.query( "name", "[a TO z]" ), emptyIterable() );
            assertThat( index.query( "name", "Matt*" ), Contains.contains( node ) );
            assertThat( index.query( "name", "matt*" ), emptyIterable() );
            assertThat( index.query( "name", "Persson" ), Contains.contains( node ) );
            assertThat( index.query( "name", "persson" ), emptyIterable() );
            restartTx();
        }
    }

    @Test
    public void makeSureCustomAnalyzerCanBeUsed()
    {
        CustomAnalyzer.called = false;
        Index<Node> index = nodeIndex( MapUtil.stringMap(
                IndexManager.PROVIDER, "lucene", "analyzer", CustomAnalyzer.class.getName(),
                "to_lower_case", "true" ) );
        Node node = graphDb.createNode();
        String key = "name";
        String value = "The value";
        index.add( node, key, value );
        restartTx();
        assertTrue( CustomAnalyzer.called );
        assertThat( index.query( key, "[A TO Z]" ), Contains.contains( node ) );
    }

    @Test
    public void makeSureCustomAnalyzerCanBeUsed2()
    {
        CustomAnalyzer.called = false;
        Index<Node> index = nodeIndex( "w-custom-analyzer-2", MapUtil.stringMap(
                IndexManager.PROVIDER, "lucene", "analyzer", CustomAnalyzer.class.getName(),
                "to_lower_case", "true", "type", "fulltext" ) );
        Node node = graphDb.createNode();
        String key = "name";
        String value = "The value";
        index.add( node, key, value );
        restartTx();
        assertTrue( CustomAnalyzer.called );
        assertThat( index.query( key, "[A TO Z]" ), Contains.contains( node ) );
    }

    @Test
    public void makeSureIndexNameAndConfigCanBeReachedFromIndex()
    {
        String indexName = "my-index-1";
        Index<Node> nodeIndex = nodeIndex( indexName, LuceneIndexImplementation.EXACT_CONFIG );
        assertEquals( indexName, nodeIndex.getName() );
        assertEquals( LuceneIndexImplementation.EXACT_CONFIG, graphDb.index().getConfiguration( nodeIndex ) );

        String indexName2 = "my-index-2";
        Index<Relationship> relIndex = relationshipIndex( indexName2, LuceneIndexImplementation.FULLTEXT_CONFIG );
        assertEquals( indexName2, relIndex.getName() );
        assertEquals( LuceneIndexImplementation.FULLTEXT_CONFIG, graphDb.index().getConfiguration( relIndex ) );
    }

    @Test
    public void testStringQueryVsQueryObject()
    {
        Index<Node> index = nodeIndex( LuceneIndexImplementation.FULLTEXT_CONFIG );
        Node node = graphDb.createNode();
        index.add( node, "name", "Mattias Persson" );
        for ( int i = 0; i < 2; i++ )
        {
            assertContains( index.query( "name:Mattias AND name:Per*" ), node );
            assertContains( index.query( "name:mattias" ), node );
            assertContains( index.query( new TermQuery( new Term( "name", "mattias" ) ) ), node );
            restartTx();
        }
        assertNull( index.query( new TermQuery( new Term( "name", "Mattias" ) ) ).getSingle() );
    }

    @SuppressWarnings( "unchecked" )
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

        Iterators.count( index.get( key, value ) );
        rollbackTx();
        beginTx();

        Iterators.count( index.get( key, value ) );
        index.add( c, "something", "whatever" );
        restartTx();

        Iterators.count( index.get( key, value ) );
    }

    @Test
    public void testAbandonedNodeIds()
    {
        testAbandonedIds( NODE_CREATOR, nodeIndex( LuceneIndexImplementation.EXACT_CONFIG ) );
    }

    @Test
    public void testAbandonedNodeIdsFulltext()
    {
        testAbandonedIds( NODE_CREATOR, nodeIndex( LuceneIndexImplementation.FULLTEXT_CONFIG ) );
    }

    @Test
    public void testAbandonedRelIds()
    {
        testAbandonedIds( RELATIONSHIP_CREATOR, relationshipIndex( LuceneIndexImplementation.EXACT_CONFIG ) );
    }

    @Test
    public void testAbandonedRelIdsFulltext()
    {
        testAbandonedIds( RELATIONSHIP_CREATOR, relationshipIndex( LuceneIndexImplementation.FULLTEXT_CONFIG ) );
    }

    @Test
    public void makeSureYouCanRemoveFromRelationshipIndex()
    {
        Node n1 = graphDb.createNode();
        Node n2 = graphDb.createNode();
        Relationship r = n1.createRelationshipTo( n2, withName( "foo" ) );
        RelationshipIndex index = graphDb.index().forRelationships( "rel-index" );
        String key = "bar";
        index.remove( r, key, "value" );
        index.add( r, key, "otherValue" );
        for ( int i = 0; i < 2; i++ )
        {
            assertThat( index.get( key, "value" ), emptyIterable() );
            assertThat( index.get( key, "otherValue" ), Contains.contains( r ) );
            restartTx();
        }
    }

    @Test
    public void makeSureYouCanGetEntityTypeFromIndex()
    {
        Index<Node> nodeIndex = nodeIndex( MapUtil.stringMap( IndexManager.PROVIDER, "lucene", "type", "exact" ) );
        Index<Relationship> relIndex = relationshipIndex( MapUtil.stringMap( IndexManager.PROVIDER, "lucene", "type", "exact" ) );
        assertEquals( Node.class, nodeIndex.getEntityType() );
        assertEquals( Relationship.class, relIndex.getEntityType() );
    }

    @Test
    public void makeSureConfigurationCanBeModified()
    {
        Index<Node> index = nodeIndex( LuceneIndexImplementation.EXACT_CONFIG );
        try
        {
            graphDb.index().setConfiguration( index, IndexManager.PROVIDER, "something" );
            fail( "Shouldn't be able to modify provider" );
        }
        catch ( IllegalArgumentException e )
        { /* Good*/ }
        try
        {
            graphDb.index().removeConfiguration( index, IndexManager.PROVIDER );
            fail( "Shouldn't be able to modify provider" );
        }
        catch ( IllegalArgumentException e )
        { /* Good*/ }

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
        Map<String, String> config = MapUtil.stringMap( IndexManager.PROVIDER, "lucene", "type", "fulltext" );
        String name = currentIndexName();
        nodeIndex( name, config );
        nodeIndex( name, MapUtil.stringMap( new HashMap<>( config ), "to_lower_case", "true" ) );
        try
        {
            nodeIndex( name, MapUtil.stringMap( new HashMap<>( config ), "to_lower_case", "false" ) );
            fail( "Shouldn't be able to get index with these kinds of differences in config" );
        }
        catch ( IllegalArgumentException e )
        { /* */ }
        nodeIndex( name, MapUtil.stringMap( new HashMap<>( config ), "whatever", "something" ) );
    }

    @Test
    public void testScoring()
    {
        Index<Node> index = nodeIndex( LuceneIndexImplementation.FULLTEXT_CONFIG );
        Node node1 = graphDb.createNode();
        Node node2 = graphDb.createNode();
        String key = "text";
        // Where the heck did I get this sentence from?
        index.add( node1, key, "a time where no one was really awake" );
        index.add( node2, key, "once upon a time there was" );
        restartTx();

        QueryContext queryContext = new QueryContext( "once upon a time was" ).sort( Sort.RELEVANCE );
        try ( IndexHits<Node> hits = index.query( key, queryContext ) )
        {
            Node hit1 = hits.next();
            float score1 = hits.currentScore();
            Node hit2 = hits.next();
            float score2 = hits.currentScore();
            assertEquals( node2, hit1 );
            assertEquals( node1, hit2 );
            assertTrue( "Score 1 (" + score1 + ") should have been higher than score 2 (" + score2 + ")",
                    score1 > score2 );
        }
    }

    @Test
    public void testTopHits()
    {
        Index<Relationship> index = relationshipIndex( LuceneIndexImplementation.FULLTEXT_CONFIG );
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
        String query = "one two three four five six seven";

        for ( int i = 0; i < 2; i++ )
        {
            assertContainsInOrder( index.query( key, new QueryContext( query ).top( 3 ).sort(
                    Sort.RELEVANCE ) ), rel1, rel2, rel3 );
            restartTx();
        }
    }

    @Test
    public void testSimilarity()
    {
        Index<Node> index = nodeIndex( MapUtil.stringMap( IndexManager.PROVIDER, "lucene",
                "type", "fulltext", "similarity",  DefaultSimilarity.class.getName() ) );
        Node node = graphDb.createNode();
        index.add( node, "key", "value" );
        restartTx();
        assertContains( index.get( "key", "value" ), node );
    }

    @Test
    public void testCombinedHitsSizeProblem()
    {
        Index<Node> index = nodeIndex( LuceneIndexImplementation.EXACT_CONFIG );
        Node node1 = graphDb.createNode();
        Node node2 = graphDb.createNode();
        Node node3 = graphDb.createNode();
        String key = "key";
        String value = "value";
        index.add( node1, key, value );
        index.add( node2, key, value );
        restartTx();
        index.add( node3, key, value );
        try ( IndexHits<Node> hits = index.get( key, value ) )
        {
            assertEquals( 3, hits.size() );
        }
    }

    @SuppressWarnings( "unchecked" )
    private <T extends PropertyContainer> void testRemoveWithoutKey(
            EntityCreator<T> creator, Index<T> index )
    {
        String key1 = "key1";
        String key2 = "key2";
        String value = "value";

        T entity1 = creator.create();
        index.add( entity1, key1, value );
        index.add( entity1, key2, value );
        T entity2 = creator.create();
        index.add( entity2, key1, value );
        index.add( entity2, key2, value );
        restartTx();

        assertContains( index.get( key1, value ), entity1, entity2 );
        assertContains( index.get( key2, value ), entity1, entity2 );
        index.remove( entity1, key2 );
        assertContains( index.get( key1, value ), entity1, entity2 );
        assertContains( index.get( key2, value ), entity2 );
        index.add( entity1, key2, value );
        for ( int i = 0; i < 2; i++ )
        {
            assertContains( index.get( key1, value ), entity1, entity2 );
            assertContains( index.get( key2, value ), entity1, entity2 );
            restartTx();
        }
    }

    @Test
    public void testRemoveWithoutKeyNodes()
    {
        testRemoveWithoutKey( NODE_CREATOR, nodeIndex(
                LuceneIndexImplementation.EXACT_CONFIG ) );
    }

    @Test
    public void testRemoveWithoutKeyRelationships()
    {
        testRemoveWithoutKey( RELATIONSHIP_CREATOR, relationshipIndex(
                LuceneIndexImplementation.EXACT_CONFIG ) );
    }

    @SuppressWarnings( "unchecked" )
    private <T extends PropertyContainer> void testRemoveWithoutKeyValue(
            EntityCreator<T> creator, Index<T> index )
    {
        String key1 = "key1";
        String value1 = "value1";
        String key2 = "key2";
        String value2 = "value2";

        T entity1 = creator.create();
        index.add( entity1, key1, value1 );
        index.add( entity1, key2, value2 );
        T entity2 = creator.create();
        index.add( entity2, key1, value1 );
        index.add( entity2, key2, value2 );
        restartTx();

        assertContains( index.get( key1, value1 ), entity1, entity2 );
        assertContains( index.get( key2, value2 ), entity1, entity2 );
        index.remove( entity1 );
        assertContains( index.get( key1, value1 ), entity2 );
        assertContains( index.get( key2, value2 ), entity2 );
        index.add( entity1, key1, value1 );

        for ( int i = 0; i < 2; i++ )
        {
            assertContains( index.get( key1, value1 ), entity1, entity2 );
            assertContains( index.get( key2, value2 ), entity2 );
            restartTx();
        }
    }

    @Test
    public void testRemoveWithoutKeyValueNodes()
    {
        testRemoveWithoutKeyValue( NODE_CREATOR, nodeIndex(
                LuceneIndexImplementation.EXACT_CONFIG ) );
    }

    @Test
    public void testRemoveWithoutKeyValueRelationships()
    {
        testRemoveWithoutKeyValue( RELATIONSHIP_CREATOR, relationshipIndex(
                LuceneIndexImplementation.EXACT_CONFIG ) );
    }

    @SuppressWarnings( "unchecked" )
    private <T extends PropertyContainer> void testRemoveWithoutKeyFulltext(
            EntityCreator<T> creator, Index<T> index )
    {
        String key1 = "key1";
        String key2 = "key2";
        String value1 = "value one";
        String value2 = "other value";
        String value = "value";

        T entity1 = creator.create();
        index.add( entity1, key1, value1 );
        index.add( entity1, key2, value1 );
        index.add( entity1, key2, value2 );
        T entity2 = creator.create();
        index.add( entity2, key1, value1 );
        index.add( entity2, key2, value1 );
        index.add( entity2, key2, value2 );
        restartTx();

        assertContains( index.query( key1, value ), entity1, entity2 );
        assertContains( index.query( key2, value ), entity1, entity2 );
        index.remove( entity1, key2 );
        assertContains( index.query( key1, value ), entity1, entity2 );
        assertContains( index.query( key2, value ), entity2 );
        index.add( entity1, key2, value1 );
        for ( int i = 0; i < 2; i++ )
        {
            assertContains( index.query( key1, value ), entity1, entity2 );
            assertContains( index.query( key2, value ), entity1, entity2 );
            restartTx();
        }
    }

    @Test
    public void testRemoveWithoutKeyFulltextNode()
    {
        testRemoveWithoutKeyFulltext( NODE_CREATOR,
                nodeIndex( LuceneIndexImplementation.FULLTEXT_CONFIG ) );
    }

    @Test
    public void testRemoveWithoutKeyFulltextRelationship()
    {
        testRemoveWithoutKeyFulltext( RELATIONSHIP_CREATOR,
                relationshipIndex( LuceneIndexImplementation.FULLTEXT_CONFIG ) );
    }

    @SuppressWarnings( "unchecked" )
    private <T extends PropertyContainer> void testRemoveWithoutKeyValueFulltext(
            EntityCreator<T> creator, Index<T> index )
    {
        String value = "value";
        String key1 = "key1";
        String value1 = value + " one";
        String key2 = "key2";
        String value2 = value + " two";

        T entity1 = creator.create();
        index.add( entity1, key1, value1 );
        index.add( entity1, key2, value2 );
        T entity2 = creator.create();
        index.add( entity2, key1, value1 );
        index.add( entity2, key2, value2 );
        restartTx();

        assertContains( index.query( key1, value ), entity1, entity2 );
        assertContains( index.query( key2, value ), entity1, entity2 );
        index.remove( entity1 );
        assertContains( index.query( key1, value ), entity2 );
        assertContains( index.query( key2, value ), entity2 );
        index.add( entity1, key1, value1 );
        for ( int i = 0; i < 2; i++ )
        {
            assertContains( index.query( key1, value ), entity1, entity2 );
            assertContains( index.query( key2, value ), entity2 );
            restartTx();
        }
    }

    @Test
    public void testRemoveWithoutKeyValueFulltextNode()
    {
        testRemoveWithoutKeyValueFulltext( NODE_CREATOR,
                nodeIndex( LuceneIndexImplementation.FULLTEXT_CONFIG ) );
    }

    @Test
    public void testRemoveWithoutKeyValueFulltextRelationship()
    {
        testRemoveWithoutKeyValueFulltext( RELATIONSHIP_CREATOR,
                relationshipIndex( LuceneIndexImplementation.FULLTEXT_CONFIG ) );
    }

    @Test
    public void testSortingWithTopHitsInPartCommittedPartLocal()
    {
        Index<Node> index = nodeIndex( LuceneIndexImplementation.FULLTEXT_CONFIG );
        Node first = graphDb.createNode();
        Node second = graphDb.createNode();
        Node third = graphDb.createNode();
        Node fourth = graphDb.createNode();
        String key = "key";

        index.add( third, key, "ccc" );
        index.add( second, key, "bbb" );
        restartTx();
        index.add( fourth, key, "ddd" );
        index.add( first, key, "aaa" );

        assertContainsInOrder( index.query( key, new QueryContext( "*" ).sort( key ) ), first, second, third, fourth );
        assertContainsInOrder( index.query( key, new QueryContext( "*" ).sort( key ).top( 2 ) ), first, second );
    }

    @Test
    public void shouldNotFindValueDeletedInSameTx()
    {
        Index<Node> nodeIndex = graphDb.index().forNodes( "size-after-removal" );
        Node node = graphDb.createNode();
        nodeIndex.add( node, "key", "value" );
        restartTx();

        nodeIndex.remove( node );
        for ( int i = 0; i < 2; i++ )
        {
            IndexHits<Node> hits = nodeIndex.get( "key", "value" );
            assertEquals( 0, hits.size() );
            assertNull( hits.getSingle() );
            hits.close();
            restartTx();
        }
    }

    @Test
    public void notAbleToIndexWithForbiddenKey()
    {
        Index<Node> index = graphDb.index().forNodes( "check-for-null" );
        Node node = graphDb.createNode();
        try
        {
            index.add( node, null, "not allowed" );
            fail( "Shouldn't be able to index something with null key" );
        }
        catch ( IllegalArgumentException e )
        { // OK
        }

        try
        {
            index.add( node, "_id_", "not allowed" );
            fail( "Shouldn't be able to index something with null key" );
        }
        catch ( IllegalArgumentException e )
        { // OK
        }
    }

    private Node createAndIndexNode( Index<Node> index, String key, String value )
    {
        Node node = graphDb.createNode();
        node.setProperty( key, value );
        index.add( node, key, value );
        return node;
    }

    @Test
    public void testRemoveNodeFromIndex()
    {
        Index<Node> index = nodeIndex( LuceneIndexImplementation.EXACT_CONFIG );
        String key = "key";
        String value = "MYID";
        Node node = createAndIndexNode( index, key, value );
        index.remove( node );
        node.delete();

        Node node2 = createAndIndexNode( index, key, value );
        assertEquals( node2, index.get( key, value ).getSingle() );
    }

    @Test
    public void canQueryWithWildcardEvenIfAlternativeRemovalMethodsUsedInSameTx1()
    {
        Index<Node> index = nodeIndex( LuceneIndexImplementation.EXACT_CONFIG );
        Node node = graphDb.createNode();
        index.add( node, "key", "value" );
        restartTx();
        index.remove( node, "key" );
        assertNull( index.query( "key", "v*" ).getSingle() );
        assertNull( index.query( "key", "*" ).getSingle() );
    }

    @Test
    public void canQueryWithWildcardEvenIfAlternativeRemovalMethodsUsedInSameTx2()
    {
        Index<Node> index = nodeIndex( LuceneIndexImplementation.EXACT_CONFIG );
        Node node = graphDb.createNode();
        index.add( node, "key", "value" );
        restartTx();
        index.remove( node );
        assertNull( index.query( "key", "v*" ).getSingle() );
        assertNull( index.query( "key", "*" ).getSingle() );
    }

    @Test
    public void updateIndex()
    {
        String TEXT = "text";
        String NUMERIC = "numeric";
        String TEXT_1 = "text_1";

        Index<Node> index = nodeIndex( LuceneIndexImplementation.EXACT_CONFIG );
        Node n = graphDb.createNode();
        index.add( n, NUMERIC, new ValueContext( 5 ).indexNumeric() );
        index.add(n, TEXT, "text");
        index.add(n, TEXT_1, "text");
        commitTx();

        beginTx();
        assertNotNull( index.query( QueryContext.numericRange( NUMERIC, 5, 5, true, true ) ).getSingle() );
        assertNotNull( index.get( TEXT_1, "text" ).getSingle() );
        index.remove( n, TEXT, "text" );
        index.add( n, TEXT, "text 1" );
        commitTx();

        beginTx();
        assertNotNull( index.get(TEXT_1, "text").getSingle() );
        assertNotNull( index.query(QueryContext.numericRange(NUMERIC, 5, 5, true, true)).getSingle() );
    }

    @Test
    public void exactIndexWithCaseInsensitive()
    {
        Index<Node> index = nodeIndex( stringMap( "analyzer", LowerCaseKeywordAnalyzer.class.getName() ) );
        Node node = graphDb.createNode();
        index.add( node, "name", "Thomas Anderson" );
        assertContains( index.query( "name", "\"Thomas Anderson\"" ), node );
        assertContains( index.query( "name", "\"thoMas ANDerson\"" ), node );
        restartTx();
        assertContains( index.query( "name", "\"Thomas Anderson\"" ), node );
        assertContains( index.query( "name", "\"thoMas ANDerson\"" ), node );
    }

    @Test
    public void exactIndexWithCaseInsensitiveWithBetterConfig()
    {
        Index<Node> index = graphDb.index().forNodes( "exact-case-insensitive",
                stringMap( "type", "exact", "to_lower_case", "true" ) );
        Node node = graphDb.createNode();
        index.add( node, "name", "Thomas Anderson" );
        assertContains( index.query( "name", "\"Thomas Anderson\"" ), node );
        assertContains( index.query( "name", "\"thoMas ANDerson\"" ), node );
        restartTx();
        assertContains( index.query( "name", "\"Thomas Anderson\"" ), node );
        assertContains( index.query( "name", "\"thoMas ANDerson\"" ), node );
    }

    @Test
    public void notAbleToRemoveWithForbiddenKey()
    {
        Index<Node> index = nodeIndex( LuceneIndexImplementation.EXACT_CONFIG );
        Node node = graphDb.createNode();
        index.add( node, "name", "Mattias" );
        restartTx();
        try
        {
            index.remove( node, null );
            fail( "Shouldn't be able to" );
        }
        catch ( IllegalArgumentException e )
        {   // OK
        }
        try
        {
            index.remove( node, "_id_" );
            fail( "Shouldn't be able to" );
        }
        catch ( IllegalArgumentException e )
        {   // OK
        }
    }

    @Test
    public void putIfAbsentSingleThreaded()
    {
        Index<Node> index = nodeIndex( LuceneIndexImplementation.EXACT_CONFIG );
        Node node = graphDb.createNode();
        String key = "name";
        String value = "Mattias";
        String value2 = "Persson";
        assertNull( index.putIfAbsent( node, key, value ) );
        assertEquals( node, index.get( key, value ).getSingle() );
        assertNotNull( index.putIfAbsent( node, key, value ) );
        assertNull( index.putIfAbsent( node, key, value2 ) );
        assertNotNull( index.putIfAbsent( node, key, value2 ) );
        restartTx();
        assertNotNull( index.putIfAbsent( node, key, value ) );
        assertNotNull( index.putIfAbsent( node, key, value2 ) );
        assertEquals( node, index.get( key, value ).getSingle() );
    }

    @Test
    public void putIfAbsentMultiThreaded() throws Exception
    {
        Index<Node> index = nodeIndex( LuceneIndexImplementation.EXACT_CONFIG );
        Node node = graphDb.createNode();
        commitTx();
        String key = "name";
        String value = "Mattias";

        WorkThread t1 = new WorkThread( "t1", index, graphDb, node );
        WorkThread t2 = new WorkThread( "t2", index, graphDb, node );
        t1.beginTransaction();
        t2.beginTransaction();
        assertNull( t2.putIfAbsent( node, key, value ).get() );
        Future<Node> futurePut = t1.putIfAbsent( node, key, value );
        t1.waitUntilWaiting();
        t2.commit();
        assertNotNull( futurePut.get() );
        t1.commit();
        t1.close();
        t2.close();

        try ( Transaction transaction = graphDb.beginTx() )
        {
            assertEquals( node, index.get( key, value ).getSingle() );
        }
    }

    @Test
    public void putIfAbsentOnOtherValueInOtherThread() throws Exception
    {
        Index<Node> index = nodeIndex( LuceneIndexImplementation.EXACT_CONFIG );
        Node node = graphDb.createNode();
        commitTx();
        String key = "name";
        String value = "Mattias";
        String otherValue = "Tobias";

        WorkThread t1 = new WorkThread( "t1", index, graphDb, node );
        WorkThread t2 = new WorkThread( "t2", index, graphDb, node );
        t1.beginTransaction();
        t2.beginTransaction();
        assertNull( t2.putIfAbsent( node, key, value ).get() );
        Future<Node> futurePut = t1.putIfAbsent( node, key, otherValue );
        t2.commit();
        assertNull( futurePut.get() );
        t1.commit();
        t1.close();
        t2.close();

        try ( Transaction transaction = graphDb.beginTx() )
        {
            assertEquals( node, index.get( key, value ).getSingle() );
            assertEquals( node, index.get( key, otherValue ).getSingle() );
        }
    }

    @Test
    public void putIfAbsentOnOtherKeyInOtherThread() throws Exception
    {
        Index<Node> index = nodeIndex( LuceneIndexImplementation.EXACT_CONFIG );
        Node node = graphDb.createNode();
        commitTx();
        String key = "name";
        String otherKey = "friend";
        String value = "Mattias";

        WorkThread t1 = new WorkThread( "t1", index, graphDb, node );
        WorkThread t2 = new WorkThread( "t2", index, graphDb, node );
        t1.beginTransaction();
        t2.beginTransaction();
        assertNull( t2.putIfAbsent( node, key, value ).get() );
        assertNull( t1.putIfAbsent( node, otherKey, value ).get() );
        t2.commit();
        t1.commit();
        t1.close();
        t2.close();

        try ( Transaction transaction = graphDb.beginTx() )
        {
            assertEquals( node, index.get( key, value ).getSingle() );
            assertEquals( node, index.get( otherKey, value ).getSingle() );
        }
    }

    @Test
    public void putIfAbsentShouldntBlockIfNotAbsent() throws Exception
    {
        Index<Node> index = nodeIndex( LuceneIndexImplementation.EXACT_CONFIG );
        Node node = graphDb.createNode();
        String key = "key";
        String value = "value";
        index.add( node, key, value );
        restartTx();

        WorkThread otherThread = new WorkThread( "other thread", index, graphDb, node );
        otherThread.beginTransaction();

        // Should not grab lock
        index.putIfAbsent( node, key, value );

        // Should be able to complete right away
        assertNotNull( otherThread.putIfAbsent( node, key, value ).get() );

        otherThread.commit();
        commitTx();

        otherThread.close();
    }

    @Test
    public void getOrCreateNodeWithUniqueFactory()
    {
        final String key = "name";
        final String value = "Mattias";
        final String property = "counter";

        final Index<Node> index = nodeIndex( LuceneIndexImplementation.EXACT_CONFIG );
        final AtomicInteger counter = new AtomicInteger();
        UniqueFactory<Node> factory = new UniqueFactory.UniqueNodeFactory( index )
        {
            @Override
            protected void initialize( Node node, Map<String, Object> properties )
            {
                assertEquals( value, properties.get( key ) );
                assertEquals( 1, properties.size() );
                node.setProperty( property, counter.getAndIncrement() );
            }
        };
        Node unique = factory.getOrCreate( key, value );

        assertNotNull( unique );
        assertEquals( "not initialized", 0, unique.getProperty( property, null ) );
        assertEquals( unique, index.get( key, value ).getSingle() );

        assertEquals( unique, factory.getOrCreate( key, value ) );
        assertEquals( "initialized more than once", 0, unique.getProperty( property ) );
        assertEquals( unique, index.get( key, value ).getSingle() );
        finishTx( false );
    }

    @Test
    public void getOrCreateRelationshipWithUniqueFactory()
    {
        final String key = "name";
        final String value = "Mattias";

        final Node root = graphDb.createNode();
        final Index<Relationship> index = relationshipIndex( LuceneIndexImplementation.EXACT_CONFIG );
        final RelationshipType type = withName( "SINGLE" );
        UniqueFactory<Relationship> factory = new UniqueFactory.UniqueRelationshipFactory( index )
        {
            @Override
            protected Relationship create( Map<String, Object> properties )
            {
                assertEquals( value, properties.get( key ) );
                assertEquals( 1, properties.size() );
                return root.createRelationshipTo( graphDatabase().createNode(), type );
            }
        };

        Relationship unique = factory.getOrCreate( key, value );
        assertEquals( unique, root.getSingleRelationship( type, Direction.BOTH ) );
        assertNotNull( unique );

        assertEquals( unique, index.get( key, value ).getSingle() );

        assertEquals( unique, factory.getOrCreate( key, value ) );
        assertEquals( unique, root.getSingleRelationship( type, Direction.BOTH ) );
        assertEquals( unique, index.get( key, value ).getSingle() );

        finishTx( false );
    }

    @Test
    public void getOrCreateMultiThreaded() throws Exception
    {
        Index<Node> index = nodeIndex( LuceneIndexImplementation.EXACT_CONFIG );
        String key = "name";
        String value = "Mattias";

        WorkThread t1 = new WorkThread( "t1", index, graphDb, null );
        WorkThread t2 = new WorkThread( "t2", index, graphDb, null );
        t1.beginTransaction();
        t2.beginTransaction();
        Node node = t2.getOrCreate( key, value, 0 ).get();
        assertNotNull( node );
        assertEquals( 0, t2.getProperty( node, key ) );
        Future<Node> futurePut = t1.getOrCreate( key, value, 1 );
        t1.waitUntilWaiting();
        t2.commit();
        assertEquals( node, futurePut.get() );
        assertEquals( 0, t1.getProperty( node, key ) );
        t1.commit();

        assertEquals( node, index.get( key, value ).getSingle() );

        t1.close();
        t2.close();
    }

    @Test
    public void useStandardAnalyzer()
    {
        Index<Node> index = nodeIndex( stringMap( "analyzer", MyStandardAnalyzer.class.getName() ) );
        Node node = graphDb.createNode();
        index.add( node, "name", "Mattias" );
    }

    @Test
    public void numericValueForGetInExactIndex()
    {
        Index<Node> index = nodeIndex( LuceneIndexImplementation.EXACT_CONFIG );
        numericValueForGet( index );
    }

    @Test
    public void numericValueForGetInFulltextIndex()
    {
        Index<Node> index = nodeIndex( LuceneIndexImplementation.FULLTEXT_CONFIG );
        numericValueForGet( index );
    }

    private void numericValueForGet( Index<Node> index )
    {
        Node node = graphDb.createNode();
        long id = 100L;
        index.add( node, "name", ValueContext.numeric( id ) );
        assertEquals( node, index.get( "name", ValueContext.numeric( id ) ).getSingle() );
        restartTx();
        assertEquals( node, index.get( "name", ValueContext.numeric( id ) ).getSingle() );
    }

    @Test
    public void combinedNumericalQuery()
    {
        Index<Node> index = nodeIndex( LuceneIndexImplementation.EXACT_CONFIG );

        Node node = graphDb.createNode();
        index.add( node, "start", ValueContext.numeric( 10 ) );
        index.add( node, "end", ValueContext.numeric( 20 ) );
        restartTx();

        BooleanQuery q = new BooleanQuery();
        q.add( LuceneUtil.rangeQuery( "start", 9, null, true, true ), Occur.MUST );
        q.add( LuceneUtil.rangeQuery( "end", null, 30, true, true ), Occur.MUST );
        assertContains( index.query( q ), node );
    }

    @Test
    public void failureToCreateAnIndexShouldNotLeaveConfigurationBehind()
    {
        // WHEN
        try
        {
            // PerFieldAnalyzerWrapper is invalid since it has no public no-arg constructor
            nodeIndex( stringMap( "analyzer", PerFieldAnalyzerWrapper.class.getName() ) );
            fail( "Should have failed" );
        }
        catch ( RuntimeException e )
        {
            assertThat( e.getMessage(), CoreMatchers.containsString( PerFieldAnalyzerWrapper.class.getName() ) );
        }

        // THEN - assert that there's no index config about this index left behind
        assertFalse( "There should be no index config for index '" + currentIndexName() + "' left behind",
                ((GraphDatabaseAPI)graphDb).getDependencyResolver().resolveDependency( IndexConfigStore.class ).has(
                        Node.class, currentIndexName() ) );
    }

    @Test
    public void shouldBeAbleToQueryAllMatchingDocsAfterRemovingWithWildcard()
    {
        // GIVEN
        Index<Node> index = nodeIndex( EXACT_CONFIG );
        Node node1 = graphDb.createNode();
        index.add( node1, "name", "Mattias" );
        finishTx( true );
        beginTx();

        // WHEN
        index.remove( node1, "name" );
        Set<Node> nodes = Iterables.asSet( index.query( "*:*" ) );

        // THEN
        assertEquals( asSet(), nodes );
    }

    @Test
    public void shouldNotSeeDeletedRelationshipWhenQueryingWithStartAndEndNode()
    {
        // GIVEN
        RelationshipIndex index = relationshipIndex( EXACT_CONFIG );
        Node start = graphDb.createNode();
        Node end = graphDb.createNode();
        RelationshipType type = withName( "REL" );
        Relationship rel = start.createRelationshipTo( end, type );
        index.add( rel, "Type", type.name() );
        finishTx( true );
        beginTx();

        // WHEN
        IndexHits<Relationship> hits = index.get( "Type", type.name(), start, end );
        assertEquals( 1, count( hits ) );
        assertEquals( 1, hits.size() );
        index.remove( rel );

        // THEN
        hits = index.get( "Type", type.name(), start, end );
        assertEquals( 0, count( hits ) );
        assertEquals( 0, hits.size() );
    }

    @Test
    public void shouldNotBeAbleToAddNullValuesToNodeIndex()
    {
        // GIVEN
        Index<Node> index = nodeIndex( EXACT_CONFIG );

        // WHEN single null
        try
        {
            index.add( graphDb.createNode(), "key", null );
            fail( "Should have failed" );
        }
        catch ( IllegalArgumentException e )
        {
            // THEN Good
        }

        // WHEN null in array
        try
        {
            index.add( graphDb.createNode(), "key", new String[] {"a", null, "c"} );
            fail( "Should have failed" );
        }
        catch ( IllegalArgumentException e )
        {
            // THEN Good
        }
    }

    @Test
    public void shouldNotBeAbleToAddNullValuesToRelationshipIndex()
    {
        // GIVEN
        RelationshipIndex index = relationshipIndex( EXACT_CONFIG );

        // WHEN single null
        try
        {
            index.add( graphDb.createNode().createRelationshipTo( graphDb.createNode(), MyRelTypes.TEST ), "key",
                    null );
            fail( "Should have failed" );
        }
        catch ( IllegalArgumentException e )
        {
            // THEN Good
        }

        // WHEN null in array
        try
        {
            index.add( graphDb.createNode().createRelationshipTo( graphDb.createNode(), MyRelTypes.TEST ), "key",
                    new String[] {"a", null, "c"} );
            fail( "Should have failed" );
        }
        catch ( IllegalArgumentException e )
        {
            // THEN Good
        }
    }

    @Test
    public void tooLongValuesAreNotAllowedInNodeIndex()
    {
        Index<Node> index = nodeIndex( EXACT_CONFIG );
        String tooLongValue = RandomStringUtils.randomAlphabetic( 36000 );
        try
        {
            index.add( graphDb.createNode(), "key", tooLongValue );
            fail( "Validation exception expected. Such long values are not allowed in the index." );
        }
        catch ( IllegalArgumentException e )
        {
            // expected
        }

        try
        {
            index.add( graphDb.createNode(), "key", new String[] {"a", tooLongValue, "c"} );
            fail( "Validation exception expected. Such long values are not allowed in the index." );
        }
        catch ( IllegalArgumentException e )
        {
            // expected
        }

    }

    @Test
    public void tooLongValuesAreNotAllowedInRelationshipIndex()
    {
        RelationshipIndex index = relationshipIndex( EXACT_CONFIG );
        String tooLongValue = RandomStringUtils.randomAlphabetic( 36000 );
        try
        {
            index.add( graphDb.createNode().createRelationshipTo( graphDb.createNode(), MyRelTypes.TEST ), "key",
                    tooLongValue );
            fail( "Validation exception expected. Such long values are not allowed in the index." );
        }
        catch ( IllegalArgumentException e )
        {
            // expected
        }

        try
        {
            index.add( graphDb.createNode().createRelationshipTo( graphDb.createNode(), MyRelTypes.TEST ), "key",
                    new String[] {"a", tooLongValue, "c"} );
            fail( "Validation exception expected. Such long values are not allowed in the index." );
        }
        catch ( IllegalArgumentException e )
        {
            // expected
        }
    }

    @Test
    public void shouldRemoveAllMatchingIndexEntriesWhenCallingRemoveWithEntityAndKey()
    {
        // GIVEN
        Index<Node> index = nodeIndex( FULLTEXT_CONFIG );
        String key = "the-key";
        String value1 = "First";
        String value2 = "Second";
        Node node = graphDb.createNode();
        node.setProperty( key, value1 );
        index.add( node, key, value1 );
        restartTx();
        assertEquals( 1, countHits( index.query( "*:*" ) ) );

        // WHEN adding one more property to the index
        node.setProperty( key, value2 );
        index.add( node, key, value2 ); // intentionally just add to the index
        restartTx();
        assertEquals( 1, countHits( index.query( "*:*" ) ) );

        // and WHEN removing by node and key
        node.removeProperty( key );
        index.remove( node, key );
        restartTx();

        // THEN
        assertEquals( 0, countHits( index.query( "*:*" ) ) );
    }

    private long countHits( IndexHits<Node> query )
    {
        try
        {
            return count( query );
        }
        finally
        {
            query.close();
        }
    }

    private void queryAndSortNodesByNumericProperty( Index<Node> index, String numericProperty )
    {
        queryAndSortNodesByNumericProperty( index, numericProperty, i -> i );
    }

    private void queryAndSortNodesByNumericProperty( Index<Node> index, String numericProperty,
            IntFunction<? extends Number> expectedValueProvider )
    {
        try ( Transaction transaction = graphDb.beginTx() )
        {
            QueryContext queryContext = new QueryContext( numericProperty + ":**" );
            queryContext.sort( new Sort( new SortedNumericSortField( numericProperty, SortField.Type.INT, false ) ) );
            IndexHits<Node> nodes = index.query( queryContext );

            int nodeIndex = 0;
            for ( Node node : nodes )
            {
                assertEquals("Nodes should be sorted by numeric property",
                        expectedValueProvider.apply( nodeIndex++), node.getProperty( numericProperty ));
            }
            transaction.success();
        }
    }

    private void queryAndSortNodesByStringProperty( Index<Node> index, String stringProperty, String[] values )
    {
        queryAndSortNodesByStringProperty( index, stringProperty, i -> values[i] );
    }

    private void queryAndSortNodesByStringProperty( Index<Node> index, String stringProperty,
            IntFunction<String> expectedValueProvider )
    {
        try ( Transaction transaction = graphDb.beginTx() )
        {
            QueryContext queryContext = new QueryContext( stringProperty + ":**" );
            queryContext.sort( new Sort( new SortedSetSortField( stringProperty, true ) ) );
            IndexHits<Node> nodes = index.query( queryContext );

            int nodeIndex = 0;
            for ( Node node : nodes )
            {
                assertEquals("Nodes should be sorted by string property", expectedValueProvider.apply( nodeIndex++ ),
                        node.getProperty( stringProperty ));
            }
            transaction.success();
        }
    }

    private void doubleNumericPropertyValueForAllNodesWithLabel( Index<Node> index, String numericProperty,
            Label label )
    {
        try ( Transaction transaction = graphDb.beginTx() )
        {
            ResourceIterator<Node> nodes = graphDb.findNodes( label );
            nodes.stream().forEach( node ->
            {
                node.setProperty( numericProperty, (Integer) node.getProperty( numericProperty ) * 2 );
                index.remove( node, numericProperty );
                index.add( node, numericProperty,
                        new ValueContext( node.getProperty( numericProperty ) ).indexNumeric() );
            } );
            transaction.success();
        }
    }

    private void setPropertiesAndUpdateToJunior( Index<Node> index, String nameProperty, String jobNameProperty,
            String[] names, String[] jobs, Label characters )
    {
        try ( Transaction transaction = graphDb.beginTx() )
        {
            for ( int i = 0; i < names.length; i++ )
            {
                Node node = graphDb.createNode(characters);
                node.setProperty( nameProperty, names[i] );
                node.setProperty( jobNameProperty, jobs[i] );
                index.add( node, nameProperty, names[i] );
                index.add( node, jobNameProperty, jobs[i] );
            }
            transaction.success();
        }

        try ( Transaction transaction = graphDb.beginTx() )
        {
            ResourceIterator<Node> nodes = graphDb.findNodes( characters );
            nodes.stream().forEach( node ->
            {
                node.setProperty( jobNameProperty, "junior " + node.getProperty( jobNameProperty ) );
                index.add( node, jobNameProperty, node.getProperty( jobNameProperty ) );
            } );
            transaction.success();
        }
    }
}
