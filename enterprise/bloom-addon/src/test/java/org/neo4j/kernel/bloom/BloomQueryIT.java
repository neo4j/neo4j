/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.bloom;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongSet;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.test.rule.DatabaseRule;
import org.neo4j.test.rule.EmbeddedDatabaseRule;

import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.joining;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.kernel.bloom.BloomIT.AWAIT_POPULATION;
import static org.neo4j.kernel.bloom.BloomIT.ENTITYID;
import static org.neo4j.kernel.bloom.BloomIT.NODES;
import static org.neo4j.kernel.bloom.BloomIT.NODES_ADVANCED;
import static org.neo4j.kernel.bloom.BloomIT.RELS_ADVANCED;
import static org.neo4j.kernel.bloom.BloomIT.SCORE;
import static org.neo4j.kernel.bloom.BloomIT.SET_NODE_KEYS;
import static org.neo4j.kernel.bloom.BloomIT.SET_REL_KEYS;

public class BloomQueryIT
{
    private static final String PROP = "prop";
    static final String NODES_PARAMETER = "CALL bloom.searchNodes($query, $fuzzy, $matchAll)";

    @Rule
    public DatabaseRule db = new EmbeddedDatabaseRule();

    @Before
    public void before() throws KernelException
    {
        db.ensureStarted();
        db.getDependencyResolver().resolveDependency( Procedures.class ).registerProcedure( BloomProcedures.class );
    }

    @After
    public void after()
    {
        if ( db != null )
        {
            db.shutdown();
        }
    }

    @Test
    public void exactQueryShouldBeExact() throws Exception
    {
        db.execute( String.format( SET_NODE_KEYS, "\"prop\"" ) );
        db.execute( String.format( SET_REL_KEYS, "\"prop\"" ) );
        try ( Transaction transaction = db.beginTx() )
        {
            Node node1 = db.createNode();
            node1.setProperty( PROP, "This is a integration test." );
            Node node2 = db.createNode();
            node2.setProperty( PROP, "This is a related integration test" );
            Relationship relationship = node1.createRelationshipTo( node2, RelationshipType.withName( "type" ) );
            relationship.setProperty( PROP, "They relate" );
            transaction.success();
        }
        db.execute( AWAIT_POPULATION ).close();

        Result result = db.execute( String.format( NODES_ADVANCED, "\"integration\"", false, false ) );
        assertEquals( 0L, result.next().get( ENTITYID ) );
        assertEquals( 1L, result.next().get( ENTITYID ) );
        assertFalse( result.hasNext() );
        result = db.execute( String.format( NODES_ADVANCED, "\"integratiun\"", false, false ) );
        assertFalse( result.hasNext() );

        result = db.execute( String.format( RELS_ADVANCED, "\"relate\"", false, false ) );
        assertEquals( 0L, result.next().get( ENTITYID ) );
        assertFalse( result.hasNext() );
        result = db.execute( String.format( RELS_ADVANCED, "\"relite\"", false, false ) );
        assertFalse( result.hasNext() );
    }

    @Test
    public void matchAllQueryShouldMatchAll() throws Exception
    {
        db.execute( String.format( SET_NODE_KEYS, "\"prop\"" ) );
        db.execute( String.format( SET_REL_KEYS, "\"prop\"" ) );
        db.execute( AWAIT_POPULATION ).close();

        try ( Transaction transaction = db.beginTx() )
        {
            Node node1 = db.createNode();
            node1.setProperty( PROP, "This is a integration test." );
            Node node2 = db.createNode();
            node2.setProperty( PROP, "This is a related integration test" );
            Relationship relationship = node1.createRelationshipTo( node2, RelationshipType.withName( "type" ) );
            relationship.setProperty( PROP, "They relate" );
            transaction.success();
        }

        Result result = db.execute( String.format( NODES_ADVANCED, "\"integration\", \"related\"", false, true ) );
        assertEquals( 1L, result.next().get( ENTITYID ) );
        assertFalse( result.hasNext() );

        result = db.execute( String.format( RELS_ADVANCED, "\"they\", \"relate\"", false, true ) );
        assertEquals( 0L, result.next().get( ENTITYID ) );
        assertFalse( result.hasNext() );
        result = db.execute( String.format( RELS_ADVANCED, "\"relate\", \"sometimes\"", false, true ) );
        assertFalse( result.hasNext() );
    }

    @Test
    public void exactMatchShouldScoreMuchBetterThatAlmostNotMatching() throws Exception
    {
        setIndexedNodeProperties( PROP );

        try ( Transaction transaction = db.beginTx() )
        {
            Node node1 = db.createNode();
            node1.setProperty( PROP, "This is a integration test that involves scoring and thus needs a longer sentence." );
            Node node2 = db.createNode();
            node2.setProperty( PROP, "tase" );
            transaction.success();
        }

        Result result = db.execute( String.format( NODES, "\"integration\", \"test\", \"involves\", \"scoring\", \"needs\", \"sentence\"" ) );
        assertTrue( result.hasNext() );
        Map<String,Object> firstResult = result.next();
        assertTrue( result.hasNext() );
        Map<String,Object> secondResult = result.next();
        assertFalse( result.hasNext() );
        assertEquals( 0L, firstResult.get( ENTITYID ) );
        assertEquals( 1L, secondResult.get( ENTITYID ) );
        assertThat( (double) firstResult.get( SCORE ), greaterThan( (double) secondResult.get( SCORE ) * 10 ) );
    }

    @Test
    public void fuzzyQueryShouldBeFuzzy() throws Exception
    {
        setIndexedNodeProperties( PROP );

        long firstID = createNodeIndexableByPropertyValue( PROP, "Hello. Hello again." );
        long secondID = createNodeIndexableByPropertyValue( PROP, "A zebroid (also zedonk, zorse, zebra mule, zonkey, and zebmule) is the offspring of any " +
                "cross between a zebra and any other equine: essentially, a zebra hybrid." );

        assertQueryFindsNodeIds( "hella", true, false, firstID );
        assertQueryFindsNodeIds( "zebre", true, false, secondID );
        assertQueryFindsNodeIds( "zedink", true, false, secondID );
        assertQueryFindsNodeIds( "cruss", true, false, secondID );
        assertQueryFindsNoNodes( "hella", false, false );
        assertQueryFindsNoNodes( "zebre", false, false );
        assertQueryFindsNoNodes( "zedink", false, false );
        assertQueryFindsNoNodes( "cruss", false, false );
    }

    @Test
    public void fuzzyQueryShouldReturnExactMatchesFirst() throws Exception
    {
        setIndexedNodeProperties( PROP );

        long firstID = createNodeIndexableByPropertyValue( PROP, "zibre" );
        long secondID = createNodeIndexableByPropertyValue( PROP, "zebrae" );
        long thirdID = createNodeIndexableByPropertyValue( PROP, "zebra" );
        long fourthID = createNodeIndexableByPropertyValue( PROP, "zibra" );

        assertQueryFindsNodeIdsInOrder( "zebra", true, true, thirdID, secondID, fourthID, firstID );
    }

    @Test
    public void shouldReturnMatchesThatContainLuceneSyntaxCharacters() throws Exception
    {
        setIndexedNodeProperties( PROP );

        String[] luceneSyntaxElements = {"+", "-", "&&", "||", "!", "(", ")", "{", "}", "[", "]", "^", "\"", "~", "*", "?", ":", "\\"};

        long nodeId;
        try ( Transaction tx = db.beginTx() )
        {
            nodeId = db.createNodeId();
            tx.success();
        }

        for ( String elm : luceneSyntaxElements )
        {
            setNodeProp( nodeId, PROP, "Hello" + elm + " How are you " + elm + "today?" );

            try ( Transaction tx = db.beginTx() )
            {
                assertQueryFindsNodeIds( "Hello" + elm, false, false, nodeId );
                assertQueryFindsNodeIds( elm + "today", false, false, nodeId );
            }
        }
    }

    @Test
    public void exactMatchAllShouldOnlyReturnStuffThatMatchesAll() throws Exception
    {
        setIndexedNodeProperties( "first", "last" );
        long firstID;
        long secondID;
        long thirdID;
        long fourthID;
        long fifthID;
        try ( Transaction tx = db.beginTx() )
        {
            firstID = db.createNodeId();
            secondID = db.createNodeId();
            thirdID = db.createNodeId();
            fourthID = db.createNodeId();
            fifthID = db.createNodeId();
        }
        setNodeProp( firstID, "first", "Full" );
        setNodeProp( firstID, "last", "Hanks" );
        setNodeProp( secondID, "first", "Tom" );
        setNodeProp( secondID, "last", "Hunk" );
        setNodeProp( thirdID, "first", "Tom" );
        setNodeProp( thirdID, "last", "Hanks" );
        setNodeProp( fourthID, "first", "Tom Hanks" );
        setNodeProp( fourthID, "last", "Tom Hanks" );
        setNodeProp( fifthID, "last", "Tom Hanks" );
        setNodeProp( fifthID, "first", "Morgan" );

        assertQueryFindsNodeIds( Arrays.asList( "Tom", "Hanks" ), false, true, thirdID, fourthID, fifthID );
    }

    @Test
    public void fuzzyMatchAllShouldOnlyReturnStuffThatKindaMatchesAll() throws Exception
    {
        setIndexedNodeProperties( "first", "last" );
        long firstID;
        long secondID;
        long thirdID;
        long fourthID;
        long fifthID;
        try ( Transaction tx = db.beginTx() )
        {
            firstID = db.createNodeId();
            secondID = db.createNodeId();
            thirdID = db.createNodeId();
            fourthID = db.createNodeId();
            fifthID = db.createNodeId();
        }
        setNodeProp( firstID, "first", "Christian" );
        setNodeProp( firstID, "last", "Hanks" );
        setNodeProp( secondID, "first", "Tom" );
        setNodeProp( secondID, "last", "Hungarian" );
        setNodeProp( thirdID, "first", "Tom" );
        setNodeProp( thirdID, "last", "Hunk" );
        setNodeProp( fourthID, "first", "Tim" );
        setNodeProp( fourthID, "last", "Hanks" );
        setNodeProp( fifthID, "last", "Tom Hanks" );
        setNodeProp( fifthID, "first", "Morgan" );

        assertQueryFindsNodeIds( Arrays.asList( "Tom", "Hanks" ), true, true, thirdID, fourthID, fifthID );
    }

    private void setIndexedNodeProperties( String... properties )
    {
        db.execute( String.format( SET_NODE_KEYS, Arrays.stream( properties ).collect( joining( "\", \"", "\"", "\"" ) ) ) );
        db.execute( AWAIT_POPULATION ).close();
    }

    private void setNodeProp( long nodeId, String prop, String value )
    {
        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.getNodeById( nodeId );
            node.setProperty( prop, value );
            tx.success();
        }
    }

    private long createNodeIndexableByPropertyValue( String key, String value )
    {
        try ( Transaction transaction = db.beginTx() )
        {
            Node node = db.createNode();
            node.setProperty( key, value );
            transaction.success();
            return node.getId();
        }
    }

    private void assertQueryFindsNoNodes( String query, boolean fuzzy, boolean matchAll ) throws IOException, IndexNotFoundKernelException
    {
        assertQueryFindsNodeIds( query, fuzzy, matchAll );
    }

    private void assertQueryFindsNodeIds( String query, boolean fuzzy, boolean matchAll, long... ids ) throws IOException, IndexNotFoundKernelException
    {
        assertQueryFindsNodeIds( singletonList( query ), fuzzy, matchAll, ids );
    }

    private void assertQueryFindsNodeIds( Collection<String> query, boolean fuzzy, boolean matchAll, long... ids )
            throws IOException, IndexNotFoundKernelException
    {
        try ( Result result = db.execute( NODES_PARAMETER, MapUtil.map( "query", query, "fuzzy", fuzzy, "matchAll", matchAll ) ) )
        {
            assertQueryResultsMatch( result, ids );
        }
    }

    private void assertQueryResultsMatch( Result result, long[] ids )
    {
        PrimitiveLongSet set = PrimitiveLongCollections.setOf( ids );
        while ( result.hasNext() )
        {
            long next = (long) result.next().get( ENTITYID );
            assertTrue( String.format( "Result returned node id %d, expected one of %s", next, Arrays.toString( ids ) ), set.remove( next ) );
        }
        if ( !set.isEmpty() )
        {
            List<Long> list = new ArrayList<>();
            set.visitKeys( k -> !list.add( k ) );
            fail( "Number of results differ from expected. " + set.size() + " IDs were not found in the result: " + list );
        }
        if ( result.hasNext() )
        {
            fail( "Query found more results than expected" );
        }
    }

    private void assertQueryFindsNodeIdsInOrder( String query, boolean fuzzy, boolean matchAll, long... ids )
    {
        assertQueryFindsNodeIdsInOrder( singletonList( query ), fuzzy, matchAll, ids );
    }

    private void assertQueryFindsNodeIdsInOrder( Collection<String> query, boolean fuzzy, boolean matchAll, long... ids )
    {
        try ( Result result = db.execute( NODES_PARAMETER, MapUtil.map( "query", query, "fuzzy", fuzzy, "matchAll", matchAll ) ) )
        {
            assertQueryResultsMatchInOrder( result, ids );
        }
    }

    private void assertQueryResultsMatchInOrder( Result result, long[] ids )
    {
        int num = 0;
        double score = Float.MAX_VALUE;
        while ( result.hasNext() )
        {
            Map<String,Object> scoredResult = result.next();
            long nextId = (long) scoredResult.get( ENTITYID );
            double nextScore = (double) scoredResult.get( SCORE );
            assertThat( nextScore, lessThanOrEqualTo( score ) );
            score = nextScore;
            assertEquals( String.format( "Result returned node id %d, expected %d", nextId, ids[num] ), ids[num], nextId );
            num++;
        }
        assertEquals( "Number of results differ from expected", ids.length, num );
    }
}
