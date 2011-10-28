/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.cypher.javacompat;

import org.junit.Before;
import org.junit.Test;
import org.neo4j.cypher.commands.Query;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.test.ImpermanentGraphDatabase;

import java.io.IOException;
import java.util.*;

import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.*;
import static org.junit.matchers.JUnitMatchers.*;
import static org.neo4j.cypher.javacompat.RegularExpressionMatcher.matchesPattern;
import static org.neo4j.helpers.collection.IteratorUtil.asIterable;

public class JavaExecutionEngineTests {

    private GraphDatabaseService db;
    private ExecutionEngine engine;
    private Node andreasNode;
    private Node johanNode;
    private Node michaelaNode;

    @Before
    public void setUp() throws IOException {
        db = new ImpermanentGraphDatabase();
        engine = new ExecutionEngine( db );
        Transaction tx = db.beginTx();
        andreasNode = db.createNode();
        johanNode = db.createNode();
        michaelaNode = db.getReferenceNode();
        andreasNode.setProperty( "name", "Andreas" );
        johanNode.setProperty( "name", "Johan" );
        michaelaNode.setProperty( "name", "Michaela" );

        index( andreasNode );
        index( johanNode );
        index( michaelaNode );


        tx.success();
        tx.finish();
    }

    private void index( Node n ) {
        db.index().forNodes( "people" ).add( n, "name", n.getProperty( "name" ) );
    }

    @Test
    public void exampleQuery() throws Exception {
// START SNIPPET: JavaQuery
        ExecutionEngine engine = new ExecutionEngine( db );
        ExecutionResult result = engine.execute( "start n=node(0) where 1=1 return n" );

        assertThat( result.columns(), hasItem( "n" ) );
        Iterator<Node> n_column = result.columnAs( "n" );
        assertThat( asIterable( n_column ), hasItem( db.getNodeById( 0 ) ) );
        assertThat( result.toString(), containsString( "Node[0]" ) );
// END SNIPPET: JavaQuery
    }

    @Test
    public void shouldBeAbleToEmitJavaIterables() throws Exception {
        makeFriends( michaelaNode, andreasNode );
        makeFriends( michaelaNode, johanNode );

        ExecutionEngine engine = new ExecutionEngine( db );

        ExecutionResult result = engine.execute( "start n=node(0) match n-->friend return collect(friend)" );

        Iterable<Node> friends = ( Iterable<Node> ) result.columnAs( "collect(friend)" ).next();
        assertThat( friends, hasItems( andreasNode, johanNode ) );

        Object friendCollection = result.iterator().next().get( "collect(friend)" );
        assertThat( friendCollection, instanceOf( Iterable.class ) );
    }

    @Test
    public void testColumnAreInTheRightOrder() throws Exception
    {
        createTenNodes();
        List<String> columns = asList( "one", "two", "three", "four", "five", "six", "seven", "eight", "nine", "ten" );
        String q = "start one=node(1), two=node(2), three=node(3), four=node(4), five=node(5), six=node(6), " +
                "seven=node(7), eight=node(8), nine=node(9), ten=node(10) " +
                "return one, two, three, four, five, six, seven, eight, nine, ten";
        ExecutionResult result = engine.execute( q );
        assertThat( result.toString(), matchesPattern( "one.*two.*three.*four.*five.*six.*seven.*eight.*nine.*ten" ) );

    }

    private void createTenNodes() {
        Transaction tx = db.beginTx();
        for(int i=0;i<10;i++){
            db.createNode();
        }
        tx.success();
        tx.finish();
    }

    @Test
    public void exampleConsole() throws Exception {
        Query query = CypherParser.parseConsole( "" +
                                                         //START SNIPPET: Identifier
                                                         "start n=node(0) return n.NOT_EXISTING"
                                                 //END SNIPPET: Identifier
        );

        ExecutionResult result = engine.execute( query );

        assertThat( result.columns(), hasItem( "n.NOT_EXISTING" ) );
        Iterator<Object> n_column = result.columnAs( "n.NOT_EXISTING" );
        assertNull( n_column.next() );
        assertThat( result.toString(), containsString( "null" ) );
    }

    @Test
    public void exampleWithParameterForNodeId() throws Exception {
        // START SNIPPET: exampleWithParameterForNodeId
        Map<String, Object> params = new HashMap<String, Object>();
        params.put( "id", 0 );
        ExecutionResult result = engine.execute( "start n=node({id}) return n.name", params );
        // END SNIPPET: exampleWithParameterForNodeId

        assertThat( result.columns(), hasItem( "n.name" ) );
        Iterator<Object> n_column = result.columnAs( "n.name" );
        assertEquals( "Michaela", n_column.next() );
    }

    @Test
    public void exampleWithParameterForMultipleNodeIds() throws Exception {
        // START SNIPPET: exampleWithParameterForMultipleNodeIds
        Map<String, Object> params = new HashMap<String, Object>();
        params.put( "id", Arrays.asList( 0, 1, 2 ) );
        ExecutionResult result = engine.execute( "start n=node({id}) return n.name", params );
        // END SNIPPET: exampleWithParameterForMultipleNodeIds

        assertEquals( asList( "Michaela", "Andreas", "Johan" ), this.<String>toList( result, "n.name" ) );
    }

    private <T> List<T> toList( ExecutionResult result, String column ) {
        List<T> results = new ArrayList<T>();
        IteratorUtil.addToCollection( result.<T>columnAs( column ), results );
        return results;
    }

    @Test
    public void exampleWithStringLiteralAsParameter() throws Exception {
        // START SNIPPET: exampleWithStringLiteralAsParameter
        Map<String, Object> params = new HashMap<String, Object>();
        params.put( "name", "Johan" );
        ExecutionResult result = engine.execute( "start n=node(0,1,2) where n.name = {name} return n", params );
        // END SNIPPET: exampleWithStringLiteralAsParameter

        assertEquals( asList( johanNode ), this.<Node>toList( result, "n" ) );
    }

    @Test
    public void exampleWithParametersForIndexKeyAndValue() throws Exception {
        // START SNIPPET: exampleWithParametersForIndexKeyAndValue
        Map<String, Object> params = new HashMap<String, Object>();
        params.put( "key", "name" );
        params.put( "value", "Michaela" );
        ExecutionResult result = engine.execute( "start n=node:people({key} = {value}) return n", params );
        // END SNIPPET: exampleWithParametersForIndexKeyAndValue

        assertEquals( asList( michaelaNode ), this.<Node>toList( result, "n" ) );
    }

    @Test
    public void exampleWithParametersForQuery() throws Exception {
        // START SNIPPET: exampleWithParametersForQuery
        Map<String, Object> params = new HashMap<String, Object>();
        params.put( "query", "name:Andreas" );
        ExecutionResult result = engine.execute( "start n=node:people({query}) return n", params );
        // END SNIPPET: exampleWithParametersForQuery

        assertEquals( asList( andreasNode ), this.<Node>toList( result, "n" ) );
    }

    @Test
    public void exampleWithParameterForNode() throws Exception {
        // START SNIPPET: exampleWithParameterForNode
        Map<String, Object> params = new HashMap<String, Object>();
        params.put( "node", andreasNode );
        ExecutionResult result = engine.execute( "start n=node({node}) return n.name", params );
        // END SNIPPET: exampleWithParameterForNode

        assertThat( result.columns(), hasItem( "n.name" ) );
        Iterator<Object> n_column = result.columnAs( "n.name" );
        assertEquals( "Andreas", n_column.next() );
    }

    @Test
    public void exampleWithParameterForSkipAndLimit() throws Exception {
        // START SNIPPET: exampleWithParameterForSkipLimit
        Map<String, Object> params = new HashMap<String, Object>();
        params.put( "s", 1 );
        params.put( "l", 1 );
        ExecutionResult result = engine.execute( "start n=node(0,1,2) return n.name skip {s} limit {l}", params );
        // END SNIPPET: exampleWithParameterForSkipLimit

        assertThat( result.columns(), hasItem( "n.name" ) );
        Iterator<Object> n_column = result.columnAs( "n.name" );
        assertEquals( "Andreas", n_column.next() );
    }

    private void makeFriends( Node a, Node b ) {
        Transaction tx = db.beginTx();
        a.createRelationshipTo( b, DynamicRelationshipType.withName( "friend" ) );
        tx.success();
        tx.finish();
    }
}
