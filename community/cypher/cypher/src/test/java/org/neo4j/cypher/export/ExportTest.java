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
package org.neo4j.cypher.export;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Iterator;
import java.util.Map;

import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphalgo.impl.util.PathImpl;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.TestGraphDatabaseFactory;

import static java.lang.System.lineSeparator;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonMap;
import static org.junit.Assert.assertEquals;

public class ExportTest
{

    private GraphDatabaseService gdb;
    private Transaction tx;

    @Before
    public void setUp() throws Exception
    {
        gdb = new TestGraphDatabaseFactory().newImpermanentDatabase();
        tx = gdb.beginTx();
    }

    @After
    public void tearDown() throws Exception
    {
        tx.close();
        gdb.shutdown();
    }

    @Test
    public void testEmptyGraph() throws Exception
    {
        assertEquals( "", doExportGraph( gdb ) );
    }

    @Test
    public void testNodeWithProperties() throws Exception
    {
        gdb.createNode().setProperty( "name", "Andres" );
        assertEquals( "create (_0 {`name`:\"Andres\"})" + lineSeparator(), doExportGraph( gdb ) );
    }

    @Test
    public void testNodeWithFloatProperty() throws Exception
    {
        final float floatValue = 10.1f;
        final String expected = "10.100000";
        gdb.createNode().setProperty( "float", floatValue );
        assertEquals( "create (_0 {`float`:" + expected + "})" + lineSeparator(), doExportGraph( gdb ) );
    }

    @Test
    public void testNodeWithDoubleProperty() throws Exception
    {
        final double doubleValue = 123456.123456;
        final String expected = "123456.123456";
        gdb.createNode().setProperty( "double", doubleValue );
        assertEquals( "create (_0 {`double`:" + expected + "})" + lineSeparator(), doExportGraph( gdb ) );
    }

    private String doExportGraph( GraphDatabaseService db )
    {
        SubGraph graph = DatabaseSubGraph.from( db );
        return doExportGraph( graph );
    }

    private String doExportGraph( SubGraph graph )
    {
        StringWriter out = new StringWriter();
        new SubGraphExporter( graph ).export( new PrintWriter( out ) );
        return out.toString();
    }

    @Test
    public void testFromSimpleCypherResult() throws Exception
    {
        Node n = gdb.createNode();
        final ExecutionResult result = result( "node", n );
        final SubGraph graph = CypherResultSubGraph.from( result, gdb, false );
        assertEquals( "create (_" + n.getId() + ")" + lineSeparator(), doExportGraph( graph ) );
    }

    @Test
    public void testSingleNode() throws Exception
    {
        Node n = gdb.createNode();
        final ExecutionResult result = result( "node", n );
        final SubGraph graph = CypherResultSubGraph.from( result, gdb, false );
        assertEquals( "create (_" + n.getId() + ")" + lineSeparator(), doExportGraph( graph ) );
    }

    @Test
    public void testSingleNodeWithProperties() throws Exception
    {
        Node n = gdb.createNode();
        n.setProperty( "name", "Node1" );
        n.setProperty( "age", 42 );
        final ExecutionResult result = result( "node", n );
        final SubGraph graph = CypherResultSubGraph.from( result, gdb, false );
        assertEquals( "create (_" + n.getId() + " {`age`:42, `name`:\"Node1\"})" + lineSeparator(), doExportGraph( graph ) );
    }

    @Test
    public void testEscapingOfNodeStringPropertyValue() throws Exception
    {
        Node n = gdb.createNode();
        n.setProperty( "name", "Brutus \"Brutal\" Howell" );
        final ExecutionResult result = result( "node", n );
        final SubGraph graph = CypherResultSubGraph.from( result, gdb, false );
        assertEquals( "create (_" + n.getId() + " {`name`:\"Brutus \\\"Brutal\\\" Howell\"})" + lineSeparator(),
                doExportGraph( graph ) );
    }

    @Test
    public void testEscapingOfNodeStringArrayPropertyValue() throws Exception
    {
        Node n = gdb.createNode();
        n.setProperty( "name", new String[]{"Brutus \"Brutal\" Howell", "Dr."} );
        final ExecutionResult result = result( "node", n );
        final SubGraph graph = CypherResultSubGraph.from( result, gdb, false );
        assertEquals( "create (_" + n.getId() + " {`name`:[\"Brutus \\\"Brutal\\\" Howell\", \"Dr.\"]})" + lineSeparator(),
                doExportGraph( graph ) );
    }

    @Test
    public void testEscapingOfRelationshipStringPropertyValue() throws Exception
    {
        Node n = gdb.createNode();
        final Relationship rel = n.createRelationshipTo( n, DynamicRelationshipType.withName( "REL" ) );
        rel.setProperty( "name", "Brutus \"Brutal\" Howell" );
        final ExecutionResult result = result( "rel", rel );
        final SubGraph graph = CypherResultSubGraph.from( result, gdb, true );
        assertEquals( "create (_0)" + lineSeparator() +
                "create _0-[:`REL` {`name`:\"Brutus \\\"Brutal\\\" Howell\"}]->_0" + lineSeparator(), doExportGraph( graph ) );
    }

    @Test
    public void testEscapingOfRelationshipStringArrayPropertyValue() throws Exception
    {
        Node n = gdb.createNode();
        final Relationship rel = n.createRelationshipTo( n, DynamicRelationshipType.withName( "REL" ) );
        rel.setProperty( "name", new String[]{"Brutus \"Brutal\" Howell", "Dr."} );
        final ExecutionResult result = result( "rel", rel );
        final SubGraph graph = CypherResultSubGraph.from( result, gdb, true );
        assertEquals( "create (_0)" + lineSeparator() +
                "create _0-[:`REL` {`name`:[\"Brutus \\\"Brutal\\\" Howell\", \"Dr.\"]}]->_0" + lineSeparator(),
                doExportGraph( graph ) );
    }

    @Test
    public void testEscapingStringPropertyWithBackslash() throws Exception
    {
        Node n = gdb.createNode();
        n.setProperty( "name", "Some\\thing" );
        final ExecutionResult result = result( "node", n );
        final SubGraph graph = CypherResultSubGraph.from( result, gdb, false );
        assertEquals( "create (_" + n.getId() + " {`name`:\"Some\\\\thing\"})" + lineSeparator(),
                doExportGraph( graph ) );
    }

    @Test
    public void testEscapingStringPropertyWithBackslashAndDoubleQuote() throws Exception
    {
        Node n = gdb.createNode();
        n.setProperty( "name", "Some\\\"thing" );
        final ExecutionResult result = result( "node", n );
        final SubGraph graph = CypherResultSubGraph.from( result, gdb, false );
        assertEquals( "create (_" + n.getId() + " {`name`:\"Some\\\\\\\"thing\"})" + lineSeparator(),
                doExportGraph( graph ) );
    }

    @Test
    public void testSingleNodeWithArrayProperties() throws Exception
    {
        Node n = gdb.createNode();
        n.setProperty( "name", new String[]{"a", "b"} );
        n.setProperty( "age", new int[]{1, 2} );
        final ExecutionResult result = result( "node", n );
        final SubGraph graph = CypherResultSubGraph.from( result, gdb, false );
        assertEquals( "create (_" + n.getId() + " {`age`:[1, 2], `name`:[\"a\", \"b\"]})" + lineSeparator(), doExportGraph( graph ) );
    }

    @Test
    public void testSingleNodeLabels() throws Exception
    {
        Node n = gdb.createNode();
        n.addLabel( DynamicLabel.label( "Foo" ) );
        n.addLabel( DynamicLabel.label( "Bar" ) );
        final ExecutionResult result = result( "node", n );
        final SubGraph graph = CypherResultSubGraph.from( result, gdb, false );
        assertEquals( "create (_" + n.getId() + ":`Foo`:`Bar`)" + lineSeparator(), doExportGraph( graph ) );
    }

    @Test
    public void testExportIndex() throws Exception
    {
        gdb.schema().indexFor( DynamicLabel.label( "Foo" ) ).on( "bar" ).create();
        assertEquals( "create index on :`Foo`(`bar`)" + lineSeparator() , doExportGraph( gdb ) );
    }

    @Test
    public void testExportUniquenessConstraint() throws Exception
    {
        gdb.schema().constraintFor( DynamicLabel.label( "Foo" ) ).assertPropertyIsUnique( "bar" ).create();
        assertEquals( "create constraint on (n:`Foo`) assert n.`bar` is unique" + lineSeparator(), doExportGraph( gdb ) );
    }

    @Test
    public void testExportIndexesViaCypherResult() throws Exception
    {
        final Label label = DynamicLabel.label( "Foo" );
        gdb.schema().indexFor( label ).on( "bar" ).create();
        gdb.schema().indexFor( label ).on( "bar2" ).create();
        commitAndStartNewTransactionAfterSchemaChanges();
        Node n = gdb.createNode( label );
        final ExecutionResult result = result( "node", n );
        final SubGraph graph = CypherResultSubGraph.from( result, gdb, true );
        assertEquals( "create index on :`Foo`(`bar2`)" + lineSeparator() +
                "create index on :`Foo`(`bar`)" + lineSeparator() +
                "create (_0:`Foo`)" + lineSeparator(), doExportGraph( graph ) );
    }

    @Test
    public void testExportConstraintsViaCypherResult() throws Exception
    {
        final Label label = DynamicLabel.label( "Foo" );
        gdb.schema().constraintFor( label ).assertPropertyIsUnique( "bar" ).create();
        gdb.schema().constraintFor( label ).assertPropertyIsUnique( "bar2" ).create();
        commitAndStartNewTransactionAfterSchemaChanges();
        Node n = gdb.createNode( label );
        final ExecutionResult result = result( "node", n );
        final SubGraph graph = CypherResultSubGraph.from( result, gdb, true );
        assertEquals( "create constraint on (n:`Foo`) assert n.`bar2` is unique" + lineSeparator() +
                "create constraint on (n:`Foo`) assert n.`bar` is unique" + lineSeparator() +
                "create (_0:`Foo`)" + lineSeparator(), doExportGraph( graph ) );
    }

    private void commitAndStartNewTransactionAfterSchemaChanges()
    {
        tx.success();
        tx.close();
        tx = gdb.beginTx();
    }

    @Test
    public void testFromRelCypherResult() throws Exception
    {
        Node n = gdb.createNode();
        final Relationship rel = n.createRelationshipTo( n, DynamicRelationshipType.withName( "REL" ) );
        final ExecutionResult result = result( "rel", rel );
        final SubGraph graph = CypherResultSubGraph.from( result, gdb, true );
        assertEquals( "create (_0)" + lineSeparator() +
                "create _0-[:`REL`]->_0" + lineSeparator(), doExportGraph( graph ) );
    }

    @Test
    public void testFromPathCypherResult() throws Exception
    {
        Node n1 = gdb.createNode();
        Node n2 = gdb.createNode();
        final Relationship rel = n1.createRelationshipTo( n2, DynamicRelationshipType.withName( "REL" ) );
        final Path path = new PathImpl.Builder( n1 ).push( rel ).build();
        final ExecutionResult result = result( "path", path );
        final SubGraph graph = CypherResultSubGraph.from( result, gdb, true );
        assertEquals( "create (_0)" + lineSeparator() +
                "create (_1)" + lineSeparator() +
                "create _0-[:`REL`]->_1" + lineSeparator(), doExportGraph( graph ) );
    }

    @SuppressWarnings("unchecked")
    private ExecutionResult result( String column, Object value )
    {
        ExecutionResult result = Mockito.mock( ExecutionResult.class );
        Mockito.when( result.columns() ).thenReturn( asList( column ) );
        final Iterator<Map<String, Object>> inner = asList( singletonMap( column, value ) ).iterator();

        final ResourceIterator<Map<String, Object>> iterator = new ResourceIterator<Map<String, Object>>()
        {
            @Override
            public void close()
            {
            }

            @Override
            public boolean hasNext()
            {
                return inner.hasNext();
            }

            @Override
            public Map<String, Object> next()
            {
                return inner.next();
            }

            @Override
            public void remove()
            {
                inner.remove();
            }
        };

        Mockito.when( result.iterator() ).thenReturn( iterator );
        Mockito.when( result.hasNext() ).thenAnswer( new Answer<Boolean>()
        {
            @Override
            public Boolean answer( InvocationOnMock invocation ) throws Throwable
            {
                return iterator.hasNext();
            }
        } );
        Mockito.when( result.next() ).thenAnswer( new Answer<Map<String,Object>>()
        {
            @Override
            public Map<String, Object> answer( InvocationOnMock invocation ) throws Throwable
            {
                return iterator.next();
            }
        } );
        return result;
    }

    @Test
    public void testFromSimpleGraph() throws Exception
    {
        final Node n0 = gdb.createNode();

        final Node n1 = gdb.createNode();
        n1.setProperty( "name", "Node1" );
        final Relationship relationship = n0.createRelationshipTo( n1, DynamicRelationshipType.withName( "REL" ) );
        relationship.setProperty( "related", true );
        final SubGraph graph = DatabaseSubGraph.from( gdb );
        assertEquals( "create (_" + n0.getId() + ")" + lineSeparator() +
                "create (_" + n1.getId() + " {`name`:\"Node1\"})" + lineSeparator() +
                "create _" + n0.getId() + "-[:`REL` {`related`:true}]->_" + n1.getId() + lineSeparator(), doExportGraph( graph ) );
    }
}
