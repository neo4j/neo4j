/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Iterator;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphalgo.impl.util.PathImpl;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.test.TestGraphDatabaseFactory;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonMap;

import static org.junit.Assert.assertEquals;

public class ExportTest
{

    private final static String NL = System.getProperty( "line.separator" );
    private GraphDatabaseService gdb;

    @Before
    public void setUp() throws Exception
    {
        gdb = new TestGraphDatabaseFactory().newImpermanentDatabase();
        gdb.beginTx();
    }

    @After
    public void tearDown() throws Exception
    {
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
        gdb.getReferenceNode().setProperty( "name", "Andres" );
        assertEquals( "start _0 = node(0) with _0 " + NL +
                "set _0.`name`=\"Andres\"" + NL, doExportGraph( gdb ) );
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
        final SubGraph graph = CypherResultSubGraph.from( result, false );
        assertEquals( "create (_" + n.getId() + ")" + NL, doExportGraph( graph ) );
    }

    @Test
    public void testSingleNode() throws Exception
    {
        Node n = gdb.createNode();
        final ExecutionResult result = result( "node", n );
        final SubGraph graph = CypherResultSubGraph.from( result, false );
        assertEquals( "create (_" + n.getId() + ")" + NL, doExportGraph( graph ) );
    }

    @Test
    public void testSingleNodeWithProperties() throws Exception
    {
        Node n = gdb.createNode();
        n.setProperty( "name", "Node1" );
        n.setProperty( "age", 42 );
        final ExecutionResult result = result( "node", n );
        final SubGraph graph = CypherResultSubGraph.from( result, false );
        assertEquals( "create (_" + n.getId() + " {`age`:42, `name`:\"Node1\"})" + NL, doExportGraph( graph ) );
    }

    @Test
    public void testSingleNodeWithArrayProperties() throws Exception
    {
        Node n = gdb.createNode();
        n.setProperty( "name", new String[]{"a", "b"} );
        n.setProperty( "age", new int[]{1, 2} );
        final ExecutionResult result = result( "node", n );
        final SubGraph graph = CypherResultSubGraph.from( result, false );
        assertEquals( "create (_" + n.getId() + " {`age`:[1, 2], `name`:[\"a\", \"b\"]})" + NL, doExportGraph( graph ) );
    }

    @Test
    public void testSingleNodeLabels() throws Exception
    {
        Node n = gdb.createNode();
        n.addLabel( DynamicLabel.label( "Foo" ) );
        n.addLabel( DynamicLabel.label( "Bar" ) );
        final ExecutionResult result = result( "node", n );
        final SubGraph graph = CypherResultSubGraph.from( result, false );
        assertEquals( "create (_" + n.getId() + ":`Foo`:`Bar`)" + NL, doExportGraph( graph ) );
    }

    @Test
    public void testExportIndex() throws Exception
    {
        gdb.schema().indexFor( DynamicLabel.label( "Foo" ) ).on( "bar" ).create();
        final SubGraph graph = DatabaseSubGraph.from( gdb );
        SubGraphExporter exporter = new SubGraphExporter( graph );
        assertEquals( asList( "create index on :`Foo`(`bar`)" ), exporter.exportIndexes() );
    }

    @Test
    public void testFromRelCypherResult() throws Exception
    {
        Node n = gdb.getReferenceNode();
        final Relationship rel = n.createRelationshipTo( n, DynamicRelationshipType.withName( "REL" ) );
        final ExecutionResult result = result( "rel", rel );
        final SubGraph graph = CypherResultSubGraph.from( result, true );
        assertEquals( "start _0 = node(0) with _0 " + NL +
                "create _0-[:`REL`]->_0" + NL, doExportGraph( graph ) );
    }

    @Test
    public void testFromPathCypherResult() throws Exception
    {
        Node n1 = gdb.getReferenceNode();
        Node n2 = gdb.createNode();
        final Relationship rel = n1.createRelationshipTo( n2, DynamicRelationshipType.withName( "REL" ) );
        final Path path = new PathImpl.Builder( n1 ).push( rel ).build();
        final ExecutionResult result = result( "path", path );
        final SubGraph graph = CypherResultSubGraph.from( result, true );
        assertEquals( "start _0 = node(0) with _0 " + NL +
                "create (_1)" + NL +
                "create _0-[:`REL`]->_1" + NL, doExportGraph( graph ) );
    }

    @SuppressWarnings("unchecked")
    private ExecutionResult result( String column, Object value )
    {
        ExecutionResult result = Mockito.mock( ExecutionResult.class );
        Mockito.when( result.columns() ).thenReturn( asList( column ) );
        final Iterator<Map<String,Object>> inner = asList( singletonMap( column, value ) ).iterator();

        ResourceIterator<Map<String, Object>> iterator = new ResourceIterator<Map<String, Object>>()
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
        assertEquals( "create (_" + n0.getId() + ")" + NL +
                "create (_" + n1.getId() + " {`name`:\"Node1\"})" + NL +
                "create _" + n0.getId() + "-[:`REL` {`related`:true}]->_" + n1.getId() + NL, doExportGraph( graph ) );
    }
}