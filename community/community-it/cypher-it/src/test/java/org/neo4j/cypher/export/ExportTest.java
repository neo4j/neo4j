/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Iterator;
import java.util.Map;

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphalgo.impl.util.PathImpl;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

import static java.lang.System.lineSeparator;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

class ExportTest
{

    private GraphDatabaseService gdb;
    private Transaction tx;
    private DatabaseManagementService managementService;

    @BeforeEach
    void setUp()
    {
        managementService = new TestDatabaseManagementServiceBuilder().impermanent().build();
        gdb = managementService.database( DEFAULT_DATABASE_NAME );
        tx = gdb.beginTx();
    }

    @AfterEach
    void tearDown()
    {
        tx.close();
        managementService.shutdown();
    }

    @Test
    void testEmptyGraph()
    {
        assertEquals( "", doExportGraph( gdb ) );
    }

    @Test
    void testNodeWithProperties()
    {
        gdb.createNode().setProperty( "name", "Andres" );
        assertEquals( "create (_0 {`name`:\"Andres\"})" + lineSeparator() + ";" + lineSeparator(), doExportGraph( gdb ) );
    }

    @Test
    void testNodeWithFloatProperty()
    {
        final float floatValue = 10.1f;
        final String expected = "10.100000";
        gdb.createNode().setProperty( "float", floatValue );
        assertEquals( "create (_0 {`float`:" + expected + "})" + lineSeparator() + ";" + lineSeparator(), doExportGraph( gdb ) );
    }

    @Test
    void testNodeWithDoubleProperty()
    {
        final double doubleValue = 123456.123456;
        final String expected = "123456.123456";
        gdb.createNode().setProperty( "double", doubleValue );
        assertEquals( "create (_0 {`double`:" + expected + "})" + lineSeparator() + ";" + lineSeparator(), doExportGraph( gdb ) );
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
    void testFromSimpleCypherResult()
    {
        Node n = gdb.createNode();
        final Result result = result( "node", n );
        final SubGraph graph = CypherResultSubGraph.from( result, gdb, false );
        assertEquals( "create (_" + n.getId() + ")" + lineSeparator() + ";" + lineSeparator(), doExportGraph( graph ) );
    }

    @Test
    void testSingleNode()
    {
        Node n = gdb.createNode();
        final Result result = result( "node", n );
        final SubGraph graph = CypherResultSubGraph.from( result, gdb, false );
        assertEquals( "create (_" + n.getId() + ")" + lineSeparator() + ";" + lineSeparator(), doExportGraph( graph ) );
    }

    @Test
    void testSingleNodeWithProperties()
    {
        Node n = gdb.createNode();
        n.setProperty( "name", "Node1" );
        n.setProperty( "age", 42 );
        final Result result = result( "node", n );
        final SubGraph graph = CypherResultSubGraph.from( result, gdb, false );
        assertEquals( "create (_" + n.getId() + " {`age`:42, `name`:\"Node1\"})" +
                lineSeparator() + ";" + lineSeparator(), doExportGraph( graph ) );
    }

    @Test
    void testEscapingOfNodeStringPropertyValue()
    {
        Node n = gdb.createNode();
        n.setProperty( "name", "Brutus \"Brutal\" Howell" );
        final Result result = result( "node", n );
        final SubGraph graph = CypherResultSubGraph.from( result, gdb, false );
        assertEquals( "create (_" + n.getId() + " {`name`:\"Brutus \\\"Brutal\\\" Howell\"})" +
                        lineSeparator() + ";" + lineSeparator(),
                doExportGraph( graph ) );
    }

    @Test
    void testEscapingOfNodeStringArrayPropertyValue()
    {
        Node n = gdb.createNode();
        n.setProperty( "name", new String[]{"Brutus \"Brutal\" Howell", "Dr."} );
        final Result result = result( "node", n );
        final SubGraph graph = CypherResultSubGraph.from( result, gdb, false );
        assertEquals( "create (_" + n.getId() + " {`name`:[\"Brutus \\\"Brutal\\\" Howell\", \"Dr.\"]})" +
                        lineSeparator() + ";" + lineSeparator(),
                doExportGraph( graph ) );
    }

    @Test
    void testEscapingOfRelationshipStringPropertyValue()
    {
        Node n = gdb.createNode();
        final Relationship rel = n.createRelationshipTo( n, RelationshipType.withName( "REL" ) );
        rel.setProperty( "name", "Brutus \"Brutal\" Howell" );
        final Result result = result( "rel", rel );
        final SubGraph graph = CypherResultSubGraph.from( result, gdb, true );
        assertEquals( "create (_0)" + lineSeparator() +
                "create (_0)-[:`REL` {`name`:\"Brutus \\\"Brutal\\\" Howell\"}]->(_0)" + lineSeparator() + ";" +
                lineSeparator(), doExportGraph( graph ) );
    }

    @Test
    void testEscapingOfRelationshipStringArrayPropertyValue()
    {
        Node n = gdb.createNode();
        final Relationship rel = n.createRelationshipTo( n, RelationshipType.withName( "REL" ) );
        rel.setProperty( "name", new String[]{"Brutus \"Brutal\" Howell", "Dr."} );
        final Result result = result( "rel", rel );
        final SubGraph graph = CypherResultSubGraph.from( result, gdb, true );
        assertEquals( "create (_0)" + lineSeparator() +
                "create (_0)-[:`REL` {`name`:[\"Brutus \\\"Brutal\\\" Howell\", \"Dr.\"]}]->(_0)" + lineSeparator() + ";" +
                        lineSeparator(),
                doExportGraph( graph ) );
    }

    @Test
    void testEscapingStringPropertyWithBackslash()
    {
        Node n = gdb.createNode();
        n.setProperty( "name", "Some\\thing" );
        final Result result = result( "node", n );
        final SubGraph graph = CypherResultSubGraph.from( result, gdb, false );
        assertEquals( "create (_" + n.getId() + " {`name`:\"Some\\\\thing\"})" + lineSeparator() + ";" +
                        lineSeparator(),
                doExportGraph( graph ) );
    }

    @Test
    void testEscapingStringPropertyWithBackslashAndDoubleQuote()
    {
        Node n = gdb.createNode();
        n.setProperty( "name", "Some\\\"thing" );
        final Result result = result( "node", n );
        final SubGraph graph = CypherResultSubGraph.from( result, gdb, false );
        assertEquals( "create (_" + n.getId() + " {`name`:\"Some\\\\\\\"thing\"})" + lineSeparator() + ";" +
                        lineSeparator(),
                doExportGraph( graph ) );
    }

    @Test
    void testSingleNodeWithArrayProperties()
    {
        Node n = gdb.createNode();
        n.setProperty( "name", new String[]{"a", "b"} );
        n.setProperty( "age", new int[]{1, 2} );
        final Result result = result( "node", n );
        final SubGraph graph = CypherResultSubGraph.from( result, gdb, false );
        assertEquals( "create (_" + n.getId() + " {`age`:[1, 2], `name`:[\"a\", \"b\"]})" + lineSeparator() + ";" +
                lineSeparator(), doExportGraph( graph ) );
    }

    @Test
    void testSingleNodeLabels()
    {
        Node n = gdb.createNode();
        n.addLabel( Label.label( "Foo" ) );
        n.addLabel( Label.label( "Bar" ) );
        final Result result = result( "node", n );
        final SubGraph graph = CypherResultSubGraph.from( result, gdb, false );
        assertEquals( "create (_" + n.getId() + ":`Foo`:`Bar`)" + lineSeparator() + ";" + lineSeparator(),
                doExportGraph( graph ) );
    }

    @Test
    void testExportIndex()
    {
        gdb.schema().indexFor( Label.label( "Foo" ) ).on( "bar" ).create();
        assertEquals( "create index on :`Foo`(`bar`);" + lineSeparator() , doExportGraph( gdb ) );
    }

    @Test
    void testExportUniquenessConstraint()
    {
        gdb.schema().constraintFor( Label.label( "Foo" ) ).assertPropertyIsUnique( "bar" ).create();
        assertEquals( "create constraint on (n:`Foo`) assert n.`bar` is unique;" + lineSeparator(),
                doExportGraph( gdb ) );
    }

    @Test
    void testExportIndexesViaCypherResult()
    {
        final Label label = Label.label( "Foo" );
        gdb.schema().indexFor( label ).on( "bar" ).create();
        gdb.schema().indexFor( label ).on( "bar2" ).create();
        commitAndStartNewTransactionAfterSchemaChanges();
        Node n = gdb.createNode( label );
        final Result result = result( "node", n );
        final SubGraph graph = CypherResultSubGraph.from( result, gdb, true );
        assertEquals( "create index on :`Foo`(`bar2`);" + lineSeparator() +
                "create index on :`Foo`(`bar`);" + lineSeparator() +
                "create (_0:`Foo`)" + lineSeparator() + ";" + lineSeparator(), doExportGraph( graph ) );
    }

    @Test
    void testExportConstraintsViaCypherResult()
    {
        final Label label = Label.label( "Foo" );
        gdb.schema().constraintFor( label ).assertPropertyIsUnique( "bar" ).create();
        gdb.schema().constraintFor( label ).assertPropertyIsUnique( "bar2" ).create();
        commitAndStartNewTransactionAfterSchemaChanges();
        Node n = gdb.createNode( label );
        final Result result = result( "node", n );
        final SubGraph graph = CypherResultSubGraph.from( result, gdb, true );
        assertEquals( "create constraint on (n:`Foo`) assert n.`bar2` is unique;" + lineSeparator() +
                "create constraint on (n:`Foo`) assert n.`bar` is unique;" + lineSeparator() +
                "create (_0:`Foo`)" + lineSeparator() + ";" + lineSeparator(), doExportGraph( graph ) );
    }

    private void commitAndStartNewTransactionAfterSchemaChanges()
    {
        tx.commit();
        tx = gdb.beginTx();
    }

    @Test
    void testFromRelCypherResult()
    {
        Node n = gdb.createNode();
        final Relationship rel = n.createRelationshipTo( n, RelationshipType.withName( "REL" ) );
        final Result result = result( "rel", rel );
        final SubGraph graph = CypherResultSubGraph.from( result, gdb, true );
        assertEquals( "create (_0)" + lineSeparator() +
                "create (_0)-[:`REL`]->(_0)" + lineSeparator() + ";" + lineSeparator(), doExportGraph( graph ) );
    }

    @Test
    void testFromPathCypherResult()
    {
        Node n1 = gdb.createNode();
        Node n2 = gdb.createNode();
        final Relationship rel = n1.createRelationshipTo( n2, RelationshipType.withName( "REL" ) );
        final Path path = new PathImpl.Builder( n1 ).push( rel ).build();
        final Result result = result( "path", path );
        final SubGraph graph = CypherResultSubGraph.from( result, gdb, true );
        assertEquals( "create (_0)" + lineSeparator() +
                "create (_1)" + lineSeparator() +
                "create (_0)-[:`REL`]->(_1)" + lineSeparator() + ";" + lineSeparator(), doExportGraph( graph ) );
    }

    private static Result result( String column, Object value )
    {
        Result result = Mockito.mock( Result.class );
        Mockito.when( result.columns() ).thenReturn( singletonList( column ) );
        final Iterator<Map<String, Object>> inner = singletonList( singletonMap( column, value ) ).iterator();

        final ResourceIterator<Map<String, Object>> iterator = new ResourceIterator<>()
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
            public Map<String,Object> next()
            {
                return inner.next();
            }

            @Override
            public void remove()
            {
                inner.remove();
            }
        };

        Mockito.when( result.hasNext() ).thenAnswer( invocation -> iterator.hasNext() );
        Mockito.when( result.next() ).thenAnswer( invocation -> iterator.next() );
        return result;
    }

    @Test
    void testFromSimpleGraph()
    {
        final Node n0 = gdb.createNode();

        final Node n1 = gdb.createNode();
        n1.setProperty( "name", "Node1" );
        final Relationship relationship = n0.createRelationshipTo( n1, RelationshipType.withName( "REL" ) );
        relationship.setProperty( "related", true );
        final SubGraph graph = DatabaseSubGraph.from( gdb );
        assertEquals( "create (_" + n0.getId() + ")" + lineSeparator() +
                "create (_" + n1.getId() + " {`name`:\"Node1\"})" + lineSeparator() +
                "create (_" + n0.getId() + ")-[:`REL` {`related`:true}]->(_" + n1.getId() + ")" +
                    lineSeparator() + ";" + lineSeparator(), doExportGraph( graph ) );
    }
}
