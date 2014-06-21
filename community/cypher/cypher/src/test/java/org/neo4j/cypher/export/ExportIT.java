/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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

import org.junit.Test;

import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.tooling.GlobalGraphOperations;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 * This tests verifies that the output of the Exporter is valid as a whole. The dumped content is
 * executed against a clean db to assert the dump is executable. The resulting content of the db is checked merely
 * for the existance of nodes, rels, labes, indexes and constraint as further details are covered by the ExportTest
 * already.
 */
public class ExportIT
{
    @Test
    public void testDumpCanBeExecutedAgainstDb() throws Exception
    {
        // Given
        Label testLabel = DynamicLabel.label( "label" );
        GraphDatabaseService gdb = new TestGraphDatabaseFactory().newImpermanentDatabase();
        try ( Transaction tx = gdb.beginTx() )
        {
            gdb.schema().constraintFor( testLabel ).assertPropertyIsUnique( "uniqueProp" ).create();
            gdb.schema().indexFor( testLabel ).on( "prop" ).create();
            tx.success();
        }
        try ( Transaction tx = gdb.beginTx() )
        {
            Node node1 = gdb.createNode( testLabel );
            node1.setProperty( "uniqueProp", "anything" );
            Node node2 = gdb.createNode( testLabel );
            node2.setProperty( "prop", "anything" );
            node1.createRelationshipTo( node2, DynamicRelationshipType.withName( "rel" ) );
            tx.success();
        }

        SubGraph graph = DatabaseSubGraph.from( gdb );
        StringWriter out = new StringWriter();
        try ( Transaction tx = gdb.beginTx() )
        {
            new SubGraphExporter( graph ).export( new PrintWriter( out ) );
            tx.success();
        }
        String dump = out.toString();
        gdb.shutdown();

        // When
        gdb = new TestGraphDatabaseFactory().newImpermanentDatabase();
        ExecutionEngine executionEngine = new ExecutionEngine( gdb );
        for(String statment : dump.split( ";" ))
        {
            executionEngine.execute( statment );
        }

        // Then
        try ( Transaction tx = gdb.beginTx() )
        {
            GlobalGraphOperations ops = GlobalGraphOperations.at( gdb );
            assertThat( ops.getAllLabels().iterator().hasNext(), is( true ) );
            assertThat( ops.getAllNodes().iterator().hasNext(), is( true ) );
            assertThat( ops.getAllRelationships().iterator().hasNext(), is( true ) );
            assertThat( gdb.schema().getConstraints().iterator().hasNext(), is( true ) );
            assertThat( gdb.schema().getIndexes().iterator().hasNext(), is( true ) );
            tx.success();
        }
        gdb.shutdown();
    }
}
