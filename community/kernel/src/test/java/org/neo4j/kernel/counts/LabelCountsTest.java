/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.counts;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.function.Supplier;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.impl.api.operations.KeyReadOperations;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.test.rule.DatabaseRule;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

import static org.junit.Assert.assertEquals;
import static org.neo4j.graphdb.Label.label;

public class LabelCountsTest
{
    @Rule
    public final DatabaseRule db = new ImpermanentDatabaseRule();

    private Supplier<Statement> statementSupplier;

    @Before
    public void exposeGuts()
    {
        statementSupplier = db.getGraphDatabaseAPI().getDependencyResolver()
                .resolveDependency( ThreadToStatementContextBridge.class );
    }

    @Test
    public void shouldGetNumberOfNodesWithLabel() throws Exception
    {
        // given
        GraphDatabaseService graphDb = db.getGraphDatabaseAPI();
        try ( Transaction tx = graphDb.beginTx() )
        {
            graphDb.createNode( label( "Foo" ) );
            graphDb.createNode( label( "Bar" ) );
            graphDb.createNode( label( "Bar" ) );

            tx.success();
        }

        // when
        long fooCount = numberOfNodesWith( label( "Foo" ) );
        long barCount = numberOfNodesWith( label( "Bar" ) );

        // then
        assertEquals( 1, fooCount );
        assertEquals( 2, barCount );
    }

    @Test
    public void shouldAccountForDeletedNodes() throws Exception
    {
        // given
        GraphDatabaseService graphDb = db.getGraphDatabaseAPI();
        Node node;
        try ( Transaction tx = graphDb.beginTx() )
        {
            node = graphDb.createNode( label( "Foo" ) );
            graphDb.createNode( label( "Foo" ) );

            tx.success();
        }
        try ( Transaction tx = graphDb.beginTx() )
        {
            node.delete();

            tx.success();
        }

        // when
        long fooCount = numberOfNodesWith( label( "Foo" ) );

        // then
        assertEquals( 1, fooCount );
    }

    @Test
    public void shouldAccountForDeletedNodesWithMultipleLabels() throws Exception
    {
        // given
        GraphDatabaseService graphDb = db.getGraphDatabaseAPI();
        Node node;
        try ( Transaction tx = graphDb.beginTx() )
        {
            node = graphDb.createNode( label( "Foo" ), label( "Bar" ) );
            graphDb.createNode( label( "Foo" ) );
            graphDb.createNode( label( "Bar" ) );

            tx.success();
        }
        try ( Transaction tx = graphDb.beginTx() )
        {
            node.delete();

            tx.success();
        }

        // when
        long fooCount = numberOfNodesWith( label( "Foo" ) );
        long barCount = numberOfNodesWith( label( "Bar" ) );

        // then
        assertEquals( 1, fooCount );
        assertEquals( 1, barCount );
    }

    @Test
    public void shouldAccountForAddedLabels() throws Exception
    {
        // given
        GraphDatabaseService graphDb = db.getGraphDatabaseAPI();
        Node n1;
        Node n2;
        Node n3;
        try ( Transaction tx = graphDb.beginTx() )
        {
            n1 = graphDb.createNode( label( "Foo" ) );
            n2 = graphDb.createNode();
            n3 = graphDb.createNode();

            tx.success();
        }
        try ( Transaction tx = graphDb.beginTx() )
        {
            n1.addLabel( label( "Bar" ) );
            n2.addLabel( label( "Bar" ) );
            n3.addLabel( label( "Foo" ) );

            tx.success();
        }

        // when
        long fooCount = numberOfNodesWith( label( "Foo" ) );
        long barCount = numberOfNodesWith( label( "Bar" ) );

        // then
        assertEquals( 2, fooCount );
        assertEquals( 2, barCount );
    }

    @Test
    public void shouldAccountForRemovedLabels() throws Exception
    {
        // given
        GraphDatabaseService graphDb = db.getGraphDatabaseAPI();
        Node n1;
        Node n2;
        Node n3;
        try ( Transaction tx = graphDb.beginTx() )
        {
            n1 = graphDb.createNode( label( "Foo" ), label( "Bar" ) );
            n2 = graphDb.createNode( label( "Bar" ) );
            n3 = graphDb.createNode( label( "Foo" ) );

            tx.success();
        }
        try ( Transaction tx = graphDb.beginTx() )
        {
            n1.removeLabel( label( "Bar" ) );
            n2.removeLabel( label( "Bar" ) );
            n3.removeLabel( label( "Foo" ) );

            tx.success();
        }

        // when
        long fooCount = numberOfNodesWith( label( "Foo" ) );
        long barCount = numberOfNodesWith( label( "Bar" ) );

        // then
        assertEquals( 1, fooCount );
        assertEquals( 0, barCount );
    }

    /** Transactional version of {@link #countsForNode(Label)} */
    private long numberOfNodesWith( Label label )
    {
        try ( Transaction tx = db.getGraphDatabaseAPI().beginTx() )
        {
            long nodeCount = countsForNode( label );
            tx.success();
            return nodeCount;
        }
    }

    /** @param label the label to get the number of nodes of, or {@code null} to get the total number of nodes. */
    private long countsForNode( Label label )
    {
        try ( Statement statement = statementSupplier.get() )
        {
            ReadOperations read = statement.readOperations();
            int labelId;
            if ( label == null )
            {
                labelId = ReadOperations.ANY_LABEL;
            }
            else
            {
                if ( KeyReadOperations.NO_SUCH_LABEL == (labelId = read.labelGetForName( label.name() )) )
                {
                    return 0;
                }
            }
            return read.countsForNode( labelId );
        }
    }

}
