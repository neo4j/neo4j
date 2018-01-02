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
package org.neo4j.kernel.counts;

import java.util.concurrent.Future;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.function.Supplier;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.test.Barrier;
import org.neo4j.test.DatabaseRule;
import org.neo4j.test.ImpermanentDatabaseRule;
import org.neo4j.test.NamedFunction;
import org.neo4j.test.ThreadingRule;

import static org.junit.Assert.assertEquals;

public class NodeCountsTest
{
    public final @Rule DatabaseRule db = new ImpermanentDatabaseRule();
    public final @Rule ThreadingRule threading = new ThreadingRule();

    @Test
    public void shouldReportNumberOfNodesInAnEmptyGraph() throws Exception
    {
        // when
        long nodeCount = numberOfNodes();

        // then
        assertEquals( 0, nodeCount );
    }

    @Test
    public void shouldReportNumberOfNodes() throws Exception
    {
        // given
        GraphDatabaseService graphDb = db.getGraphDatabaseService();
        try ( Transaction tx = graphDb.beginTx() )
        {
            graphDb.createNode();
            graphDb.createNode();
            tx.success();
        }

        // when
        long nodeCount = numberOfNodes();

        // then
        assertEquals( 2, nodeCount );
    }

    @Test
    public void shouldReportAccurateNumberOfNodesAfterDeletion() throws Exception
    {
        // given
        GraphDatabaseService graphDb = db.getGraphDatabaseService();
        Node one;
        try ( Transaction tx = graphDb.beginTx() )
        {
            one = graphDb.createNode();
            graphDb.createNode();
            tx.success();
        }
        try ( Transaction tx = graphDb.beginTx() )
        {
            one.delete();
            tx.success();
        }

        // when
        long nodeCount = numberOfNodes();

        // then
        assertEquals( 1, nodeCount );
    }

    @Test
    @Ignore("TODO: reenable this test when we can etract proper counts form TxState")
    public void shouldIncludeNumberOfNodesAddedInTransaction() throws Exception
    {
        // given
        GraphDatabaseService graphDb = db.getGraphDatabaseService();
        try ( Transaction tx = graphDb.beginTx() )
        {
            graphDb.createNode();
            graphDb.createNode();
            tx.success();
        }
        long before = numberOfNodes();

        try ( Transaction tx = graphDb.beginTx() )
        {
            // when
            graphDb.createNode();
            long nodeCount = countsForNode();

            // then
            assertEquals( before + 1, nodeCount );
            tx.success();
        }
    }

    @Test
    @Ignore("TODO: reenable this test when we can etract proper counts form TxState")
    public void shouldIncludeNumberOfNodesDeletedInTransaction() throws Exception
    {
        // given
        GraphDatabaseService graphDb = db.getGraphDatabaseService();
        Node one;
        try ( Transaction tx = graphDb.beginTx() )
        {
            one = graphDb.createNode();
            graphDb.createNode();
            tx.success();
        }
        long before = numberOfNodes();

        try ( Transaction tx = graphDb.beginTx() )
        {
            // when
            one.delete();
            long nodeCount = countsForNode();

            // then
            assertEquals( before - 1, nodeCount );
            tx.success();
        }
    }

    @Test
    public void shouldNotSeeNodeCountsOfOtherTransaction() throws Exception
    {
        // given
        GraphDatabaseService graphDb = db.getGraphDatabaseService();
        final Barrier.Control barrier = new Barrier.Control();
        long before = numberOfNodes();
        Future<Long> done = threading.execute( new NamedFunction<GraphDatabaseService, Long>( "create-nodes" )
        {
            @Override
            public Long apply( GraphDatabaseService graphDb )
            {
                try ( Transaction tx = graphDb.beginTx() )
                {
                    graphDb.createNode();
                    graphDb.createNode();
                    barrier.reached();
                    long during = countsForNode();
                    tx.success();
                    return during;
                }
            }
        }, graphDb );
        barrier.await();

        // when
        long nodes = numberOfNodes();
        barrier.release();
        long during = done.get();
        long after = numberOfNodes();

        // then
        assertEquals( 0, before );
        assertEquals( 0, nodes );
        assertEquals( before, during );
        assertEquals( 2, after );
    }

    /** Transactional version of {@link #countsForNode()} */
    private long numberOfNodes()
    {
        try ( Transaction tx = db.getGraphDatabaseService().beginTx() )
        {
            long nodeCount = countsForNode();
            tx.success();
            return nodeCount;
        }
    }

    private long countsForNode()
    {
        return statementSupplier.get().readOperations().countsForNode( ReadOperations.ANY_LABEL );
    }

    private Supplier<Statement> statementSupplier;

    @Before
    public void exposeGuts()
    {
        statementSupplier = db.getGraphDatabaseAPI().getDependencyResolver()
                              .resolveDependency( ThreadToStatementContextBridge.class );
    }
}
