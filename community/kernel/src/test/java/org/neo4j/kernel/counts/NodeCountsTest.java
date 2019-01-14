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
package org.neo4j.kernel.counts;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.concurrent.Future;
import java.util.function.Supplier;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.StatementConstants;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.test.Barrier;
import org.neo4j.test.NamedFunction;
import org.neo4j.test.rule.DatabaseRule;
import org.neo4j.test.rule.ImpermanentDatabaseRule;
import org.neo4j.test.rule.concurrent.ThreadingRule;

import static org.junit.Assert.assertEquals;

public class NodeCountsTest
{
    @Rule
    public final DatabaseRule db = new ImpermanentDatabaseRule();
    @Rule
    public final ThreadingRule threading = new ThreadingRule();

    private Supplier<KernelTransaction> kernelTransactionSupplier;

    @Before
    public void setUp()
    {
        kernelTransactionSupplier = () -> db.getGraphDatabaseAPI().getDependencyResolver()
                .resolveDependency( ThreadToStatementContextBridge.class ).getKernelTransactionBoundToThisThread( true );
    }

    @Test
    public void shouldReportNumberOfNodesInAnEmptyGraph()
    {
        // when
        long nodeCount = numberOfNodes();

        // then
        assertEquals( 0, nodeCount );
    }

    @Test
    public void shouldReportNumberOfNodes()
    {
        // given
        GraphDatabaseService graphDb = db.getGraphDatabaseAPI();
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
    public void shouldReportAccurateNumberOfNodesAfterDeletion()
    {
        // given
        GraphDatabaseService graphDb = db.getGraphDatabaseAPI();
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
    public void shouldIncludeNumberOfNodesAddedInTransaction()
    {
        // given
        GraphDatabaseService graphDb = db.getGraphDatabaseAPI();
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
    public void shouldIncludeNumberOfNodesDeletedInTransaction()
    {
        // given
        GraphDatabaseService graphDb = db.getGraphDatabaseAPI();
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
        GraphDatabaseService graphDb = db.getGraphDatabaseAPI();
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
                    long whatThisThreadSees = countsForNode();
                    tx.success();
                    return whatThisThreadSees;
                }
            }
        }, graphDb );
        barrier.await();

        // when
        long during = numberOfNodes();
        barrier.release();
        long whatOtherThreadSees = done.get();
        long after = numberOfNodes();

        // then
        assertEquals( 0, before );
        assertEquals( 0, during );
        assertEquals( after, whatOtherThreadSees );
        assertEquals( 2, after );
    }

    /** Transactional version of {@link #countsForNode()} */
    private long numberOfNodes()
    {
        try ( Transaction tx = db.getGraphDatabaseAPI().beginTx() )
        {
            long nodeCount = countsForNode();
            tx.success();
            return nodeCount;
        }
    }

    private long countsForNode()
    {
        return kernelTransactionSupplier.get().dataRead().countsForNode( StatementConstants.ANY_LABEL );
    }
}
