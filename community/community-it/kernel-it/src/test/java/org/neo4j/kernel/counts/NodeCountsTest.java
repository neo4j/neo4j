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

import org.junit.jupiter.api.Test;

import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.NamedThreadFactory;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.Barrier;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.internal.kernel.api.TokenRead.ANY_LABEL;

@ImpermanentDbmsExtension
class NodeCountsTest
{
    @Inject
    private GraphDatabaseAPI db;

    @Test
    void shouldReportNumberOfNodesInAnEmptyGraph()
    {
        // when
        long nodeCount = numberOfNodes();

        // then
        assertEquals( 0, nodeCount );
    }

    @Test
    void shouldReportNumberOfNodes()
    {
        // given
        try ( Transaction tx = db.beginTx() )
        {
            tx.createNode();
            tx.createNode();
            tx.commit();
        }

        // when
        long nodeCount = numberOfNodes();

        // then
        assertEquals( 2, nodeCount );
    }

    @Test
    void shouldReportAccurateNumberOfNodesAfterDeletion()
    {
        // given
        Node one;
        try ( Transaction tx = db.beginTx() )
        {
            one = tx.createNode();
            tx.createNode();
            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            tx.getNodeById( one.getId() ).delete();
            tx.commit();
        }

        // when
        long nodeCount = numberOfNodes();

        // then
        assertEquals( 1, nodeCount );
    }

    @Test
    void shouldIncludeNumberOfNodesAddedInTransaction()
    {
        // given
        try ( Transaction tx = db.beginTx() )
        {
            tx.createNode();
            tx.createNode();
            tx.commit();
        }
        long before = numberOfNodes();

        try ( Transaction tx = db.beginTx() )
        {
            // when
            tx.createNode();
            long nodeCount = countsForNode( tx );

            // then
            assertEquals( before + 1, nodeCount );
            tx.commit();
        }
    }

    @Test
    void shouldIncludeNumberOfNodesDeletedInTransaction()
    {
        // given
        Node one;
        try ( Transaction tx = db.beginTx() )
        {
            one = tx.createNode();
            tx.createNode();
            tx.commit();
        }
        long before = numberOfNodes();

        try ( Transaction tx = db.beginTx() )
        {
            // when
            tx.getNodeById( one.getId() ).delete();
            long nodeCount = countsForNode( tx );

            // then
            assertEquals( before - 1, nodeCount );
            tx.commit();
        }
    }

    @Test
    void shouldNotSeeNodeCountsOfOtherTransaction() throws Exception
    {
        // given
        final Barrier.Control barrier = new Barrier.Control();
        long before = numberOfNodes();

        var executor = Executors.newSingleThreadExecutor( NamedThreadFactory.named( "create-nodes" ) );
        var graphDb = db;
        try
        {

            Future<Long> done = executor.submit( () ->
            {
                try ( Transaction tx = graphDb.beginTx() )
                {
                    tx.createNode();
                    tx.createNode();
                    barrier.reached();
                    long whatThisThreadSees = countsForNode( tx );
                    tx.commit();
                    return whatThisThreadSees;
                }
            } );

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
        finally
        {
            executor.shutdown();
        }
    }

    /** Transactional version of {@link #countsForNode(Transaction)} */
    private long numberOfNodes()
    {
        try ( Transaction tx = db.beginTx() )
        {
            long nodeCount = countsForNode( tx );
            tx.commit();
            return nodeCount;
        }
    }

    private long countsForNode( Transaction tx )
    {
        return ((InternalTransaction) tx).kernelTransaction().dataRead().countsForNode( ANY_LABEL );
    }
}
