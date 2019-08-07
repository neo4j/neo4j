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
package org.neo4j.kernel.api;

import org.junit.jupiter.api.Test;

import java.util.concurrent.ThreadLocalRandom;

import org.neo4j.internal.kernel.api.Kernel;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;
import org.neo4j.internal.kernel.api.Transaction;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.io.IOUtils;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.Inject;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.internal.kernel.api.Transaction.Type.explicit;

@DbmsExtension
class KernelAPIParallelTraversalStressIT
{
    private static final int N_THREADS = 10;
    private static final int N_NODES = 10_000;
    private static final int N_RELATIONSHIPS = 4 * N_NODES;

    @Inject
    private GraphDatabaseAPI db;

    @Test
    void shouldScanNodesAndTraverseInParallel() throws Throwable
    {
        Kernel kernel = db.getDependencyResolver().resolveDependency( Kernel.class );

        createRandomGraph( kernel );

        KernelAPIParallelStress.parallelStressInTx( kernel, N_THREADS, NodeAndTraverseCursors::new, KernelAPIParallelTraversalStressIT::scanAndTraverse );
    }

    private static void createRandomGraph( Kernel kernel ) throws Exception
    {
        ThreadLocalRandom random = ThreadLocalRandom.current();

        long[] nodes = new long[N_NODES];
        Transaction setup = kernel.beginTransaction( explicit, LoginContext.AUTH_DISABLED );

        int relationshipType = setup.token().relationshipTypeCreateForName( "R", false );
        for ( int i = 0; i < N_NODES; i++ )
        {
            nodes[i] = setup.dataWrite().nodeCreate();
            if ( (i + 1) % 10000 == 0 )
            {
                setup.commit();
                setup = kernel.beginTransaction( explicit, LoginContext.AUTH_DISABLED );
            }
        }

        for ( int i = 0; i < N_RELATIONSHIPS; i++ )
        {
            int n1 = random.nextInt( N_NODES );
            int n2 = random.nextInt( N_NODES );
            while ( n2 == n1 )
            {
                n2 = random.nextInt( N_NODES );
            }
            setup.dataWrite().relationshipCreate( nodes[n1], relationshipType, nodes[n2] );
            if ( (i + 1) % 10000 == 0 )
            {
                setup.commit();
                setup = kernel.beginTransaction( explicit, LoginContext.AUTH_DISABLED );
            }
        }

        setup.commit();
    }

    private static Runnable scanAndTraverse( Read read, NodeAndTraverseCursors cursors )
    {
        return () ->
        {
            read.allNodesScan( cursors.nodeCursor );
            int n = 0;
            int r = 0;
            while ( cursors.nodeCursor.next() )
            {
                cursors.nodeCursor.allRelationships( cursors.traversalCursor );
                while ( cursors.traversalCursor.next() )
                {
                    r++;
                }
                n++;
            }
            assertEquals( N_NODES, n, "correct number of nodes" );
            assertEquals( 2 * N_RELATIONSHIPS, r, "correct number of traversals" );
        };
    }

    static class NodeAndTraverseCursors implements AutoCloseable
    {
        final NodeCursor nodeCursor;
        final RelationshipTraversalCursor traversalCursor;

        NodeAndTraverseCursors( Transaction tx )
        {
            nodeCursor = tx.cursors().allocateNodeCursor();
            traversalCursor = tx.cursors().allocateRelationshipTraversalCursor();
        }

        @Override
        public void close() throws Exception
        {
            IOUtils.closeAll( nodeCursor, traversalCursor );
        }
    }
}
