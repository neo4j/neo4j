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
import org.junit.jupiter.api.extension.ExtendWith;

import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.kernel.api.Kernel;
import org.neo4j.internal.kernel.api.NodeLabelIndexCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.Transaction;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.rule.RandomRule;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.internal.kernel.api.Transaction.Type.explicit;

@DbmsExtension
@ExtendWith( RandomExtension.class )
class KernelAPIParallelLabelScanStressIT
{
    private static final int N_THREADS = 10;
    private static final int N_NODES = 10_000;

    @Inject
    private GraphDatabaseAPI db;
    @Inject
    private RandomRule random;

    @Test
    void shouldDoParallelLabelScans() throws Throwable
    {
        int[] labels = new int[3];
        Kernel kernel = db.getDependencyResolver().resolveDependency( Kernel.class );

        // Create nodes with labels
        try ( Transaction tx = kernel.beginTransaction( explicit, LoginContext.AUTH_DISABLED ) )
        {
            labels[0] = createLabeledNodes( tx, N_NODES, "LABEL1" );
            labels[1] = createLabeledNodes( tx, N_NODES, "LABEL2" );
            labels[2] = createLabeledNodes( tx, N_NODES, "LABEL3" );
            tx.commit();
        }

        KernelAPIParallelStress.parallelStressInTx( kernel,
                                                    N_THREADS,
                                                    tx -> tx.cursors().allocateNodeLabelIndexCursor(),
                                                    ( read, cursor ) -> labelScan( read,
                                                                                   cursor,
                                                                                   labels[random.nextInt( labels.length )] ) );
    }

    private static int createLabeledNodes( Transaction tx, int nNodes, String labelName ) throws KernelException
    {
        int label = tx.tokenWrite().labelCreateForName( labelName, false );
        for ( int i = 0; i < nNodes; i++ )
        {
            long n = tx.dataWrite().nodeCreate();
            tx.dataWrite().nodeAddLabel( n, label );
        }
        return label;
    }

    private Runnable labelScan( Read read, NodeLabelIndexCursor cursor, int label )
    {
        return () ->
        {
            read.nodeLabelScan( label, cursor );

            int n = 0;
            while ( cursor.next() )
            {
                n++;
            }
            assertEquals( N_NODES, n, "correct number of nodes" );
        };
    }
}
