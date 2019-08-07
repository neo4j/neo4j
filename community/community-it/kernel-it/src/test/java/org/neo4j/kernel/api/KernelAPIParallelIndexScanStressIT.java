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
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.kernel.api.IndexReadSession;
import org.neo4j.internal.kernel.api.Kernel;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.Transaction;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexOrder;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.rule.RandomRule;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.internal.kernel.api.Transaction.Type.explicit;

@DbmsExtension
@ExtendWith( RandomExtension.class )
class KernelAPIParallelIndexScanStressIT
{
    private static final int N_THREADS = 10;
    private static final int N_NODES = 10_000;

    @Inject
    private GraphDatabaseAPI db;
    @Inject
    private RandomRule random;

    @Test
    void shouldDoParallelIndexScans() throws Throwable
    {
        // Given
        try ( org.neo4j.graphdb.Transaction tx = db.beginTx() )
        {
            createLabeledNodes( N_NODES, "LABEL1", "prop" );
            createLabeledNodes( N_NODES, "LABEL2", "prop" );
            createLabeledNodes( N_NODES, "LABEL3", "prop" );
            tx.commit();
        }

        try ( org.neo4j.graphdb.Transaction tx = db.beginTx() )
        {
            db.schema().indexFor( Label.label( "LABEL1" ) ).on( "prop" ).create();
            db.schema().indexFor( Label.label( "LABEL2" ) ).on( "prop" ).create();
            db.schema().indexFor( Label.label( "LABEL3" ) ).on( "prop" ).create();
            tx.commit();
        }

        try ( org.neo4j.graphdb.Transaction tx = db.beginTx() )
        {
            db.schema().awaitIndexesOnline( 10, MINUTES );
            tx.commit();
        }

        // when & then
        Kernel kernel = db.getDependencyResolver().resolveDependency( Kernel.class );
        IndexReadSession[] indexes = new IndexReadSession[3];
        try ( Transaction tx = kernel.beginTransaction( explicit, LoginContext.AUTH_DISABLED ) )
        {
            int propKey = tx.tokenRead().propertyKey( "prop" );

            indexes[0] = indexReadSession( tx, propKey, "LABEL1" );
            indexes[1] = indexReadSession( tx, propKey, "LABEL2" );
            indexes[2] = indexReadSession( tx, propKey, "LABEL3" );
            tx.commit();
        }

        KernelAPIParallelStress.parallelStressInTx( kernel,
                                                    N_THREADS,
                                                    tx -> tx.cursors().allocateNodeValueIndexCursor(),
                                                    ( read, cursor ) -> indexSeek( read,
                                                                                   cursor,
                                                                                   indexes[random.nextInt( indexes.length )] ));

    }

    private static IndexReadSession indexReadSession( Transaction tx, int propKey, String label ) throws IndexNotFoundKernelException
    {
        int labelId = tx.tokenRead().nodeLabel( label );
        IndexDescriptor index = tx.schemaRead().index( labelId, propKey );
        return tx.dataRead().indexReadSession( index );
    }

    private void createLabeledNodes( int nNodes, String labelName, String propKey )
    {
        for ( int i = 0; i < nNodes; i++ )
        {
            Node node = db.createNode();
            node.addLabel( Label.label( labelName ) );
            node.setProperty( propKey, i );
        }
    }

    private static Runnable indexSeek( Read read, NodeValueIndexCursor cursor, IndexReadSession index )
    {
        return () ->
        {
            try
            {
                IndexQuery.ExistsPredicate query = IndexQuery.exists( index.reference().schema().getPropertyIds()[0] );
                read.nodeIndexSeek( index, cursor, IndexOrder.NONE, true, query );
                int n = 0;
                while ( cursor.next() )
                {
                    n++;
                }
                assertEquals( N_NODES, n, "correct number of nodes" );
            }
            catch ( KernelException e )
            {
                throw new RuntimeException( e );
            }
        };
    }
}
