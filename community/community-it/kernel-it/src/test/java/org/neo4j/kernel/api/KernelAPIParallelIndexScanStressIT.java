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
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.kernel.api.IndexReadSession;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexOrder;
import org.neo4j.kernel.impl.coreapi.schema.IndexDefinitionImpl;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.rule.RandomRule;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.kernel.api.KernelTransaction.Type.EXPLICIT;

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
            createLabeledNodes( tx, N_NODES, "LABEL1", "prop" );
            createLabeledNodes( tx, N_NODES, "LABEL2", "prop" );
            createLabeledNodes( tx, N_NODES, "LABEL3", "prop" );
            tx.commit();
        }

        IndexDescriptor index1;
        IndexDescriptor index2;
        IndexDescriptor index3;
        try ( org.neo4j.graphdb.Transaction tx = db.beginTx() )
        {
            index1 = unwrap( tx.schema().indexFor( Label.label( "LABEL1" ) ).on( "prop" ).create() );
            index2 = unwrap( tx.schema().indexFor( Label.label( "LABEL2" ) ).on( "prop" ).create() );
            index3 = unwrap( tx.schema().indexFor( Label.label( "LABEL3" ) ).on( "prop" ).create() );
            tx.commit();
        }

        try ( org.neo4j.graphdb.Transaction tx = db.beginTx() )
        {
            tx.schema().awaitIndexesOnline( 10, MINUTES );
            tx.commit();
        }

        // when & then
        Kernel kernel = db.getDependencyResolver().resolveDependency( Kernel.class );
        IndexReadSession[] indexes = new IndexReadSession[3];
        try ( KernelTransaction tx = kernel.beginTransaction( EXPLICIT, LoginContext.AUTH_DISABLED ) )
        {
            indexes[0] = indexReadSession( tx, index1 );
            indexes[1] = indexReadSession( tx, index2 );
            indexes[2] = indexReadSession( tx, index3 );
            tx.commit();
        }

        KernelAPIParallelStress.parallelStressInTx( kernel,
                                                    N_THREADS,
                                                    tx -> tx.cursors().allocateNodeValueIndexCursor(),
                                                    ( read, cursor ) -> indexSeek( read,
                                                                                   cursor,
                                                                                   indexes[random.nextInt( indexes.length )] ));

    }

    private IndexDescriptor unwrap( IndexDefinition indexDefinition )
    {
        return ((IndexDefinitionImpl) indexDefinition).getIndexReference();
    }

    private static IndexReadSession indexReadSession( KernelTransaction tx, IndexDescriptor index ) throws IndexNotFoundKernelException
    {
        return tx.dataRead().indexReadSession( index );
    }

    private void createLabeledNodes( org.neo4j.graphdb.Transaction tx, int nNodes, String labelName, String propKey )
    {
        for ( int i = 0; i < nNodes; i++ )
        {
            Node node = tx.createNode();
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
