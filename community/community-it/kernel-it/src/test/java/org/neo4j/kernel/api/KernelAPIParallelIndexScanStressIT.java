/*
 * Copyright (c) 2002-2018 "Neo4j,"
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

import org.junit.ClassRule;
import org.junit.Test;

import java.util.concurrent.ThreadLocalRandom;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.internal.kernel.api.IndexOrder;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.kernel.api.IndexReference;
import org.neo4j.internal.kernel.api.Kernel;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.Transaction;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.test.rule.DatabaseRule;
import org.neo4j.test.rule.EmbeddedDatabaseRule;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.neo4j.internal.kernel.api.Transaction.Type.explicit;

public class KernelAPIParallelIndexScanStressIT
{
    @ClassRule
    public static final DatabaseRule db = new EmbeddedDatabaseRule();

    private final int N_THREADS = 10;
    private final int N_NODES = 10_000;

    private ThreadLocalRandom random = ThreadLocalRandom.current();

    @Test
    public void shouldDoParallelIndexScans() throws Throwable
    {
        // Given
        try ( org.neo4j.graphdb.Transaction tx = db.beginTx() )
        {
            createLabeledNodes( N_NODES, "LABEL1", "prop" );
            createLabeledNodes( N_NODES, "LABEL2", "prop" );
            createLabeledNodes( N_NODES, "LABEL3", "prop" );
            tx.success();
        }

        try ( org.neo4j.graphdb.Transaction tx = db.beginTx() )
        {
            db.schema().indexFor( Label.label( "LABEL1" ) ).on( "prop" ).create();
            db.schema().indexFor( Label.label( "LABEL2" ) ).on( "prop" ).create();
            db.schema().indexFor( Label.label( "LABEL3" ) ).on( "prop" ).create();
            tx.success();
        }

        try ( org.neo4j.graphdb.Transaction tx = db.beginTx() )
        {
            db.schema().awaitIndexesOnline( 30, SECONDS );
            tx.success();
        }

        // when & then
        Kernel kernel = db.resolveDependency( Kernel.class );
        IndexReference[] indexes = new IndexReference[3];
        try ( Transaction tx = kernel.beginTransaction( explicit, LoginContext.AUTH_DISABLED ) )
        {
            int propKey = tx.tokenRead().propertyKey( "prop" );

            indexes[0] = tx.schemaRead().index( tx.tokenRead().nodeLabel( "LABEL1" ), propKey );
            indexes[1] = tx.schemaRead().index( tx.tokenRead().nodeLabel( "LABEL2" ), propKey );
            indexes[2] = tx.schemaRead().index( tx.tokenRead().nodeLabel( "LABEL3" ), propKey );
            tx.success();
        }

        KernelAPIParallelStress.parallelStressInTx( kernel,
                                                    N_THREADS,
                                                    tx -> tx.cursors().allocateNodeValueIndexCursor(),
                                                    ( read, cursor ) -> indexSeek( read,
                                                                                   cursor,
                                                                                   indexes[random.nextInt( indexes.length )] ));

    }

    private void createLabeledNodes( int nNodes, String labelName, String propKey ) throws KernelException
    {
        for ( int i = 0; i < nNodes; i++ )
        {
            Node node = db.createNode();
            node.addLabel( Label.label( labelName ) );
            node.setProperty( propKey, i );
        }
    }

    private Runnable indexSeek( Read read, NodeValueIndexCursor cursor, IndexReference index )
    {
        return () ->
        {
            try
            {
                read.nodeIndexSeek( index, cursor, IndexOrder.NONE, true, IndexQuery.exists( index.properties()[0] ) );
                int n = 0;
                while ( cursor.next() )
                {
                    n++;
                }
                assertEquals( "correct number of nodes", N_NODES, n );
            }
            catch ( KernelException e )
            {
                throw new RuntimeException( e );
            }
        };
    }
}
