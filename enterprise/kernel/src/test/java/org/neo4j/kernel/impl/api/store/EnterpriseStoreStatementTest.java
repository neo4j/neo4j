/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.api.store;

import org.junit.Rule;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.neo4j.cursor.Cursor;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.Exceptions;
import org.neo4j.kernel.impl.api.state.TxState;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.RecordStorageEngine;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.storageengine.api.NodeItem;
import org.neo4j.storageengine.api.txstate.ReadableTransactionState;
import org.neo4j.test.rule.EnterpriseDatabaseRule;
import org.neo4j.test.rule.RandomRule;

import static java.util.Collections.disjoint;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.neo4j.kernel.impl.locking.LockService.NO_LOCK_SERVICE;

public class EnterpriseStoreStatementTest
{
    @Rule
    public final EnterpriseDatabaseRule databaseRule = new EnterpriseDatabaseRule();
    @Rule
    public final RandomRule random = new RandomRule();

    @Test
    public void parallelScanShouldProvideTheSameResultAsANormalScan() throws Throwable
    {
        GraphDatabaseAPI db = databaseRule.getGraphDatabaseAPI();
        NeoStores neoStores =
                db.getDependencyResolver().resolveDependency( RecordStorageEngine.class ).testAccessNeoStores();
        int nodes = randomNodes( neoStores.getNodeStore() );
        createNodes( db, nodes );

        Set<Long> expected = singleThreadExecution( neoStores, null );

        int threads = random.nextInt( 2, 6 );
        ExecutorService executorService = Executors.newCachedThreadPool();
        try
        {
            Set<Long> parallelResult = parallelExecution( neoStores, executorService, threads, null );
            assertEquals( expected, parallelResult );
        }
        finally
        {
            executorService.shutdown();
        }
    }

    @Test
    public void parallelScanWithTxStateChangesShouldProvideTheSameResultAsANormalScanWithTheSameChanges()
            throws Throwable
    {
        GraphDatabaseAPI db = databaseRule.getGraphDatabaseAPI();
        NeoStores neoStores =
                db.getDependencyResolver().resolveDependency( RecordStorageEngine.class ).testAccessNeoStores();
        int nodes = randomNodes( neoStores.getNodeStore() );
        long lastNodeId = createNodes( db, nodes );

        TxState txState = crateTxStateWithRandomAddedAndDeletedNodes( nodes, lastNodeId );

        Set<Long> expected = singleThreadExecution( neoStores, txState );

        ExecutorService executorService = Executors.newCachedThreadPool();
        try
        {
            int threads = random.nextInt( 2, 6 );
            Set<Long> parallelResult = parallelExecution( neoStores, executorService, threads, txState );
            assertEquals( expected, parallelResult );
        }
        finally
        {
            executorService.shutdown();
        }
    }

    private Set<Long> parallelExecution( NeoStores neoStores, ExecutorService executorService, int threads,
            ReadableTransactionState state ) throws Throwable
    {
        EnterpriseStoreStatement[] localStatements = new EnterpriseStoreStatement[threads];
        for ( int i = 0; i < threads; i++ )
        {
            localStatements[i] = new EnterpriseStoreStatement( neoStores, null, null, NO_LOCK_SERVICE );
        }
        // use any of the local statements to build the shared progression
        NodeProgression progression = localStatements[0].parallelNodeScanProgression( state );

        @SuppressWarnings( "unchecked" )
        Future<Set<Long>>[] futures = new Future[threads];
        for ( int i = 0; i < threads; i++ )
        {
            int id = i;
            futures[i] = executorService.submit( () ->
            {
                HashSet<Long> ids = new HashSet<>();
                try ( Cursor<NodeItem> cursor = localStatements[id]
                        .acquireParallelScanNodeCursor( progression ) )
                {
                    while ( cursor.next() )
                    {
                        long nodeId = cursor.get().id();
                        assertTrue( ids.add( nodeId ) );
                    }
                }
                return ids;
            } );
        }

        Throwable t = null;
        @SuppressWarnings( "unchecked" )
        Set<Long> parallelResult = new HashSet<>();
        for ( int i = 0; i < threads; i++ )
        {
            try
            {
                Set<Long> set = futures[i].get();
                assertTrue( disjoint( parallelResult, set ) );
                parallelResult.addAll( set );
            }
            catch ( Throwable current )
            {
                t = Exceptions.chain( t, current );
            }
        }

        if ( t != null )
        {
            throw t;
        }

        return parallelResult;
    }

    private Set<Long> singleThreadExecution( NeoStores neoStores, ReadableTransactionState state )
    {
        Set<Long> expected = new HashSet<>();
        EnterpriseStoreStatement statement = new EnterpriseStoreStatement( neoStores, null, null, NO_LOCK_SERVICE );
        try ( Cursor<NodeItem> cursor = statement
                .acquireNodeCursor( new AllNodeProgression( neoStores.getNodeStore(), state ) ) )
        {
            while ( cursor.next() )
            {
                long nodeId = cursor.get().id();
                assertTrue( expected.add( nodeId ) );

            }
        }
        return expected;
    }

    private int randomNodes( NodeStore nodeStore )
    {
        int recordsPerPage = nodeStore.getRecordsPerPage();
        int pages = random.nextInt( 40, 1000 );
        int nonAlignedRecords = random.nextInt( 0, recordsPerPage - 1 );
        return pages * recordsPerPage + nonAlignedRecords;
    }

    private TxState crateTxStateWithRandomAddedAndDeletedNodes( int nodes, long lastNodeId )
    {
        TxState txState = new TxState();
        for ( long i = lastNodeId + 1; i <= lastNodeId + 100; i++ )
        {
            txState.nodeDoCreate( i );
        }

        for ( int i = 0; i < 100; i++ )
        {
            long id = random.nextLong( 0, nodes );
            txState.nodeDoDelete( id );
        }
        return txState;
    }

    private long createNodes( GraphDatabaseAPI db, int nodes )
    {
        try ( Transaction tx = db.beginTx() )
        {
            long nodeId = -1;
            for ( int j = 0; j < nodes; j++ )
            {
                nodeId = db.createNode().getId();
            }
            tx.success();
            return nodeId;
        }
    }

    private void noCache( NodeCursor c )
    {
    }
}
