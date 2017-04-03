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
package org.neo4j.kernel.impl.api.integrationtest;

import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Predicate;

import org.neo4j.cursor.Cursor;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.Exceptions;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.RecordStorageEngine;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.storageengine.api.BatchingLongProgression;
import org.neo4j.storageengine.api.NodeItem;
import org.neo4j.test.rule.DatabaseRule;
import org.neo4j.test.rule.EnterpriseDatabaseRule;
import org.neo4j.test.rule.RandomRule;

import static java.util.Collections.disjoint;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ParallelNodeScanReadOperationsIntegrationTest
{
    @Rule
    public DatabaseRule databaseRule = new EnterpriseDatabaseRule();

    @Rule
    public final RandomRule random = new RandomRule();

    @Test
    public void shouldRunParallelNodeScanThroughTheReadOperationsUsingTransactions() throws Throwable
    {
        GraphDatabaseAPI db = databaseRule.getGraphDatabaseAPI();
        createNodes( db, randomNodes( nodeStore( db ) ) );

        int threads = random.nextInt( 2, 6 );
        ExecutorService executorService = Executors.newFixedThreadPool( threads );

        Set<Long> singleThreadExecutionResult = new HashSet<>();
        Set<Long> parallelExecutionResult = new HashSet<>();
        ThreadToStatementContextBridge bridge =
                db.getDependencyResolver().resolveDependency( ThreadToStatementContextBridge.class );

        try ( Transaction tx = db.beginTx() )
        {
            KernelTransaction ktx = bridge.getKernelTransactionBoundToThisThread( true );

            try ( Statement statement = ktx.acquireStatement() )
            {
                statement.readOperations().nodeGetAllCursor().forAll( n -> singleThreadExecutionResult.add( n.id() ) );
            }

            try ( Statement statement = ktx.acquireStatement() )
            {
                parallelExecution( statement.readOperations(), executorService, threads, parallelExecutionResult );
            }

            tx.success();
        }

        assertEquals( singleThreadExecutionResult, parallelExecutionResult );
    }

    @Test
    public void shouldRunParallelNodeScanWithTxStateChangesThroughTheReadOperationsUsingTransactions() throws Throwable
    {
        GraphDatabaseAPI db = databaseRule.getGraphDatabaseAPI();
        int nodes = randomNodes( nodeStore( db ) );
        createNodes( db, nodes );
        int threads = random.nextInt( 2, 6 );
        ExecutorService executorService = Executors.newFixedThreadPool( threads );

        Set<Long> singleThreadExecutionResult = new HashSet<>();
        Set<Long> parallelExecutionResult = new HashSet<>();
        ThreadToStatementContextBridge bridge =
                db.getDependencyResolver().resolveDependency( ThreadToStatementContextBridge.class );

        try ( Transaction tx = db.beginTx() )
        {
            createTemporaryNodesAndDeleteSomeOfTheExistingNodes( db, nodes );
            KernelTransaction ktx = bridge.getKernelTransactionBoundToThisThread( true );

            try ( Statement statement = ktx.acquireStatement() )
            {
                statement.readOperations().nodeGetAllCursor().forAll( n -> singleThreadExecutionResult.add( n.id() ) );
            }

            try ( Statement statement = ktx.acquireStatement() )
            {
                parallelExecution( statement.readOperations(), executorService, threads, parallelExecutionResult );
            }

            tx.success();
        }

        assertEquals( message( singleThreadExecutionResult, parallelExecutionResult ), singleThreadExecutionResult,
                parallelExecutionResult );
    }

    private void parallelExecution( ReadOperations readOperations, ExecutorService executorService, int threads,
            Set<Long> parallelExecutionResult ) throws Throwable
    {
        try
        {
            BatchingLongProgression progression = readOperations.parallelNodeScan();

            @SuppressWarnings( "unchecked" )
            Future<Set<Long>>[] futures = new Future[threads];
            for ( int i = 0; i < threads; i++ )
            {
                futures[i] = executorService.submit( () ->
                {
                    HashSet<Long> ids = new HashSet<>();
                    try ( Cursor<NodeItem> cursor = readOperations.nodeGeCursor( progression ) )
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
            for ( int i = 0; i < threads; i++ )
            {
                try
                {
                    Set<Long> set = futures[i].get();
                    assertTrue( i + "" + message( parallelExecutionResult, set ),
                            disjoint( parallelExecutionResult, set ) );
                    parallelExecutionResult.addAll( set );
                }
                catch ( Throwable current )
                {
                    current.printStackTrace();
                    t = Exceptions.chain( t, current );
                }
            }

            if ( t != null )
            {
                throw t;
            }
        }
        finally
        {
            executorService.shutdown();
        }
    }

    private String message( Set<Long> expected, Set<Long> actual )
    {
        List<Long> sortedExpected = expected.stream().sorted().collect( toList() );
        List<Long> sortedActual = actual.stream().sorted().collect( toList() );

        List<Long> missingInActual =
                expected.stream().filter( ((Predicate<Long>) actual::contains).negate() ).sorted().collect( toList() );
        List<Long> extraInActual =
                actual.stream().filter( ((Predicate<Long>) expected::contains).negate() ).sorted().collect( toList() );

        List<Long> intersect = expected.stream().filter( actual::contains ).sorted().collect( toList() );

        String expectedResult = "Expected: " + sortedExpected;
        String actualResult = "Actual: " + sortedActual;
        String diff = "Missing: " + missingInActual + "\n" + "Extra: " + extraInActual;
        String overlap = "Overlap: " + intersect;
        return expectedResult + "\n" + actualResult + "\n" + diff + "\n" + overlap;
    }

    private NodeStore nodeStore( GraphDatabaseAPI db )
    {
        return db.getDependencyResolver().resolveDependency( RecordStorageEngine.class )
                .testAccessNeoStores().getNodeStore();
    }

    private int randomNodes( NodeStore nodeStore )
    {
        int recordsPerPage = nodeStore.getRecordsPerPage();
        int pages = random.nextInt( 40, 1000 );
        int nonAlignedRecords = random.nextInt( 0, recordsPerPage - 1 );
        return pages * recordsPerPage + nonAlignedRecords;
    }

    private void createTemporaryNodesAndDeleteSomeOfTheExistingNodes( GraphDatabaseService db, int nodes )
    {
        for ( long i = 0; i < 100; i++ )
        {
            db.createNode();
        }

        List<Long> deleted = new ArrayList<>( 100 );
        for ( int i = 0; i < 100; i++ )
        {
            try
            {
                long id = random.nextLong( 0, nodes );
                db.getNodeById( id ).delete();
                deleted.add( id );
            }
            catch ( NotFoundException ex )
            {
                // already deleted, it doesn't matter let's proceed
            }
        }
        Collections.sort( deleted );
    }

    private void createNodes( GraphDatabaseAPI db, int nodes )
    {
        try ( Transaction tx = db.beginTx() )
        {
            for ( int j = 0; j < nodes; j++ )
            {
                db.createNode().getId();
            }
            tx.success();
        }
    }
}
