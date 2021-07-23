/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.kernel.impl.query;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import org.neo4j.configuration.Config;
import org.neo4j.cypher.internal.config.MEMORY_TRACKING$;
import org.neo4j.cypher.internal.runtime.GrowingArray;
import org.neo4j.cypher.internal.runtime.memory.QueryMemoryTracker;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.TransactionTerminatedException;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.GraphDatabaseQueryService;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.query.QueryObfuscator;
import org.neo4j.kernel.impl.api.KernelStatement;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.factory.FacadeKernelTransactionFactory;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.factory.KernelTransactionFactory;
import org.neo4j.memory.LocalMemoryTracker;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.values.virtual.MapValue;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.kernel.api.KernelTransaction.Type.IMPLICIT;
import static org.neo4j.values.virtual.VirtualValues.EMPTY_MAP;

@ImpermanentDbmsExtension
class Neo4jTransactionalContextIT
{
    @Inject
    private GraphDatabaseService graphOps;
    @Inject
    private GraphDatabaseQueryService graph;

    private KernelTransactionFactory transactionFactory;

    // Helpers

    private long getPageCacheHits( TransactionalContext ctx )
    {
        return ctx.transaction().kernelTransaction().executionStatistics().pageHits();
    }

    private long getPageCacheFaults( TransactionalContext ctx )
    {
        return ctx.transaction().kernelTransaction().executionStatistics().pageFaults();
    }

    @SuppressWarnings( "ResultOfMethodCallIgnored" )
    private void generatePageCacheHits( TransactionalContext ctx )
    {
        ctx.transaction().getAllNodes().iterator().stream().count();
    }

    private void getLocks( TransactionalContext ctx, String label )
    {
        ctx.transaction().findNodes( Label.label( label ) ).stream().forEach( Node::delete );
    }

    private long getActiveLockCount( TransactionalContext ctx )
    {
        return ((KernelStatement) ctx.statement()).locks().activeLockCount();
    }

    private TransactionalContext createTransactionContext( InternalTransaction transaction )
    {
        return Neo4jTransactionalContextFactory
                .create( () -> graph, transactionFactory )
                .newContext( transaction, "no query", EMPTY_MAP );
    }

    @BeforeEach
    void setup()
    {
        transactionFactory = new FacadeKernelTransactionFactory( Config.newBuilder().build(), (GraphDatabaseFacade) graphOps );
    }

    @Test
    void contextWithNewTransaction_executing_query_should_use_outer_transaction_id_and_query_text()
    {
        // Given
        var outerTx = graph.beginTransaction( IMPLICIT, LoginContext.AUTH_DISABLED );
        var queryText = "<query text>";
        var outerCtx = Neo4jTransactionalContextFactory
                .create( () -> graph, transactionFactory )
                .newContext( outerTx, queryText, MapValue.EMPTY );
        var executingQuery = outerCtx.executingQuery();

        // When
        var innerCtx = outerCtx.contextWithNewTransaction();

        // Then
        assertThat( executingQuery, sameInstance( innerCtx.executingQuery() ) );
        assertThat( executingQuery.rawQueryText(), equalTo( queryText ) );
        var snapshot = executingQuery.snapshot();
        assertThat( snapshot.transactionId(), equalTo( outerTx.kernelTransaction().getUserTransactionId() ) );
    }

    @Test
    void contextWithNewTransaction_executing_query_should_sum_up_page_hits_faults_from_inner_and_outer_transaction()
    {
        // Given

        // Add data to database so that we get page hits/faults
        graphOps.executeTransactionally( "CREATE (n)" );

        var outerTx = graph.beginTransaction( IMPLICIT, LoginContext.AUTH_DISABLED );
        var outerCtx = createTransactionContext( outerTx );
        var executingQuery = outerCtx.executingQuery();

        // When
        // Generate some page cache hits/faults
        generatePageCacheHits( outerCtx );
        var outerHits = getPageCacheHits( outerCtx );
        var outerFaults = getPageCacheFaults( outerCtx );

        var innerCtx = outerCtx.contextWithNewTransaction();

        // Generate some page cache hits/faults
        generatePageCacheHits( innerCtx );
        var innerHits = getPageCacheHits( innerCtx );
        var innerFaults = getPageCacheFaults( innerCtx );

        // Then
        var snapshot = executingQuery.snapshot();
        // Make sure we are not just summing up 0s
        assertThat( outerHits, greaterThan( 0L ) );
        assertThat( innerHits, greaterThan( 0L ) );
        // Actual assertion
        assertThat( snapshot.pageHits(), equalTo( outerHits + innerHits ) );
        assertThat( snapshot.pageFaults(), equalTo( outerFaults + innerFaults ) );
    }

    @Test
    void contextWithNewTransaction_executing_query_should_sum_up_page_hits_faults_from_inner_and_outer_transactions_also_when_committed()
    {
        // Given

        // Add data to database so that we get page hits/faults
        graphOps.executeTransactionally( "CREATE (n)" );

        var outerTx = graph.beginTransaction( IMPLICIT, LoginContext.AUTH_DISABLED );
        var outerCtx = createTransactionContext( outerTx );
        var executingQuery = outerCtx.executingQuery();

        // When
        // Generate some page cache hits/faults
        generatePageCacheHits( outerCtx );
        var outerHits = getPageCacheHits( outerCtx );
        var outerFaults = getPageCacheFaults( outerCtx );

        var closedInnerHits = 0L;
        var closedInnerFaults = 0L;
        for ( int i = 0; i < 10; i++ )
        {
            var innerCtx = outerCtx.contextWithNewTransaction();
            //  Generate page cache hits/faults for half of the executions
            if ( i % 2 == 0 )
            {
                generatePageCacheHits( innerCtx );
            }
            closedInnerHits += getPageCacheHits( innerCtx );
            closedInnerFaults += getPageCacheFaults( innerCtx );
            innerCtx.commit();
        }

        var openInnerCtx = outerCtx.contextWithNewTransaction();
        // Generate page cache hits/faults
        generatePageCacheHits( openInnerCtx );
        var openInnerHits = getPageCacheHits( openInnerCtx );
        var openInnerFaults = getPageCacheFaults( openInnerCtx );

        // Then
        var snapshot = executingQuery.snapshot();
        // Make sure we are not just summing up 0s
        assertThat( outerHits, greaterThan( 0L ) );
        assertThat( closedInnerHits, greaterThan( 0L ) );
        assertThat( openInnerHits, greaterThan( 0L ) );
        // Actual assertion
        assertThat( snapshot.pageHits(), equalTo( outerHits + closedInnerHits + openInnerHits ) );
        assertThat( snapshot.pageFaults(), equalTo( outerFaults + closedInnerFaults + openInnerFaults ) );
    }

    @Test
    void contextWithNewTransaction_kernel_statistics_provider_should_only_see_page_hits_faults_from_current_transactions_in_PROFILE()
    {
        // Given

        // Add data to database so that we get page hits/faults
        graphOps.executeTransactionally( "CREATE (n)" );

        var outerTx = graph.beginTransaction( IMPLICIT, LoginContext.AUTH_DISABLED );
        var outerCtx = createTransactionContext( outerTx );

        // When
        // Generate some page cache hits/faults
        generatePageCacheHits( outerCtx );
        var outerHits = getPageCacheHits( outerCtx );
        var outerFaults = getPageCacheFaults( outerCtx );

        var closedInnerHits = 0L;
        for ( int i = 0; i < 10; i++ )
        {
            var innerCtx = outerCtx.contextWithNewTransaction();
            //  Generate page cache hits/faults for half of the executions
            if ( i % 2 == 0 )
            {
                generatePageCacheHits( innerCtx );
            }
            closedInnerHits += getPageCacheHits( innerCtx );
            innerCtx.commit();
        }

        var openInnerCtx = outerCtx.contextWithNewTransaction();
        // Generate page cache hits/faults
        generatePageCacheHits( openInnerCtx );
        var openInnerHits = getPageCacheHits( openInnerCtx );
        var openInnerFaults = getPageCacheFaults( openInnerCtx );

        // Then
        var outerProfileStatisticsProvider = outerCtx.kernelStatisticProvider();
        var innerProfileStatisticsProvider = openInnerCtx.kernelStatisticProvider();
        // Make sure we are not just summing up 0s
        assertThat( outerHits, greaterThan( 0L ) );
        assertThat( closedInnerHits, greaterThan( 0L ) );
        assertThat( openInnerHits, greaterThan( 0L ) );
        // Actual assertion
        assertThat( outerProfileStatisticsProvider.getPageCacheHits(), equalTo( outerHits ) );
        assertThat( outerProfileStatisticsProvider.getPageCacheMisses(), equalTo( outerFaults ) );
        assertThat( innerProfileStatisticsProvider.getPageCacheHits(), equalTo( openInnerHits ) );
        assertThat( innerProfileStatisticsProvider.getPageCacheMisses(), equalTo( openInnerFaults ) );
    }

    @Test
    void contextWithNewTransaction_executing_query_should_sum_up_page_hits_faults_from_inner_and_outer_transactions_also_when_rolled_back()
    {
        // Given

        // Add data to database so that we get page hits/faults
        graphOps.executeTransactionally( "CREATE (n)" );

        var outerTx = graph.beginTransaction( IMPLICIT, LoginContext.AUTH_DISABLED );
        var outerCtx = createTransactionContext( outerTx );
        var executingQuery = outerCtx.executingQuery();

        // When
        // Generate some page cache hits/faults
        generatePageCacheHits( outerCtx );
        var outerHits = getPageCacheHits( outerCtx );
        var outerFaults = getPageCacheFaults( outerCtx );

        var closedInnerHits = 0L;
        var closedInnerFaults = 0L;
        for ( int i = 0; i < 10; i++ )
        {
            var innerCtx = outerCtx.contextWithNewTransaction();
            //  Generate page cache hits/faults for half of the executions
            if ( i % 2 == 0 )
            {
                generatePageCacheHits( innerCtx );
            }
            closedInnerHits += getPageCacheHits( innerCtx );
            closedInnerFaults += getPageCacheFaults( innerCtx );
            innerCtx.rollback();
        }

        var openInnerCtx = outerCtx.contextWithNewTransaction();
        // Generate page cache hits/faults
        generatePageCacheHits( openInnerCtx );
        var openInnerHits = getPageCacheHits( openInnerCtx );
        var openInnerFaults = getPageCacheFaults( openInnerCtx );

        // Then
        var snapshot = executingQuery.snapshot();
        // Make sure we are not just summing up 0s
        assertThat( outerHits, greaterThan( 0L ) );
        assertThat( closedInnerHits, greaterThan( 0L ) );
        assertThat( openInnerHits, greaterThan( 0L ) );
        // Actual assertion
        assertThat( snapshot.pageHits(), equalTo( outerHits + closedInnerHits + openInnerHits ) );
        assertThat( snapshot.pageFaults(), equalTo( outerFaults + closedInnerFaults + openInnerFaults ) );
    }

    @Test
    void contextWithNewTransaction_executing_query_should_sum_up_active_locks_from_open_inner_and_outer_transactions()
    {
        // Given

        // Add data to database so that we can get locks
        graphOps.executeTransactionally( "CREATE (:A), (:B), (:C)" );

        var outerTx = graph.beginTransaction( IMPLICIT, LoginContext.AUTH_DISABLED );
        var outerCtx = createTransactionContext( outerTx );
        var executingQuery = outerCtx.executingQuery();

        // When
        // Get some locks
        getLocks( outerCtx, "A" );
        var outerActiveLocks = getActiveLockCount( outerCtx );

        // First inner tx
        var openInnerCtx1 = outerCtx.contextWithNewTransaction();
        // Get some locks
        getLocks( openInnerCtx1, "B" );
        var innerActiveLocks1 = getActiveLockCount( openInnerCtx1 );

        // Second inner tx
        var openInnerCtx2 = outerCtx.contextWithNewTransaction();
        // Get some locks
        getLocks( openInnerCtx2, "C" );
        var innerActiveLocks2 = getActiveLockCount( openInnerCtx2 );

        // Then
        var snapshot = executingQuery.snapshot();
        // Make sure we are not just summing up 0s
        assertThat( outerActiveLocks, greaterThan( 0L ) );
        assertThat( innerActiveLocks1, greaterThan( 0L ) );
        assertThat( innerActiveLocks2, greaterThan( 0L ) );
        // Actual assertion
        assertThat( snapshot.activeLockCount(), equalTo( outerActiveLocks + innerActiveLocks1 + innerActiveLocks2 ) );
    }

    @Test
    void contextWithNewTransaction_executing_query_should_sum_up_active_locks_from_open_inner_and_outer_transactions_but_not_from_closed_transactions()
    {
        // Given

        // Add data to database so that we get page hits/faults
        graphOps.executeTransactionally( "CREATE (:A), (:B), (:C), (:D)" );

        var outerTx = graph.beginTransaction( IMPLICIT, LoginContext.AUTH_DISABLED );
        var outerCtx = createTransactionContext( outerTx );
        var executingQuery = outerCtx.executingQuery();

        // When
        getLocks( outerCtx, "A" );
        var outerActiveLocks = getActiveLockCount( outerCtx );

        // Abort
        var innerCtxAbort = outerCtx.contextWithNewTransaction();
        // Get some locks
        getLocks( innerCtxAbort, "B" );
        var closedInnerActiveLocksAbort = getActiveLockCount( innerCtxAbort );
        innerCtxAbort.rollback();

        // Commit
        var innerCtxCommit = outerCtx.contextWithNewTransaction();
        // Get some locks
        getLocks( innerCtxCommit, "C" );
        var closedInnerActiveLocksCommit = getActiveLockCount( innerCtxCommit );
        innerCtxCommit.commit();


        // Leave open
        var innerCtxOpen = outerCtx.contextWithNewTransaction();
        // Get some locks
        getLocks( innerCtxOpen, "D" );
        var openInnerActiveLocks = getActiveLockCount( innerCtxOpen );

        // Then
        var snapshot = executingQuery.snapshot();
        // Make sure we are not just summing up 0s
        assertThat( outerActiveLocks, greaterThan( 0L ) );
        assertThat( closedInnerActiveLocksAbort, greaterThan( 0L ) );
        assertThat( closedInnerActiveLocksCommit, greaterThan( 0L ) );
        assertThat( openInnerActiveLocks, greaterThan( 0L ) );
        // Actual assertion
        assertThat( snapshot.activeLockCount(), equalTo( outerActiveLocks + openInnerActiveLocks ) );
    }

    @Test
    void contextWithNewTransaction_executing_query_should_calculate_high_water_mark_memory_usage_also_when_committed_in_query_snapshot()
    {
        // Given
        var openHighWaterMark = 3L;
        var outerHighWaterMark = 10L;

        var outerTx = graph.beginTransaction( IMPLICIT, LoginContext.AUTH_DISABLED );
        var outerTxMemoryTracker = outerTx.kernelTransaction().memoryTracker();
        var outerCtx = createTransactionContext( outerTx );
        var executingQuery = outerCtx.executingQuery();
        var queryMemoryTracker = QueryMemoryTracker.apply( MEMORY_TRACKING$.MODULE$ );
        var outerTxMemoryTrackerForOperatorProvider = queryMemoryTracker.newMemoryTrackerForOperatorProvider( outerTxMemoryTracker );

        // We allocate memory trough the same operator id, so it is easy for us to calculate how much memory the GrowingArray of MemoryTrackerPerOperator takes
        var operatorId = 0;
        var localMem = new LocalMemoryTracker();
        var ga = new GrowingArray<>( localMem );
        ga.computeIfAbsent( operatorId, Object::new );
        var growingArraySize = localMem.heapHighWaterMark();

        // Start query execution
        executingQuery.onObfuscatorReady( QueryObfuscator.PASSTHROUGH );
        executingQuery.onCompilationCompleted( null, null );
        executingQuery.onExecutionStarted( queryMemoryTracker );

        // Some operator in outer transaction allocates some memory
        outerTxMemoryTrackerForOperatorProvider.memoryTrackerForOperator( operatorId ).allocateHeap( outerHighWaterMark );

        // When
        var innerHighWaterMark = 0L;
        for ( int i = 0; i < 10; i++ )
        {
            TransactionalContext innerCtx = outerCtx.contextWithNewTransaction();
            var innerTxMemoryTracker = innerCtx.kernelTransaction().memoryTracker();
            var innerTxMemoryTrackerForOperatorProvider = queryMemoryTracker.newMemoryTrackerForOperatorProvider( innerTxMemoryTracker );
            var operatorMemoryTracker = innerTxMemoryTrackerForOperatorProvider.memoryTrackerForOperator( operatorId );
            // Inner transaction allocates some memory for half of the executions
            var accHighWaterMark = 0;
            if ( i % 2 == 0 )
            {
                operatorMemoryTracker.allocateHeap( i );
                operatorMemoryTracker.releaseHeap( i );
                accHighWaterMark = i;
            }
            innerCtx.commit();
            innerHighWaterMark = Math.max( innerHighWaterMark, accHighWaterMark );
        }

        var openCtx = outerCtx.contextWithNewTransaction();
        var openTxMemoryTracker = openCtx.kernelTransaction().memoryTracker();
        var innerTxMemoryTrackerForOperatorProvider = queryMemoryTracker.newMemoryTrackerForOperatorProvider( openTxMemoryTracker );
        innerTxMemoryTrackerForOperatorProvider.memoryTrackerForOperator( operatorId ).allocateHeap( openHighWaterMark );

        // Then
        var snapshot = executingQuery.snapshot();
        var snapshotBytes = snapshot.allocatedBytes();
        var profilingBytes = queryMemoryTracker.heapHighWaterMark();
        assertThat( snapshotBytes, equalTo( growingArraySize + outerHighWaterMark + Math.max( innerHighWaterMark, openHighWaterMark ) ) );
        assertThat( profilingBytes, equalTo( snapshotBytes ) );
    }

    @Test
    void contextWithNewTransaction_throws_after_transaction_terminate()
    {
        // Given
        var tx = graph.beginTransaction( IMPLICIT, LoginContext.AUTH_DISABLED );
        var ctx = createTransactionContext( tx );

        // When
        tx.kernelTransaction().markForTermination( Status.Transaction.Terminated );

        // Then
        assertThrows( TransactionTerminatedException.class, ctx::contextWithNewTransaction );
    }

    @Test
    void contextWithNewTransaction_terminate_inner_context_after_outer_transaction_terminate()
    {
        // Given
        var outerTx = graph.beginTransaction( IMPLICIT, LoginContext.AUTH_DISABLED );
        var ctx = createTransactionContext( outerTx );
        var innerCtx = ctx.contextWithNewTransaction();

        // When
        ctx.kernelTransaction().markForTermination( Status.Transaction.Terminated );

        // Then
        assertTrue( innerCtx.transaction().terminationReason().isPresent() );
    }

    @Test
    void contextWithNewTransaction_deregister_inner_transaction_on_inner_commit()
    {
        // Given
        var outerTx = graph.beginTransaction( IMPLICIT, LoginContext.AUTH_DISABLED );
        var ctx = createTransactionContext( outerTx );
        var innerCtx = ctx.contextWithNewTransaction();

        // When
        innerCtx.commit();

        // Then
        assertFalse( ctx.transaction().hasInnerTransactions() );
    }

    @Test
    void contextWithNewTransaction_deregister_inner_transaction_on_inner_rollback()
    {
        // Given
        var outerTx = graph.beginTransaction( IMPLICIT, LoginContext.AUTH_DISABLED );
        var ctx = createTransactionContext( outerTx );
        var innerCtx = ctx.contextWithNewTransaction();

        // When
        innerCtx.rollback();

        // Then
        assertFalse( ctx.transaction().hasInnerTransactions() );
    }

    @Test
    void contextWithNewTransaction_deregister_inner_transaction_on_inner_close()
    {
        // Given
        var outerTx = graph.beginTransaction( IMPLICIT, LoginContext.AUTH_DISABLED );
        var ctx = createTransactionContext( outerTx );
        var innerCtx = ctx.contextWithNewTransaction();

        // When
        innerCtx.transaction().close();

        // Then
        assertFalse( ctx.transaction().hasInnerTransactions() );
    }

    @Test
    void contextWithNewTransaction_do_not_terminate_outer_context_after_inner_transaction_terminate()
    {
        // Given
        var outerTx = graph.beginTransaction( IMPLICIT, LoginContext.AUTH_DISABLED );
        var ctx = createTransactionContext( outerTx );
        var innerCtx = ctx.contextWithNewTransaction();

        // When
        innerCtx.kernelTransaction().markForTermination( Status.Transaction.Terminated );

        // Then
        assertTrue( ctx.transaction().terminationReason().isEmpty() );
    }

    @Disabled("Strictly speaking this does not need to work, but it would protect us from our own programming mistakes in Cypher")
    @Test
    void contextWithNewTransaction_close_inner_context_on_rollback_of_outer_context()
    {
        // Given
        var outerTx = graph.beginTransaction( IMPLICIT, LoginContext.AUTH_DISABLED );
        var ctx = createTransactionContext( outerTx );
        var innerCtx = ctx.contextWithNewTransaction();

        // When
        ctx.rollback();

        // Then
        assertFalse( innerCtx.isOpen() );
    }

    @Test
    void contextWithNewTransaction_do_not_close_outer_context_on_rollback_of_inner_context()
    {
        // Given
        var outerTx = graph.beginTransaction( IMPLICIT, LoginContext.AUTH_DISABLED );
        var ctx = createTransactionContext( outerTx );
        var innerCtx = ctx.contextWithNewTransaction();

        // When
        innerCtx.rollback();

        // Then
        assertTrue( ctx.isOpen() );
    }

    // PERIODIC COMMIT

    @Test
    void periodic_commit_query_should_sum_up_page_hits_faults_from_first_and_second_transaction_in_query_snapshot()
    {
        // Given

        // Add data to database so that we get page hits/faults
        graphOps.executeTransactionally( "CREATE (n)" );

        var outerTx = graph.beginTransaction( IMPLICIT, LoginContext.AUTH_DISABLED );
        var ctx = createTransactionContext( outerTx );
        var executingQuery = ctx.executingQuery();

        // When
        var closedTxHits = 0L;
        var closedTxFaults = 0L;
        for ( int i = 0; i < 10; i++ )
        {
            //  Generate page cache hits/faults for half of the executions
            if ( i % 2 == 0 )
            {
                generatePageCacheHits( ctx );
            }
            closedTxHits += getPageCacheHits( ctx );
            closedTxFaults += getPageCacheFaults( ctx );
            ctx.commitAndRestartTx();
        }

        generatePageCacheHits( ctx );

        var lastHits = getPageCacheHits( ctx );
        var lastFaults = getPageCacheFaults( ctx );
        var lastTx = ctx.transaction();

        // Then
        var snapshot = executingQuery.snapshot();
        // Make sure we are not just summing up 0s
        assertThat( closedTxHits, greaterThan( 0L ) );
        assertThat( lastHits, greaterThan( 0L ) );
        // Actual assertion
        assertThat( snapshot.transactionId(), equalTo( lastTx.kernelTransaction().getUserTransactionId() ) );
        assertThat( snapshot.pageHits(), equalTo( closedTxHits + lastHits ) );
        assertThat( snapshot.pageFaults(), equalTo( closedTxFaults + lastFaults ) );
    }

    @Test
    void periodic_commit_query_should_sum_up_page_hits_faults_from_first_and_second_transaction_in_PROFILE()
    {
        // Given

        // Add data to database so that we get page hits/faults
        graphOps.executeTransactionally( "CREATE (n)" );

        var outerTx = graph.beginTransaction( IMPLICIT, LoginContext.AUTH_DISABLED );
        var ctx = createTransactionContext( outerTx );
        var executingQuery = ctx.executingQuery();

        var closedTxHits = 0L;
        var closedTxFaults = 0L;
        for ( int i = 0; i < 10; i++ )
        {
            //  Generate page cache hits/faults for half of the executions
            if ( i % 2 == 0 )
            {
                generatePageCacheHits( ctx );
            }
            closedTxHits += getPageCacheHits( ctx );
            closedTxFaults += getPageCacheFaults( ctx );
            ctx.commitAndRestartTx();
        }

        generatePageCacheHits( ctx );

        var lastHits = getPageCacheHits( ctx );
        var lastFaults = getPageCacheFaults( ctx );

        // Then
        var profileStatisticsProvider = ctx.kernelStatisticProvider();
        // Make sure we are not just summing up 0s
        assertThat( closedTxHits, greaterThan( 0L ) );
        assertThat( lastHits, greaterThan( 0L ) );
        // Actual assertion
        assertThat( profileStatisticsProvider.getPageCacheHits(), equalTo( closedTxHits + lastHits ) );
        assertThat( profileStatisticsProvider.getPageCacheMisses(), equalTo( closedTxFaults + lastFaults ) );
    }
}
