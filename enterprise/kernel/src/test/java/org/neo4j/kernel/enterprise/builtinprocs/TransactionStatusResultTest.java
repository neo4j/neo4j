/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.kernel.enterprise.builtinprocs;

import org.junit.Test;

import java.net.InetSocketAddress;
import java.time.ZoneId;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import org.neo4j.collection.pool.Pool;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracerSupplier;
import org.neo4j.io.pagecache.tracing.cursor.context.EmptyVersionContextSupplier;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.KernelTransactionHandle;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.api.explicitindex.AutoIndexing;
import org.neo4j.kernel.api.query.ExecutingQuery;
import org.neo4j.kernel.api.query.QuerySnapshot;
import org.neo4j.kernel.api.txstate.ExplicitIndexTransactionState;
import org.neo4j.kernel.impl.api.KernelTransactionImplementation;
import org.neo4j.kernel.impl.api.SchemaState;
import org.neo4j.kernel.impl.api.SchemaWriteGuard;
import org.neo4j.kernel.impl.api.StatementOperationParts;
import org.neo4j.kernel.impl.api.TestKernelTransactionHandle;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.api.TransactionExecutionStatistic;
import org.neo4j.kernel.impl.api.TransactionHooks;
import org.neo4j.kernel.impl.api.index.IndexProviderMap;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.state.ConstraintIndexCreator;
import org.neo4j.kernel.impl.constraints.StandardConstraintSemantics;
import org.neo4j.kernel.impl.factory.CanWrite;
import org.neo4j.kernel.impl.index.ExplicitIndexStore;
import org.neo4j.kernel.impl.locking.ActiveLock;
import org.neo4j.kernel.impl.locking.LockTracer;
import org.neo4j.kernel.impl.locking.ResourceTypes;
import org.neo4j.kernel.impl.newapi.DefaultCursors;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.impl.query.clientconnection.HttpConnectionInfo;
import org.neo4j.kernel.impl.transaction.TransactionHeaderInformationFactory;
import org.neo4j.kernel.impl.transaction.TransactionStats;
import org.neo4j.kernel.impl.transaction.tracing.TransactionTracer;
import org.neo4j.resources.CpuClock;
import org.neo4j.resources.HeapAllocation;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.time.Clocks;
import org.neo4j.time.SystemNanoClock;
import org.neo4j.values.virtual.VirtualValues;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.commons.lang3.StringUtils.EMPTY;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.mock;
import static org.neo4j.kernel.impl.util.collection.CollectionsFactorySupplier.ON_HEAP;

public class TransactionStatusResultTest
{

    private TestKernelTransactionHandle transactionHandle =
            new TransactionHandleWithLocks( new StubKernelTransaction() );
    private HashMap<KernelTransactionHandle,List<QuerySnapshot>> snapshotsMap = new HashMap<>();
    private TransactionDependenciesResolver blockerResolver = new TransactionDependenciesResolver( snapshotsMap );

    @Test
    public void statusOfTransactionWithSingleQuery() throws InvalidArgumentsException
    {
        snapshotsMap.put( transactionHandle, singletonList( createQuerySnapshot( 7L ) ) );
        TransactionStatusResult statusResult =
                new TransactionStatusResult( transactionHandle, blockerResolver, snapshotsMap, ZoneId.of( "UTC" ) );

        checkTransactionStatus( statusResult, "testQuery", "query-7", "1970-01-01T00:00:01.984Z" );
    }

    @Test
    public void statusOfTransactionWithoutRunningQuery() throws InvalidArgumentsException
    {
        snapshotsMap.put( transactionHandle, emptyList() );
        TransactionStatusResult statusResult =
                new TransactionStatusResult( transactionHandle, blockerResolver, snapshotsMap, ZoneId.of( "UTC" ) );

        checkTransactionStatusWithoutQueries( statusResult );
    }

    @Test
    public void statusOfTransactionWithMultipleQueries() throws InvalidArgumentsException
    {
        snapshotsMap.put( transactionHandle, asList( createQuerySnapshot( 7L ), createQuerySnapshot( 8L ) ) );
        TransactionStatusResult statusResult =
                new TransactionStatusResult( transactionHandle, blockerResolver, snapshotsMap, ZoneId.of( "UTC" ) );

        checkTransactionStatus( statusResult, "testQuery", "query-7", "1970-01-01T00:00:01.984Z" );
    }

    @Test
    public void statusOfTransactionWithDifferentTimeZone() throws InvalidArgumentsException
    {
        snapshotsMap.put( transactionHandle, singletonList( createQuerySnapshot( 7L ) ) );
        TransactionStatusResult statusResult =
                new TransactionStatusResult( transactionHandle, blockerResolver, snapshotsMap, ZoneId.of( "UTC+1" ) );

        checkTransactionStatus( statusResult, "testQuery", "query-7", "1970-01-01T01:00:01.984+01:00" );
    }

    private void checkTransactionStatusWithoutQueries( TransactionStatusResult statusResult )
    {
        assertEquals( "transaction-8", statusResult.transactionId );
        assertEquals( "testUser", statusResult.username );
        assertEquals( Collections.emptyMap(), statusResult.metaData );
        assertEquals( "1970-01-01T00:00:01.984Z", statusResult.startTime );
        assertEquals( EMPTY, statusResult.protocol );
        assertEquals( EMPTY, statusResult.clientAddress );
        assertEquals( EMPTY, statusResult.requestUri );
        assertEquals( EMPTY, statusResult.currentQueryId );
        assertEquals( EMPTY, statusResult.currentQuery );
        assertEquals( 1, statusResult.activeLockCount );
        assertEquals( "Running", statusResult.status );
        assertEquals( Collections.emptyMap(), statusResult.resourceInformation );
        assertEquals( 1810L, statusResult.elapsedTimeMillis );
        assertEquals( Long.valueOf( 1L ), statusResult.cpuTimeMillis );
        assertEquals( 0L, statusResult.waitTimeMillis );
        assertEquals( Long.valueOf( 1809 ), statusResult.idleTimeMillis );
        assertEquals( Long.valueOf( 1 ), statusResult.allocatedBytes );
        assertEquals( Long.valueOf( 0 ), statusResult.allocatedDirectBytes );
        assertEquals( 0L, statusResult.pageHits );
        assertEquals( 0L, statusResult.pageFaults );
    }

    private void checkTransactionStatus( TransactionStatusResult statusResult, String currentQuery,
            String currentQueryId, String startTime )
    {
        assertEquals( "transaction-8", statusResult.transactionId );
        assertEquals( "testUser", statusResult.username );
        assertEquals( Collections.emptyMap(), statusResult.metaData );
        assertEquals( startTime, statusResult.startTime );
        assertEquals( "https", statusResult.protocol );
        assertEquals( "localhost:1000", statusResult.clientAddress );
        assertEquals( "https://localhost:1001/path", statusResult.requestUri );
        assertEquals( currentQueryId, statusResult.currentQueryId );
        assertEquals( currentQuery, statusResult.currentQuery );
        assertEquals( 1, statusResult.activeLockCount );
        assertEquals( "Running", statusResult.status );
        assertEquals( Collections.emptyMap(), statusResult.resourceInformation );
        assertEquals( 1810, statusResult.elapsedTimeMillis );
        assertEquals( Long.valueOf( 1 ), statusResult.cpuTimeMillis );
        assertEquals( 0L, statusResult.waitTimeMillis );
        assertEquals( Long.valueOf( 1809 ), statusResult.idleTimeMillis );
        assertEquals( Long.valueOf( 1 ), statusResult.allocatedBytes );
        assertEquals( Long.valueOf( 0 ), statusResult.allocatedDirectBytes );
        assertEquals( 0, statusResult.pageHits );
        assertEquals( 0, statusResult.pageFaults );
    }

    private QuerySnapshot createQuerySnapshot( long queryId )
    {
        ExecutingQuery executingQuery = createExecutingQuery( queryId );
        return executingQuery.snapshot();
    }

    private ExecutingQuery createExecutingQuery( long queryId )
    {
        return new ExecutingQuery( queryId, getTestConnectionInfo(), "testUser", "testQuery", VirtualValues.EMPTY_MAP,
                Collections.emptyMap(), () -> 1L, PageCursorTracer.NULL,
                Thread.currentThread().getId(), Thread.currentThread().getName(),
                new CountingNanoClock(), new CountingCpuClock(), new CountingHeapAllocation() );
    }

    private HttpConnectionInfo getTestConnectionInfo()
    {
        return new HttpConnectionInfo( "https", "agent", new InetSocketAddress( "localhost", 1000 ),
                new InetSocketAddress( "localhost", 1001 ), "/path" );
    }

    private static class TransactionHandleWithLocks extends TestKernelTransactionHandle
    {

        TransactionHandleWithLocks( KernelTransaction tx )
        {
            super( tx );
        }

        @Override
        public Stream<ActiveLock> activeLocks()
        {
            return Stream.of( ActiveLock.sharedLock( ResourceTypes.NODE, 3 ) );
        }

        @Override
        public TransactionExecutionStatistic transactionStatistic()
        {
            KernelTransactionImplementation transaction = new KernelTransactionImplementation(
                        mock( StatementOperationParts.class ), mock( SchemaWriteGuard.class ), new TransactionHooks(),
                        mock( ConstraintIndexCreator.class ), new Procedures(), TransactionHeaderInformationFactory.DEFAULT,
                        mock( TransactionCommitProcess.class ), new TransactionStats(), () -> mock( ExplicitIndexTransactionState.class ),
                        mock( Pool.class ), Clocks.fakeClock(),
                        new AtomicReference<>( CpuClock.NOT_AVAILABLE ), new AtomicReference<>( HeapAllocation.NOT_AVAILABLE ),
                        TransactionTracer.NULL,
                        LockTracer.NONE, PageCursorTracerSupplier.NULL,
                        mock( StorageEngine.class, RETURNS_MOCKS ), new CanWrite(),
                        mock( DefaultCursors.class ), AutoIndexing.UNSUPPORTED, mock( ExplicitIndexStore.class ),
                        EmptyVersionContextSupplier.EMPTY, ON_HEAP, new StandardConstraintSemantics(), mock( SchemaState.class),
                        mock( IndexingService.class ), mock( IndexProviderMap.class ) )
            {
                @Override
                public Statistics getStatistics()
                {
                    TestStatistics statistics = new TestStatistics( this, new AtomicReference<>( new CountingCpuClock() ),
                                    new AtomicReference<>( new CountingHeapAllocation() ) );
                    statistics.init( Thread.currentThread().getId(), PageCursorTracer.NULL );
                    return statistics;
                }
            };
            return new TransactionExecutionStatistic( transaction, Clocks.fakeClock().forward( 2010, MILLISECONDS ), 200 );
        }
    }

    private static class TestStatistics extends KernelTransactionImplementation.Statistics
    {
        @Override
        protected void init( long threadId, PageCursorTracer pageCursorTracer )
        {
            super.init( threadId, pageCursorTracer );
        }

        TestStatistics( KernelTransactionImplementation transaction, AtomicReference<CpuClock> cpuClockRef,
                AtomicReference<HeapAllocation> heapAllocationRef )
        {
            super( transaction, cpuClockRef, heapAllocationRef );
        }
    }

    private static class CountingNanoClock extends SystemNanoClock
    {
        private long time;

        @Override
        public long nanos()
        {
            time += MILLISECONDS.toNanos( 1 );
            return time;
        }
    }

    private static class CountingCpuClock extends CpuClock
    {
        private long cpuTime;

        @Override
        public long cpuTimeNanos( long threadId )
        {
            cpuTime += MILLISECONDS.toNanos( 1 );
            return cpuTime;
        }
    }

    private static class CountingHeapAllocation extends HeapAllocation
    {
        private long allocatedBytes;

        @Override
        public long allocatedBytes( long threadId )
        {
            return allocatedBytes++;
        }
    }
}
