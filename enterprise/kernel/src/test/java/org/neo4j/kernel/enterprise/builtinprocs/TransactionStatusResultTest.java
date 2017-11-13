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
package org.neo4j.kernel.enterprise.builtinprocs;

import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Stream;

import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.KernelTransactionHandle;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.api.query.ExecutingQuery;
import org.neo4j.kernel.api.query.QuerySnapshot;
import org.neo4j.kernel.impl.api.KernelTransactionImplementation;
import org.neo4j.kernel.impl.api.TestKernelTransactionHandle;
import org.neo4j.kernel.impl.api.TransactionExecutionStatistic;
import org.neo4j.kernel.impl.locking.ActiveLock;
import org.neo4j.kernel.impl.locking.ResourceTypes;
import org.neo4j.kernel.impl.query.clientconnection.HttpConnectionInfo;
import org.neo4j.resources.CpuClock;
import org.neo4j.resources.HeapAllocation;
import org.neo4j.time.Clocks;
import org.neo4j.time.SystemNanoClock;
import org.neo4j.values.virtual.VirtualValues;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.commons.lang3.StringUtils.EMPTY;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
                new TransactionStatusResult( transactionHandle, blockerResolver, snapshotsMap );

        checkTransactionStatus( statusResult, "testQuery", "query-7" );
    }

    @Test
    public void statusOfTransactionWithoutRunningQuery() throws InvalidArgumentsException
    {
        snapshotsMap.put( transactionHandle, emptyList() );
        TransactionStatusResult statusResult =
                new TransactionStatusResult( transactionHandle, blockerResolver, snapshotsMap );

        checkTransactionStatusWithoutQueries( statusResult );
    }

    @Test
    public void statusOfTransactionWithMultipleQueries() throws InvalidArgumentsException
    {
        snapshotsMap.put( transactionHandle, asList( createQuerySnapshot( 7L ), createQuerySnapshot( 8L ) ) );
        TransactionStatusResult statusResult =
                new TransactionStatusResult( transactionHandle, blockerResolver, snapshotsMap );

        checkTransactionStatus( statusResult, "testQuery", "query-7" );
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
        assertEquals( Long.valueOf( 0 ), statusResult.allocatedBytes );
        assertEquals( 0L, statusResult.pageHits );
        assertEquals( 0L, statusResult.pageFaults );
    }

    private void checkTransactionStatus( TransactionStatusResult statusResult, String currentQuery,
            String currentQueryId )
    {
        assertEquals( "transaction-8", statusResult.transactionId );
        assertEquals( "testUser", statusResult.username );
        assertEquals( Collections.emptyMap(), statusResult.metaData );
        assertEquals( "1970-01-01T00:00:01.984Z", statusResult.startTime );
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
        assertEquals( Long.valueOf( 0 ), statusResult.allocatedBytes );
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
                new CountingSystemNanoClock(), new CountingCpuClock(), new CountingHeapAllocation() );
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
        public Stream<? extends ActiveLock> activeLocks()
        {
            return Stream.of( ActiveLock.sharedLock( ResourceTypes.NODE, 3 ) );
        }

        @Override
        public TransactionExecutionStatistic transactionStatistic()
        {
            KernelTransactionImplementation transaction = mock( KernelTransactionImplementation.class );
            KernelTransactionImplementation.Statistics statistics =
                    new KernelTransactionImplementation.Statistics( transaction, new CountingCpuClock(), new CountingHeapAllocation() );
            when( transaction.getStatistics() ).thenReturn(
                    statistics );
            return new TransactionExecutionStatistic( transaction, Clocks.fakeClock().forward( 2010, MILLISECONDS ),
                    200 );
        }
    }

    private static class CountingSystemNanoClock extends SystemNanoClock
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
