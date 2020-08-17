/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.procedure.builtin;

import java.time.ZoneId;
import java.util.Map;
import java.util.Optional;

import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo;
import org.neo4j.kernel.api.KernelTransactionHandle;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.api.query.QuerySnapshot;
import org.neo4j.kernel.impl.api.TransactionExecutionStatistic;

import static java.lang.String.format;
import static org.apache.commons.lang3.StringUtils.EMPTY;

@SuppressWarnings( "WeakerAccess" )
public class TransactionStatusResult
{
    private static final String RUNNING_STATE = "Running";
    private static final String CLOSING_STATE = "Closing";
    private static final String BLOCKED_STATE = "Blocked by: ";
    private static final String TERMINATED_STATE = "Terminated with reason: %s";

    public final String transactionId;
    public final String username;
    public final Map<String,Object> metaData;
    public final String startTime;
    public final String protocol;
    public final String clientAddress;
    public final String requestUri;

    public final String currentQueryId;
    public final String currentQuery;

    public final long activeLockCount;
    public final String status;
    public Map<String,Object> resourceInformation;

    public final long elapsedTimeMillis;
    public final Long cpuTimeMillis;
    public final long waitTimeMillis;
    public final Long idleTimeMillis;
    public final Long allocatedBytes;
    public final Long allocatedDirectBytes;
    public final long pageHits;
    public final long pageFaults;
    /** @since Neo4j 3.5 */
    public final String connectionId;
    public final String initializationStackTrace;
    /** @since Neo4j 4.0 */
    public final String database;
    /** @since Neo4j 4.1 */
    public final Long estimatedUsedHeapMemory;

    public TransactionStatusResult( String database, KernelTransactionHandle transaction,
            TransactionDependenciesResolver transactionDependenciesResolver,
            Map<KernelTransactionHandle,Optional<QuerySnapshot>> handleSnapshotsMap, ZoneId zoneId ) throws InvalidArgumentsException
    {
        this.database = database;
        this.transactionId = new TransactionId( database, transaction.getUserTransactionId() ).toString();
        this.username = transaction.subject().username();
        this.startTime = ProceduresTimeFormatHelper.formatTime( transaction.startTime(), zoneId );
        this.activeLockCount = transaction.activeLocks().count();
        Optional<QuerySnapshot> querySnapshot = handleSnapshotsMap.get( transaction );
        TransactionExecutionStatistic statistic = transaction.transactionStatistic();
        elapsedTimeMillis = statistic.getElapsedTimeMillis();
        cpuTimeMillis = statistic.getCpuTimeMillis();
        allocatedBytes = statistic.getHeapAllocatedBytes();
        allocatedDirectBytes = statistic.getNativeAllocatedBytes();
        estimatedUsedHeapMemory = statistic.getEstimatedUsedHeapMemory();
        waitTimeMillis = statistic.getWaitTimeMillis();
        idleTimeMillis = statistic.getIdleTimeMillis();
        pageHits = statistic.getPageHits();
        pageFaults = statistic.getPageFaults();

        if ( querySnapshot.isPresent() )
        {
            QuerySnapshot snapshot = querySnapshot.get();
            this.currentQueryId = new QueryId( snapshot.internalQueryId() ).toString();
            this.currentQuery = snapshot.obfuscatedQueryText().orElse( null );
        }
        else
        {
            this.currentQueryId = EMPTY;
            this.currentQuery = EMPTY;
        }

        var clientInfo = transaction.clientInfo();
        this.protocol = clientInfo.map( ClientConnectionInfo::protocol ).orElse( EMPTY );
        this.clientAddress = clientInfo.map( ClientConnectionInfo::clientAddress ).orElse( EMPTY );
        this.requestUri = clientInfo.map( ClientConnectionInfo::requestURI ).orElse( EMPTY ) ;
        this.connectionId = clientInfo.map( ClientConnectionInfo::connectionId ).orElse( EMPTY );
        this.resourceInformation = transactionDependenciesResolver.describeBlockingLocks( transaction );
        this.status = getStatus( transaction, transactionDependenciesResolver );
        this.metaData = transaction.getMetaData();
        this.initializationStackTrace = transaction.transactionInitialisationTrace().getTrace();
    }

    private static String getStatus( KernelTransactionHandle handle, TransactionDependenciesResolver transactionDependenciesResolver )
    {
        return handle.terminationReason().map( reason -> format( TERMINATED_STATE, reason.code() ) )
                .orElseGet( () -> getExecutingStatus( handle, transactionDependenciesResolver ) );
    }

    private static String getExecutingStatus( KernelTransactionHandle handle, TransactionDependenciesResolver transactionDependenciesResolver )
    {
        if ( transactionDependenciesResolver.isBlocked( handle ) )
        {
            return BLOCKED_STATE + transactionDependenciesResolver.describeBlockingTransactions( handle );
        }
        else if ( handle.isClosing() )
        {
            return CLOSING_STATE;
        }
        return RUNNING_STATE;
    }
}
