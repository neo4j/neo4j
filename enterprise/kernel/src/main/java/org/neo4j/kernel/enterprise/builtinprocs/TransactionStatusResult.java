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

import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.neo4j.kernel.api.KernelTransactionHandle;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.query.QuerySnapshot;
import org.neo4j.kernel.impl.api.TransactionExecutionStatistic;
import org.neo4j.kernel.impl.query.clientconnection.ClientConnectionInfo;

import static java.lang.String.format;
import static org.neo4j.kernel.enterprise.builtinprocs.QueryId.ofInternalId;

@SuppressWarnings( "WeakerAccess" )
public class TransactionStatusResult
{
    private static final String RUNNINS_STATE = "Running";
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
    public final long pageHits;
    public final long pageFaults;

    public TransactionStatusResult( KernelTransactionHandle transaction,
            TransactionDependenciesResolver transactionDependenciesResolver,
            Map<KernelTransactionHandle,List<QuerySnapshot>> handleSnapshotsMap ) throws InvalidArgumentsException
    {
        this.transactionId = transaction.getUserTransactionName();
        this.username = transaction.securityContext().subject().username();
        this.startTime = ProceduresTimeFormatHelper.formatTime( transaction.startTime() );
        Optional<Status> terminationReason = transaction.terminationReason();
        this.activeLockCount = transaction.activeLocks().count();
        List<QuerySnapshot> querySnapshots = handleSnapshotsMap.get( transaction );
        TransactionExecutionStatistic statistic = transaction.transactionStatistic();
        elapsedTimeMillis = statistic.getElapsedTimeMillis();
        cpuTimeMillis = statistic.getCpuTimeMillis();
        allocatedBytes = statistic.getHeapAllocateBytes();
        waitTimeMillis = statistic.getWaitTimeMillis();
        idleTimeMillis = statistic.getIdleTimeMillis();
        pageHits = statistic.getPageHits();
        pageFaults = statistic.getPageFaults();

        if ( !querySnapshots.isEmpty() )
        {
            QuerySnapshot snapshot = querySnapshots.get( 0 );
            ClientConnectionInfo clientConnectionInfo = snapshot.clientConnection();
            this.currentQueryId = ofInternalId( snapshot.internalQueryId() ).toString();
            this.currentQuery = snapshot.queryText();
            this.protocol = clientConnectionInfo.protocol();
            this.clientAddress = clientConnectionInfo.clientAddress();
            this.requestUri = clientConnectionInfo.requestURI();
        }
        else
        {
            this.currentQueryId = StringUtils.EMPTY;
            this.currentQuery = StringUtils.EMPTY;
            this.protocol = StringUtils.EMPTY;
            this.clientAddress = StringUtils.EMPTY;
            this.requestUri = StringUtils.EMPTY;
        }
        this.resourceInformation = transactionDependenciesResolver.describeBlockingLocks( transaction );
        this.status = getStatus( transaction, terminationReason, transactionDependenciesResolver );
        this.metaData = transaction.getMetaData();
    }

    private String getStatus( KernelTransactionHandle handle, Optional<Status> terminationReason,
            TransactionDependenciesResolver transactionDependenciesResolver )
    {
        return terminationReason.map( reason -> format( TERMINATED_STATE, reason.code() ) )
                .orElseGet( () -> getExecutingStatus( handle, transactionDependenciesResolver ) );
    }

    private String getExecutingStatus( KernelTransactionHandle handle,
            TransactionDependenciesResolver transactionDependenciesResolver )
    {
        return transactionDependenciesResolver.isBlocked( handle ) ? "Blocked by: " +
                transactionDependenciesResolver.describeBlockingTransactions( handle ) : RUNNINS_STATE;
    }
}
