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
package org.neo4j.causalclustering.readreplica;

import java.io.IOException;
import java.util.concurrent.locks.LockSupport;

import org.neo4j.causalclustering.catchup.storecopy.CopiedStoreRecovery;
import org.neo4j.causalclustering.catchup.storecopy.LocalDatabase;
import org.neo4j.causalclustering.catchup.storecopy.StoreCopyFailedException;
import org.neo4j.causalclustering.catchup.storecopy.StoreFetcher;
import org.neo4j.causalclustering.catchup.storecopy.StoreIdDownloadFailedException;
import org.neo4j.causalclustering.catchup.storecopy.StreamingTransactionsFailedException;
import org.neo4j.causalclustering.core.state.machines.tx.RetryStrategy;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.identity.StoreId;
import org.neo4j.causalclustering.messaging.routing.CoreMemberSelectionException;
import org.neo4j.causalclustering.messaging.routing.CoreMemberSelectionStrategy;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.lang.String.format;

class ReadReplicaStartupProcess implements Lifecycle
{
    private static final int MAX_ATTEMPTS = 5;

    private final FileSystemAbstraction fs;
    private final StoreFetcher storeFetcher;
    private final LocalDatabase localDatabase;
    private final Lifecycle txPulling;
    private final CoreMemberSelectionStrategy connectionStrategy;
    private final Log debugLog;
    private final Log userLog;
    private final RetryStrategy.Timeout timeout;

    private final CopiedStoreRecovery copiedStoreRecovery;
    private String lastIssue;

    ReadReplicaStartupProcess( FileSystemAbstraction fs, StoreFetcher storeFetcher, LocalDatabase localDatabase,
            Lifecycle txPulling, CoreMemberSelectionStrategy connectionStrategy, RetryStrategy retryStrategy,
            LogProvider debugLogProvider, LogProvider userLogProvider, CopiedStoreRecovery copiedStoreRecovery )
    {
        this.fs = fs;
        this.storeFetcher = storeFetcher;
        this.localDatabase = localDatabase;
        this.txPulling = txPulling;
        this.connectionStrategy = connectionStrategy;
        this.copiedStoreRecovery = copiedStoreRecovery;
        this.timeout = retryStrategy.newTimeout();
        this.debugLog = debugLogProvider.getLog( getClass() );
        this.userLog = userLogProvider.getLog( getClass() );
    }

    @Override
    public void init() throws Throwable
    {
        localDatabase.init();
        txPulling.init();
    }

    private String issueOf( String operation, int attempt )
    {
        return format( "Failed attempt %d of %s", attempt, operation );
    }

    @Override
    public void start() throws IOException
    {
        long retryInterval = 5_000;
        boolean syncedWithCore = false;
        for ( int attempt = 1; attempt <= MAX_ATTEMPTS && !syncedWithCore; attempt++ )
        {
            MemberId source = findCoreMemberToCopyFrom();
            try
            {
                syncStoreWithCore( source );
                syncedWithCore = true;
            }
            catch ( StoreCopyFailedException e )
            {
                lastIssue = issueOf( format( "copying store files from %s", source ), attempt );
                debugLog.warn( lastIssue );
            }
            catch ( StreamingTransactionsFailedException e )
            {
                lastIssue = issueOf( format( "streaming transactions from %s", source ), attempt );
                debugLog.warn( lastIssue );
            }
            catch ( StoreIdDownloadFailedException e )
            {
                lastIssue = issueOf( format( "getting store id from %s", source ), attempt );
                debugLog.warn( lastIssue );
            }

            try
            {
                Thread.sleep( retryInterval );
                retryInterval = Math.min( 60_000, retryInterval * 2 );
            }
            catch ( InterruptedException e )
            {
                Thread.interrupted();
                lastIssue = "Interrupted while trying to start read replica";
                debugLog.warn( lastIssue );
                break;
            }

            attempt++;
        }

        if ( !syncedWithCore )
        {
            userLog.error( lastIssue );
            throw new RuntimeException( lastIssue );
        }

        try
        {
            localDatabase.start();
            txPulling.start();
        }
        catch ( Throwable e )
        {
            throw new RuntimeException( e );
        }
    }

    private void syncStoreWithCore( MemberId source ) throws IOException, StoreIdDownloadFailedException,
            StoreCopyFailedException, StreamingTransactionsFailedException
    {
        if ( localDatabase.isEmpty() )
        {
            debugLog.info( "Local database is empty, attempting to replace with copy from core server %s", source );

            debugLog.info( "Finding store id of core server %s", source );
            StoreId storeId = storeFetcher.getStoreIdOf( source );

            debugLog.info( "Copying store from core server %s", source );
            localDatabase.delete();
            new CopyStoreSafely( fs, localDatabase, copiedStoreRecovery, debugLog )
                    .copyWholeStoreFrom( source, storeId, storeFetcher );

            debugLog.info( "Restarting local database after copy.", source );
        }
        else
        {
            ensureSameStoreIdAs( source );
        }
    }

    private void ensureSameStoreIdAs( MemberId remoteCore ) throws StoreIdDownloadFailedException
    {
        StoreId localStoreId = localDatabase.storeId();
        StoreId remoteStoreId = storeFetcher.getStoreIdOf( remoteCore );
        if ( !localStoreId.equals( remoteStoreId ) )
        {
            throw new IllegalStateException( format( "This read replica cannot join the cluster. " +
                            "The local database is not empty and has a mismatching storeId: expected %s actual %s.",
                    remoteStoreId, localStoreId ) );
        }
    }

    private MemberId findCoreMemberToCopyFrom()
    {
        while ( true )
        {
            try
            {
                MemberId memberId = connectionStrategy.coreMember();
                debugLog.info( "Server starting, connecting to core server %s", memberId );
                return memberId;
            }
            catch ( CoreMemberSelectionException ex )
            {
                debugLog.info( "Failed to connect to core server. Retrying in %d ms.", timeout.getMillis() );
                LockSupport.parkUntil( timeout.getMillis() + System.currentTimeMillis() );
                timeout.increment();
            }
        }
    }

    @Override
    public void stop() throws Throwable
    {
        txPulling.stop();
        localDatabase.stop();
    }

    @Override
    public void shutdown() throws Throwable
    {
        txPulling.shutdown();
        localDatabase.shutdown();
    }
}
