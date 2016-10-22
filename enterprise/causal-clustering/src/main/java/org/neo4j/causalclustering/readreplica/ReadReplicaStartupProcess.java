/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
    private final FileSystemAbstraction fs;
    private final StoreFetcher storeFetcher;
    private final LocalDatabase localDatabase;
    private final Lifecycle txPulling;
    private final CoreMemberSelectionStrategy connectionStrategy;
    private final Log log;
    private final RetryStrategy.Timeout timeout;
    private final CopiedStoreRecovery copiedStoreRecovery;

    ReadReplicaStartupProcess( FileSystemAbstraction fs, StoreFetcher storeFetcher, LocalDatabase localDatabase,
            Lifecycle txPulling, CoreMemberSelectionStrategy connectionStrategy, RetryStrategy retryStrategy,
            LogProvider logProvider, CopiedStoreRecovery copiedStoreRecovery )
    {
        this.fs = fs;
        this.storeFetcher = storeFetcher;
        this.localDatabase = localDatabase;
        this.txPulling = txPulling;
        this.connectionStrategy = connectionStrategy;
        this.copiedStoreRecovery = copiedStoreRecovery;
        this.timeout = retryStrategy.newTimeout();
        this.log = logProvider.getLog( getClass() );
    }

    @Override
    public void init() throws Throwable
    {
        localDatabase.init();
        txPulling.init();
    }

    @Override
    public void start() throws Throwable
    {
        long retryInterval = 5_000;
        int attempts = 0;
        while ( attempts++ < 5 )
        {
            MemberId source = findCoreMemberToCopyFrom();
            try
            {
                tryToStart( source );
                return;
            }
            catch ( StoreCopyFailedException e )
            {
                log.info( "Attempt #%d to start read replica failed while copying store files from %s.",
                        attempts,
                        source );
            }
            catch ( StreamingTransactionsFailedException e )
            {
                log.info( "Attempt #%d to start read replica failed while streaming transactions from %s.", attempts,
                        source );
            }
            catch ( StoreIdDownloadFailedException e )
            {
                log.info( "Attempt #%d to start read replica failed while getting store id from %s.", attempts, source );
            }

            try
            {
                Thread.sleep( retryInterval );
                retryInterval = Math.min( 60_000, retryInterval * 2 );
            }
            catch ( InterruptedException e )
            {
                Thread.interrupted();
                throw new RuntimeException( "Interrupted while trying to start read replica.", e );
            }
        }
        throw new Exception( "Failed to start read replica after " + (attempts - 1) + " attempts" );
    }

    private void tryToStart( MemberId source ) throws Throwable
    {
        if ( localDatabase.isEmpty() )
        {
            log.info( "Local database is empty, attempting to replace with copy from core server %s", source );

            log.info( "Finding store id of core server %s", source );
            StoreId storeId = storeFetcher.getStoreIdOf( source );

            log.info( "Copying store from core server %s", source );
            localDatabase.delete();
            new CopyStoreSafely( fs, localDatabase, copiedStoreRecovery, log )
                    .copyWholeStoreFrom( source, storeId, storeFetcher );

            log.info( "Restarting local database after copy.", source );
        }
        else
        {
            ensureSameStoreIdAs( source );
        }

        localDatabase.start();
        txPulling.start();
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
                log.info( "Server starting, connecting to core server %s", memberId );
                return memberId;
            }
            catch ( CoreMemberSelectionException ex )
            {
                log.info( "Failed to connect to core server. Retrying in %d ms.", timeout.getMillis() );
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
