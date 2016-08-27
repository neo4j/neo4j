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
package org.neo4j.coreedge.edge;

import java.io.IOException;
import java.util.concurrent.locks.LockSupport;

import org.neo4j.coreedge.catchup.storecopy.CopiedStoreRecovery;
import org.neo4j.coreedge.catchup.storecopy.LocalDatabase;
import org.neo4j.coreedge.catchup.storecopy.StoreCopyFailedException;
import org.neo4j.coreedge.catchup.storecopy.StoreFetcher;
import org.neo4j.coreedge.catchup.storecopy.StoreIdDownloadFailedException;
import org.neo4j.coreedge.catchup.storecopy.TemporaryStoreDirectory;
import org.neo4j.coreedge.core.state.machines.tx.RetryStrategy;
import org.neo4j.coreedge.identity.MemberId;
import org.neo4j.coreedge.identity.StoreId;
import org.neo4j.coreedge.messaging.routing.CoreMemberSelectionException;
import org.neo4j.coreedge.messaging.routing.CoreMemberSelectionStrategy;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.lang.String.format;

class EdgeStartupProcess implements Lifecycle
{
    private final StoreFetcher storeFetcher;
    private final LocalDatabase localDatabase;
    private final Lifecycle txPulling;
    private final CoreMemberSelectionStrategy connectionStrategy;
    private final Log log;
    private final RetryStrategy.Timeout timeout;
    private final CopiedStoreRecovery copiedStoreRecovery;

    EdgeStartupProcess(
            StoreFetcher storeFetcher,
            LocalDatabase localDatabase,
            Lifecycle txPulling,
            CoreMemberSelectionStrategy connectionStrategy,
            RetryStrategy retryStrategy,
            LogProvider logProvider, CopiedStoreRecovery copiedStoreRecovery )
    {
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
        localDatabase.start();

        MemberId source = findCoreMemberToCopyFrom();
        if ( localDatabase.isEmpty() )
        {
            log.info( "Local database is empty, attempting to replace with copy from core server %s", source );
            log.info( "Stopping local database before copy." );
            localDatabase.stop();

            log.info( "Finding store id of core server %s", source );
            StoreId storeId = storeFetcher.getStoreIdOf( source );

            log.info( "Copying store from core server %s", source );
            localDatabase.delete();
            copyWholeStoreFrom( source, storeId, storeFetcher );

            log.info( "Restarting local database after copy.", source );
            localDatabase.start();
        }
        else
        {
            ensureSameStoreIdAs( source );
        }

        txPulling.start();
    }

    private void ensureSameStoreIdAs( MemberId remoteCore ) throws StoreIdDownloadFailedException
    {
        StoreId localStoreId = localDatabase.storeId();
        StoreId remoteStoreId = storeFetcher.getStoreIdOf( remoteCore );
        if ( !localStoreId.equals( remoteStoreId ) )
        {
            throw new IllegalStateException(
                    format( "This edge machine cannot join the cluster. " +
                            "The local database is not empty and has a mismatching storeId: expected %s actual %s.",
                            remoteStoreId, localStoreId ) );
        }
    }

    private void copyWholeStoreFrom( MemberId source, StoreId expectedStoreId, StoreFetcher storeFetcher ) throws IOException, StoreCopyFailedException
    {
        TemporaryStoreDirectory tempStore = new TemporaryStoreDirectory( localDatabase.storeDir() );
        storeFetcher.copyStore( source, expectedStoreId, tempStore.storeDir() );
        copiedStoreRecovery.recoverCopiedStore( tempStore.storeDir() );
        localDatabase.replaceWith( tempStore.storeDir() );
        // TODO: Delete tempDir.
        log.info( "Replaced store with one downloaded from %s", source );
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
