/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.causalclustering.readreplica;

import java.io.IOException;

import org.neo4j.causalclustering.catchup.CatchupAddressProvider.SingleAddressProvider;
import org.neo4j.causalclustering.catchup.storecopy.DatabaseShutdownException;
import org.neo4j.causalclustering.catchup.storecopy.LocalDatabase;
import org.neo4j.causalclustering.catchup.storecopy.RemoteStore;
import org.neo4j.causalclustering.catchup.storecopy.StoreCopyFailedException;
import org.neo4j.causalclustering.catchup.storecopy.StoreCopyProcess;
import org.neo4j.causalclustering.catchup.storecopy.StoreIdDownloadFailedException;
import org.neo4j.causalclustering.core.state.snapshot.TopologyLookupException;
import org.neo4j.causalclustering.discovery.TopologyService;
import org.neo4j.causalclustering.helper.TimeoutStrategy;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.identity.StoreId;
import org.neo4j.causalclustering.upstream.UpstreamDatabaseSelectionException;
import org.neo4j.causalclustering.upstream.UpstreamDatabaseStrategySelector;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.lang.String.format;

class ReadReplicaStartupProcess implements Lifecycle
{
    private final RemoteStore remoteStore;
    private final LocalDatabase localDatabase;
    private final Lifecycle txPulling;
    private final Log debugLog;
    private final Log userLog;

    private final TimeoutStrategy timeoutStrategy;
    private final UpstreamDatabaseStrategySelector selectionStrategy;
    private final TopologyService topologyService;

    private String lastIssue;
    private final StoreCopyProcess storeCopyProcess;

    ReadReplicaStartupProcess( RemoteStore remoteStore, LocalDatabase localDatabase, Lifecycle txPulling,
            UpstreamDatabaseStrategySelector selectionStrategy, TimeoutStrategy timeoutStrategy, LogProvider debugLogProvider,
            LogProvider userLogProvider, StoreCopyProcess storeCopyProcess, TopologyService topologyService )
    {
        this.remoteStore = remoteStore;
        this.localDatabase = localDatabase;
        this.txPulling = txPulling;
        this.selectionStrategy = selectionStrategy;
        this.timeoutStrategy = timeoutStrategy;
        this.debugLog = debugLogProvider.getLog( getClass() );
        this.userLog = userLogProvider.getLog( getClass() );
        this.storeCopyProcess = storeCopyProcess;
        this.topologyService = topologyService;
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
    public void start() throws IOException, DatabaseShutdownException
    {
        boolean syncedWithUpstream = false;
        TimeoutStrategy.Timeout timeout = timeoutStrategy.newTimeout();
        int attempt = 0;
        while ( !syncedWithUpstream )
        {
            attempt++;
            MemberId source = null;
            try
            {
                source = selectionStrategy.bestUpstreamDatabase();
                syncStoreWithUpstream( source );
                syncedWithUpstream = true;
            }
            catch ( UpstreamDatabaseSelectionException e )
            {
                lastIssue = issueOf( "finding upstream member", attempt );
                debugLog.warn( lastIssue );
            }
            catch ( StoreCopyFailedException e )
            {
                lastIssue = issueOf( format( "copying store files from %s", source ), attempt );
                debugLog.warn( lastIssue );
            }
            catch ( StoreIdDownloadFailedException e )
            {
                lastIssue = issueOf( format( "getting store id from %s", source ), attempt );
                debugLog.warn( lastIssue );
            }
            catch ( TopologyLookupException e )
            {
                lastIssue = issueOf( format( "getting address of %s", source ), attempt );
                debugLog.warn( lastIssue );
            }

            try
            {
                Thread.sleep( timeout.getMillis() );
                timeout.increment();
            }
            catch ( InterruptedException e )
            {
                Thread.interrupted();
                lastIssue = "Interrupted while trying to start read replica";
                debugLog.warn( lastIssue );
                break;
            }
        }

        if ( !syncedWithUpstream )
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

    private void syncStoreWithUpstream( MemberId source ) throws IOException, StoreIdDownloadFailedException,
            StoreCopyFailedException, TopologyLookupException, DatabaseShutdownException
    {
        if ( localDatabase.isEmpty() )
        {
            debugLog.info( "Local database is empty, attempting to replace with copy from upstream server %s", source );

            debugLog.info( "Finding store id of upstream server %s", source );
            AdvertisedSocketAddress fromAddress = topologyService.findCatchupAddress( source ).orElseThrow( () -> new TopologyLookupException( source ) );
            StoreId storeId = remoteStore.getStoreId( fromAddress );

            debugLog.info( "Copying store from upstream server %s", source );
            localDatabase.delete();
            storeCopyProcess.replaceWithStoreFrom( new SingleAddressProvider( fromAddress ), storeId );

            debugLog.info( "Restarting local database after copy.", source );
        }
        else
        {
            ensureSameStoreIdAs( source );
        }
    }

    private void ensureSameStoreIdAs( MemberId upstream ) throws StoreIdDownloadFailedException, TopologyLookupException
    {
        StoreId localStoreId = localDatabase.storeId();
        AdvertisedSocketAddress advertisedSocketAddress =
                topologyService.findCatchupAddress( upstream ).orElseThrow( () -> new TopologyLookupException( upstream ) );
        StoreId remoteStoreId = remoteStore.getStoreId( advertisedSocketAddress );
        if ( !localStoreId.equals( remoteStoreId ) )
        {
            throw new IllegalStateException( format( "This read replica cannot join the cluster. " +
                    "The local database is not empty and has a mismatching storeId: expected %s actual %s.", remoteStoreId, localStoreId ) );
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
