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

import org.neo4j.causalclustering.catchup.storecopy.LocalDatabase;
import org.neo4j.causalclustering.catchup.storecopy.RemoteStore;
import org.neo4j.causalclustering.catchup.storecopy.StoreCopyFailedException;
import org.neo4j.causalclustering.catchup.storecopy.StoreCopyProcess;
import org.neo4j.causalclustering.catchup.storecopy.StoreIdDownloadFailedException;
import org.neo4j.causalclustering.catchup.storecopy.StreamingTransactionsFailedException;
import org.neo4j.causalclustering.core.state.snapshot.TopologyLookupException;
import org.neo4j.causalclustering.discovery.TopologyService;
import org.neo4j.causalclustering.helper.TimeoutStrategy;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.identity.StoreId;
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
    private final UpstreamDatabaseStrategySelector selectionStrategyPipeline;
    private final TopologyService topologyService;

    private String lastIssue;
    private final StoreCopyProcess storeCopyProcess;

    ReadReplicaStartupProcess( RemoteStore remoteStore, LocalDatabase localDatabase, Lifecycle txPulling,
            UpstreamDatabaseStrategySelector selectionStrategyPipeline, TimeoutStrategy timeoutStrategy, LogProvider debugLogProvider,
            LogProvider userLogProvider, StoreCopyProcess storeCopyProcess, TopologyService topologyService )
    {
        this.remoteStore = remoteStore;
        this.localDatabase = localDatabase;
        this.txPulling = txPulling;
        this.selectionStrategyPipeline = selectionStrategyPipeline;
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
    public void start() throws IOException
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
                source = selectionStrategyPipeline.bestUpstreamDatabase();
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

    private void syncStoreWithUpstream( MemberId source )
            throws IOException, StoreIdDownloadFailedException, StoreCopyFailedException, StreamingTransactionsFailedException, TopologyLookupException
    {
        if ( localDatabase.isEmpty() )
        {
            debugLog.info( "Local database is empty, attempting to replace with copy from upstream server %s", source );

            debugLog.info( "Finding store id of upstream server %s", source );
            AdvertisedSocketAddress fromAddress =
                    topologyService.findCatchupAddress( source ).orElseThrow( () -> new TopologyLookupException( source ) );
            StoreId storeId = remoteStore.getStoreId( fromAddress );

            debugLog.info( "Copying store from upstream server %s", source );
            localDatabase.delete();
            storeCopyProcess.replaceWithStoreFrom( fromAddress, storeId );

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
