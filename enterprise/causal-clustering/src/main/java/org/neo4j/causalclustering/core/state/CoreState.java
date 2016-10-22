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
package org.neo4j.causalclustering.core.state;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.neo4j.causalclustering.catchup.storecopy.LocalDatabase;
import org.neo4j.causalclustering.catchup.storecopy.StoreCopyFailedException;
import org.neo4j.causalclustering.core.consensus.RaftMachine;
import org.neo4j.causalclustering.core.consensus.RaftMessages;
import org.neo4j.causalclustering.core.consensus.log.pruning.LogPruner;
import org.neo4j.causalclustering.core.consensus.outcome.ConsensusOutcome;
import org.neo4j.causalclustering.core.state.snapshot.CoreSnapshot;
import org.neo4j.causalclustering.core.state.snapshot.CoreStateDownloader;
import org.neo4j.causalclustering.identity.ClusterId;
import org.neo4j.causalclustering.identity.ClusterIdentity;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.messaging.Inbound.MessageHandler;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static org.neo4j.function.Predicates.awaitEx;

public class CoreState implements MessageHandler<RaftMessages.ClusterIdAwareMessage>, LogPruner, Lifecycle
{
    private final RaftMachine raftMachine;
    private final LocalDatabase localDatabase;
    private final Log log;
    private final ClusterIdentity clusterIdentity;
    private final CoreStateDownloader downloader;
    private final CommandApplicationProcess applicationProcess;
    private final CountDownLatch bootstrapLatch = new CountDownLatch( 1 );

    public CoreState(
            RaftMachine raftMachine,
            LocalDatabase localDatabase,
            ClusterIdentity clusterIdentity,
            LogProvider logProvider,
            CoreStateDownloader downloader,
            CommandApplicationProcess commandApplicationProcess )
    {
        this.raftMachine = raftMachine;
        this.localDatabase = localDatabase;
        this.clusterIdentity = clusterIdentity;
        this.downloader = downloader;
        this.log = logProvider.getLog( getClass() );
        this.applicationProcess = commandApplicationProcess;
    }

    public void handle( RaftMessages.ClusterIdAwareMessage clusterIdAwareMessage )
    {
        ClusterId clusterId = clusterIdAwareMessage.clusterId();
        if ( clusterId.equals( clusterIdentity.clusterId() ) )
        {
            try
            {
                ConsensusOutcome outcome = raftMachine.handle( clusterIdAwareMessage.message() );
                if ( outcome.needsFreshSnapshot() )
                {
                    downloadSnapshot( clusterIdAwareMessage.message().from() );
                }
                else
                {
                    notifyCommitted( outcome.getCommitIndex() );
                }
            }
            catch ( Throwable e )
            {
                log.error( "Error handling message", e );
                raftMachine.stopTimers();
                localDatabase.panic( e );
            }
        }
        else
        {
            log.info( "Discarding message[%s] owing to mismatched storeId. Expected: %s, Encountered: %s",
                    clusterIdAwareMessage.message(), clusterId, clusterIdentity.clusterId() );
        }
    }

    private synchronized void notifyCommitted( long commitIndex )
    {
        applicationProcess.notifyCommitted( commitIndex );
    }

    /**
     * Attempts to download a fresh snapshot from another core instance.
     *
     * @param source The source address to attempt a download of a snapshot from.
     */
    private synchronized void downloadSnapshot( MemberId source )
    {
        try
        {
            applicationProcess.sync();
            downloader.downloadSnapshot( source, this );
        }
        catch ( InterruptedException | StoreCopyFailedException e )
        {
            log.error( "Failed to download snapshot", e );
        }
    }

    public synchronized CoreSnapshot snapshot() throws IOException, InterruptedException
    {
        return applicationProcess.snapshot( raftMachine );
    }

    public synchronized void installSnapshot( CoreSnapshot coreSnapshot ) throws Throwable
    {
        applicationProcess.installSnapshot( coreSnapshot, raftMachine );
        bootstrapLatch.countDown();
    }

    @SuppressWarnings("unused") // used in embedded robustness testing
    public long lastApplied()
    {
        return applicationProcess.lastApplied();
    }

    @Override
    public void prune() throws IOException
    {
        applicationProcess.prune();
    }

    @Override
    public void init() throws Throwable
    {
        localDatabase.init();
        applicationProcess.init();
    }

    @Override
    public void start() throws Throwable
    {
        // How can state be installed?
        // 1. Already installed (detected by checking on-disk state)
        // 2. Bootstrap (single selected server)
        // 3. Download from someone else (others)

        clusterIdentity.bindToCluster( this::installSnapshot );

        // TODO: Move haveState and CoreBootstrapper into CommandApplicationProcess, which perhaps needs a better name.
        // TODO: Include the None/Partial/Full in the move.
        awaitEx( () -> haveState() || snapshotDownloaded(), 30, TimeUnit.MINUTES );
        localDatabase.start();
        applicationProcess.start();
    }

    private boolean snapshotDownloaded() throws InterruptedException
    {
        boolean acquired = bootstrapLatch.await( 30, TimeUnit.SECONDS );
        if ( !acquired )
        {
            log.warn( "This machine is not ready to start yet. " +
                    "It is waiting for the download of the cluster snapshot from another member to complete." );
        }
        return acquired;
    }

    private boolean haveState()
    {
        return raftMachine.state().appendIndex() > -1;
    }

    @Override
    public void stop() throws Throwable
    {
        applicationProcess.stop();
        localDatabase.stop();
    }

    @Override
    public void shutdown() throws Throwable
    {
        applicationProcess.shutdown();
        localDatabase.shutdown();
    }
}
