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
package org.neo4j.coreedge.core.state;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.neo4j.coreedge.catchup.storecopy.LocalDatabase;
import org.neo4j.coreedge.catchup.storecopy.StoreCopyFailedException;
import org.neo4j.coreedge.core.consensus.RaftMachine;
import org.neo4j.coreedge.core.consensus.RaftMessages;
import org.neo4j.coreedge.core.consensus.log.pruning.LogPruner;
import org.neo4j.coreedge.core.consensus.outcome.ConsensusOutcome;
import org.neo4j.coreedge.core.state.snapshot.CoreSnapshot;
import org.neo4j.coreedge.core.state.snapshot.CoreStateDownloader;
import org.neo4j.coreedge.identity.ClusterId;
import org.neo4j.coreedge.identity.MemberId;
import org.neo4j.coreedge.messaging.Inbound.MessageHandler;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.util.concurrent.TimeUnit.MINUTES;

public class CoreState implements MessageHandler<RaftMessages.StoreIdAwareMessage>, LogPruner, Lifecycle
{
    private final RaftMachine raftMachine;
    private final LocalDatabase localDatabase;
    private final Log log;
    private final CoreStateDownloader downloader;
    private final BindingService bindingService;
    private final CommandApplicationProcess applicationProcess;
    private final CountDownLatch bootstrapLatch = new CountDownLatch( 1 );

    private ClusterId boundClusterId; // TODO: Use for network message filtering.

    public CoreState(
            RaftMachine raftMachine,
            LocalDatabase localDatabase,
            LogProvider logProvider,
            CoreStateDownloader downloader,
            BindingService bindingService,
            CommandApplicationProcess commandApplicationProcess )
    {
        this.raftMachine = raftMachine;
        this.localDatabase = localDatabase;
        this.downloader = downloader;
        this.bindingService = bindingService;
        this.log = logProvider.getLog( getClass() );
        this.applicationProcess = commandApplicationProcess;
    }

    public void handle( RaftMessages.StoreIdAwareMessage storeIdAwareMessage )
    {
        try
        {
            ConsensusOutcome outcome = raftMachine.handle( storeIdAwareMessage.message() );
            if ( outcome.needsFreshSnapshot() )
            {
                downloadSnapshot( storeIdAwareMessage.message().from() );
            }
            else
            {
                notifyCommitted( outcome.getCommitIndex() );
            }
        }
        catch ( Throwable e )
        {
            raftMachine.stopTimers();
            localDatabase.panic( e );
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

        // TODO: Binding service can return whether or not we are allowed to bootstrap. ClusterId can be exposed at the interface.
        boundClusterId = bindingService.bindToCluster( this::installSnapshot );

        // TODO: Move haveState and CoreBootstrapper into CommandApplicationProcess, which perhaps needs a better name.
        // TODO: Include the None/Partial/Full in the move.
        if ( !haveState() )
        {
            boolean acquired = bootstrapLatch.await( 1, MINUTES );
            if ( !acquired )
            {
                throw new RuntimeException( "Timed out while waiting to download a snapshot from another cluster member" );
            }
        }
        localDatabase.start();
        applicationProcess.start();
    }

    private boolean haveState()
    {
        return raftMachine.state().entryLog().appendIndex() > -1;
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
