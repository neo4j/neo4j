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

import org.neo4j.coreedge.catchup.storecopy.LocalDatabase;
import org.neo4j.coreedge.catchup.storecopy.StoreCopyFailedException;
import org.neo4j.coreedge.core.state.snapshot.CoreSnapshot;
import org.neo4j.coreedge.core.state.snapshot.CoreStateDownloader;
import org.neo4j.coreedge.core.consensus.RaftMachine;
import org.neo4j.coreedge.core.consensus.RaftMessages;
import org.neo4j.coreedge.core.consensus.log.pruning.LogPruner;
import org.neo4j.coreedge.core.consensus.outcome.ConsensusOutcome;
import org.neo4j.coreedge.identity.MemberId;
import org.neo4j.coreedge.messaging.Inbound.MessageHandler;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

public class CoreState implements MessageHandler<RaftMessages.StoreIdAwareMessage>, LogPruner, Lifecycle
{
    private final RaftMachine raftMachine;
    private final LocalDatabase localDatabase;
    private final Log log;
    private final CoreStateDownloader downloader;
    private final CommandApplicationProcess applicationProcess;

    public CoreState(
            RaftMachine raftMachine,
            LocalDatabase localDatabase,
            LogProvider logProvider,
            CoreStateDownloader downloader,
            CommandApplicationProcess commandApplicationProcess )
    {
        this.raftMachine = raftMachine;
        this.localDatabase = localDatabase;
        this.downloader = downloader;
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
                notifyNeedFreshSnapshot( storeIdAwareMessage.message().from() );
            }
            else
            {
                notifyCommitted( outcome.getCommitIndex());
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

    private synchronized void notifyNeedFreshSnapshot( MemberId source )
    {
        downloadSnapshot( source );
    }

    /**
     * Attempts to download a fresh snapshot from another core instance.
     *
     * @param source The source address to attempt a download of a snapshot from.
     */
    private void downloadSnapshot( MemberId source )
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

    public synchronized void installSnapshot( CoreSnapshot coreSnapshot ) throws IOException
    {
        applicationProcess.installSnapshot( coreSnapshot, raftMachine );
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
    public void start() throws IOException, InterruptedException
    {
        applicationProcess.start();
    }

    @Override
    public void stop() throws IOException, InterruptedException
    {
        log.info( "CoreState stopping" );
        applicationProcess.stop();
    }

    @Override
    public void init() throws Throwable
    {
        applicationProcess.init();
    }

    @Override
    public void shutdown() throws Throwable
    {
        applicationProcess.shutdown();
    }
}
