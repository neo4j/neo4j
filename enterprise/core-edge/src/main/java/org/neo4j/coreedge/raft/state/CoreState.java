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
package org.neo4j.coreedge.raft.state;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.coreedge.catchup.storecopy.LocalDatabase;
import org.neo4j.coreedge.catchup.storecopy.StoreCopyFailedException;
import org.neo4j.coreedge.discovery.CoreMemberSelectionException;
import org.neo4j.coreedge.raft.MismatchedStoreIdService;
import org.neo4j.coreedge.raft.RaftInstance;
import org.neo4j.coreedge.raft.RaftMessages;
import org.neo4j.coreedge.raft.log.pruning.LogPruner;
import org.neo4j.coreedge.raft.net.Inbound.MessageHandler;
import org.neo4j.coreedge.raft.outcome.ConsensusOutcome;
import org.neo4j.coreedge.server.MemberId;
import org.neo4j.coreedge.server.StoreId;
import org.neo4j.coreedge.server.edge.CoreMemberSelectionStrategy;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

public class CoreState implements MessageHandler<RaftMessages.StoreIdAwareMessage>, LogPruner, MismatchedStoreIdService, Lifecycle
{
    private final RaftInstance raftInstance;
    private final LocalDatabase localDatabase;
    private final Log log;
    private final CoreMemberSelectionStrategy someoneElse;
    private final CoreStateDownloader downloader;
    private final List<MismatchedStoreIdService.MismatchedStoreListener> listeners = new ArrayList<>(  );
    private final CommandApplicationProcess applicationProcess;

    public CoreState(
            RaftInstance raftInstance,
            LocalDatabase localDatabase,
            LogProvider logProvider,
            CoreMemberSelectionStrategy someoneElse,
            CoreStateDownloader downloader,
            CommandApplicationProcess commandApplicationProcess )
    {
        this.raftInstance = raftInstance;
        this.localDatabase = localDatabase;
        this.someoneElse = someoneElse;
        this.downloader = downloader;
        this.log = logProvider.getLog( getClass() );
        this.applicationProcess = commandApplicationProcess;
    }

    public void handle( RaftMessages.StoreIdAwareMessage storeIdAwareMessage )
    {
        // Break out each if branch into a new CoreState instance
        StoreId storeId = storeIdAwareMessage.storeId();
        if ( storeId.equals( localDatabase.storeId() ) )
        {
            try
            {
                ConsensusOutcome outcome = raftInstance.handle( storeIdAwareMessage.message() );
                if ( outcome.needsFreshSnapshot() )
                {
                    notifyNeedFreshSnapshot();
                }
                else
                {
                    notifyCommitted( outcome.getCommitIndex());
                }
            }
            catch ( Throwable e )
            {
                raftInstance.stopTimers();
                localDatabase.panic( e );
            }

        }
        else
        {
            RaftMessages.RaftMessage message = storeIdAwareMessage.message();
            if ( localDatabase.isEmpty() )
            {
                log.info( "StoreId mismatch but store was empty so downloading new store from %s. Expected: " +
                        "%s, Encountered: %s. ", message.from(), storeId, localDatabase.storeId() );
                downloadSnapshot( message.from() );
            }
            else
            {
                log.info( "Discarding message[%s] owing to mismatched storeId and non-empty store. " +
                        "Expected: %s, Encountered: %s", message,  storeId, localDatabase.storeId() );
                listeners.forEach( l -> {
                    MismatchedStoreIdService.MismatchedStoreIdException ex = new MismatchedStoreIdService.MismatchedStoreIdException( storeId, localDatabase.storeId() );
                    l.onMismatchedStore( ex );
                } );
            }

        }
    }

    public void addMismatchedStoreListener( MismatchedStoreListener listener )
    {
        listeners.add(listener);
    }

    private synchronized void notifyCommitted( long commitIndex )
    {
        applicationProcess.notifyCommitted( commitIndex );
    }

    private synchronized void notifyNeedFreshSnapshot()
    {
        try
        {
            downloadSnapshot( someoneElse.coreMember() );
        }
        catch ( CoreMemberSelectionException e )
        {
            log.error( "Failed to select server", e );
        }
    }

    /**
     * Attempts to download a fresh snapshot from another core instance.
     *
     * @param source The source address to attempt a download of a snapshot from.
     */
    public synchronized void downloadSnapshot( MemberId source )
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
        return applicationProcess.snapshot();
    }

    synchronized void installSnapshot( CoreSnapshot coreSnapshot )
    {
        applicationProcess.installSnapshot( coreSnapshot );
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
