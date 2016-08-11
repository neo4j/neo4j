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
import java.util.ArrayList;
import java.util.List;

import org.neo4j.coreedge.catchup.storecopy.LocalDatabase;
import org.neo4j.coreedge.catchup.storecopy.StoreCopyFailedException;
import org.neo4j.coreedge.core.state.snapshot.CoreSnapshot;
import org.neo4j.coreedge.core.state.snapshot.CoreStateDownloader;
import org.neo4j.coreedge.messaging.routing.CoreMemberSelectionException;
import org.neo4j.coreedge.identity.StoreId;
import org.neo4j.coreedge.core.consensus.MismatchedStoreIdService;
import org.neo4j.coreedge.core.consensus.RaftMachine;
import org.neo4j.coreedge.core.consensus.RaftMessages;
import org.neo4j.coreedge.core.consensus.log.pruning.LogPruner;
import org.neo4j.coreedge.identity.MemberId;
import org.neo4j.coreedge.messaging.routing.CoreMemberSelectionStrategy;
import org.neo4j.coreedge.messaging.Inbound.MessageHandler;
import org.neo4j.coreedge.core.consensus.outcome.ConsensusOutcome;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

public class CoreState implements MessageHandler<RaftMessages.StoreIdAwareMessage>, LogPruner, MismatchedStoreIdService, Lifecycle
{
    private final RaftMachine raftMachine;
    private final LocalDatabase localDatabase;
    private final Log log;
    private final CoreMemberSelectionStrategy someoneElse;
    private final CoreStateDownloader downloader;
    private final List<MismatchedStoreIdService.MismatchedStoreListener> listeners = new ArrayList<>(  );
    private final CommandApplicationProcess applicationProcess;

    public CoreState(
            RaftMachine raftMachine,
            LocalDatabase localDatabase,
            LogProvider logProvider,
            CoreMemberSelectionStrategy someoneElse,
            CoreStateDownloader downloader,
            CommandApplicationProcess commandApplicationProcess )
    {
        this.raftMachine = raftMachine;
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
                ConsensusOutcome outcome = raftMachine.handle( storeIdAwareMessage.message() );
                if ( outcome.needsFreshSnapshot() )
                {
                    notifyNeedFreshSnapshot( storeId );
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
        else
        {
            RaftMessages.RaftMessage message = storeIdAwareMessage.message();
            if ( localDatabase.isEmpty() && !StoreId.isDefault( storeId ) )
            {
                /*
                 * Checking for the actual value of StoreId is not a very pretty thing to do, but it is currently
                 * necessary, "currently" here meaning as long as raft message sending is not dependent on LocalDatabase
                 * start/stop state.
                 * The problem lies in how storeid is retrieved. LocalDatabase is the sole manager of the neo store
                 * lifecycle (technically the DataSourceManager) but the fact is that the storeid of the underlying
                 * store can be asked from it even if it is in the stopped state. That means that sender threads can
                 * ask for the storeid of a store that is not actually open. This is obviously a lifecycle management
                 * issue which needs to be solved properly, but until then...
                 * ...until then we accept the fact that storeid will be asked by raft message sending threads and they
                 * need to be provided with a value. This is quite simple, that value is StoreId.DEFAULT. However,
                 * members receiving that message will go through this handle() method and they need to make a call
                 * on it. That storeId by construction mismatches everything, so the message will not be processed.
                 * However, a StoreIdAwareMessage with a DEFAULT storeId should not be considered a legitimate
                 * bearer of storeId information, even if the local database is empty. This can manifest in a simple
                 * race, if for example a follower has just copied an empty store from the leader, the leader is
                 * shutting down but a leader thread sends a message after its LocalDatabase stops() but before the
                 * SenderService is stopped. This will result in the follower receiving a message with a DEFAULT
                 * storeId. The resulting mismatch will make the message be skipped but if we don't explicitly
                 * ignore DEFAULT storeId it will result in an attempt to copy a store from a member that is no longer
                 * there.
                 * Obviously the correct thing to do is not allow the leader to send that message (explicitly checking
                 * for the DEFAULT storeid in receivers may still be a correct thing to do, defensively, but it should
                 * not be the only way to guard against this).
                 */
                log.info( "StoreId mismatch but store was empty so downloading new store from %s. Expected: " +
                        "%s, Encountered: %s. ", message.from(), storeId, localDatabase.storeId() );
                downloadSnapshot( message.from(), storeId );
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
        listeners.add( listener );
    }

    private synchronized void notifyCommitted( long commitIndex )
    {
        applicationProcess.notifyCommitted( commitIndex );
    }

    private synchronized void notifyNeedFreshSnapshot( StoreId storeId )
    {
        try
        {
            downloadSnapshot( someoneElse.coreMember(), storeId );
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
     * @param storeId
     */
    private void downloadSnapshot( MemberId source, StoreId storeId )
    {
        try
        {
            applicationProcess.sync();
            downloader.downloadSnapshot( source, storeId, this );
        }
        catch ( InterruptedException | StoreCopyFailedException e )
        {
            e.printStackTrace();
            log.error( "Failed to download snapshot", e );
        }
    }

    public synchronized CoreSnapshot snapshot() throws IOException, InterruptedException
    {
        return applicationProcess.snapshot();
    }

    public synchronized void installSnapshot( CoreSnapshot coreSnapshot )
    {
        applicationProcess.installSnapshot( coreSnapshot );
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
