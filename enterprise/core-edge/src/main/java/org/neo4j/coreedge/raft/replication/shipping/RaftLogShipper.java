/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.coreedge.raft.replication.shipping;

import org.neo4j.coreedge.raft.LeaderContext;
import org.neo4j.coreedge.raft.RaftMessages;
import org.neo4j.coreedge.raft.log.RaftLogEntry;
import org.neo4j.coreedge.raft.log.RaftStorageException;
import org.neo4j.coreedge.raft.log.ReadableRaftLog;
import org.neo4j.coreedge.raft.net.Outbound;
import org.neo4j.coreedge.raft.DelayedRenewableTimeoutService;
import org.neo4j.coreedge.raft.RenewableTimeoutService.TimeoutName;
import org.neo4j.helpers.Clock;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.lang.Long.max;
import static java.lang.Long.min;
import static java.lang.String.format;
import static org.neo4j.coreedge.raft.RenewableTimeoutService.*;

/// Optimizations
// TODO: Have several outstanding batches in catchup mode, to bridge the latency gap.
// TODO: Bisect search for mismatch.
// TODO: Maximum bound on size of batch in bytes, not just entry count.

// Production ready
// TODO: Replace sender service with something more appropriate. No need for queue and multiplex capability, in fact is it bad to have?
//  TODO Should we drop messages to unconnected channels instead? Use UDP? Because we are not allowed to go below a certain cluster size (safety)
//  TODO then leader will keep trying to replicate to gone members, thus queuing things up is hurtful.

// TODO: Replace the timeout service with something better. More efficient for the constantly rescheduling use case and also useful for deterministic unit tests.

// Core functionality
// TODO: Consider making even CommitUpdate a raft-message of its own.

/**
 * This class handles the shipping of raft logs from this node when it is the leader to the followers.
 * Each instance handles a single follower and acts on events and associated state updates originating
 * within the main raft state machine.
 *
 * It is crucial that all actions happen within the context of the leaders state at some point in time.
 *
 * @param <MEMBER> The member type.
 */
public class RaftLogShipper<MEMBER>
{
    enum Mode
    {
        /**
         * In the mismatch mode we are unsure about the follower state, thus
         * we tread with caution, going backwards trying to find the point where
         * our logs match.
         */
        MISMATCH,
        /**
         * In the catchup mode we are trying to catch up the follower as quickly
         * as possible. The follower receives batches of entries in series until
         * it is fully caught up.
         */
        CATCHUP,
        /**
         * In the pipeline mode the follower is treated as caught up and we
         * optimistically ship any latest entries without waiting for responses,
         * expecting successful responses.
         */
        PIPELINE
    }

    private final Outbound<MEMBER> outbound;
    private final LogProvider logProvider;
    private final Log log;
    private final ReadableRaftLog raftLog;
    private final Clock clock;

    private final MEMBER follower;
    private final MEMBER leader;

    private DelayedRenewableTimeoutService timeoutService;
    private final TimeoutName timeoutName = () -> "RESEND";
    private final long retryTimeMillis;
    private final int catchupBatchSize;
    private final int maxAllowedShippingLag;
    private RenewableTimeout timeout;

    private long timeoutAbsoluteMillis;
    private long lastSentIndex;

    private long matchIndex = -1;

    private LeaderContext lastLeaderContext;

    private Mode mode = Mode.MISMATCH;

    public RaftLogShipper( Outbound<MEMBER> outbound, LogProvider logProvider, ReadableRaftLog raftLog, Clock clock, MEMBER leader, MEMBER follower,
                           long leaderTerm, long leaderCommit, long retryTimeMillis, int catchupBatchSize, int maxAllowedShippingLag )
    {
        this.outbound = outbound;
        this.catchupBatchSize = catchupBatchSize;
        this.maxAllowedShippingLag = maxAllowedShippingLag;
        this.logProvider = logProvider;
        this.log = logProvider.getLog( getClass() );
        this.raftLog = raftLog;
        this.clock = clock;
        this.follower = follower;
        this.leader = leader;
        this.retryTimeMillis = retryTimeMillis;
        this.lastLeaderContext = new LeaderContext( leaderTerm, leaderCommit );
    }

    public Object identity()
    {
        return follower;
    }

    public synchronized void start()
    {
        log.info( "Starting log shipper to: " + follower );

        try
        {
            timeoutService = new DelayedRenewableTimeoutService( clock, logProvider );
            timeoutService.init();
            timeoutService.start();
        }
        catch ( Throwable e )
        {
            // TODO: Think about how to handle this. We cannot be allowed to throw when
            // TODO: starting the log shippers from the main RAFT handling. The timeout
            // TODO: service is a LifeCycle.
            // TODO: Should we have and use one system level timeout service instead?

            log.error( "Failed to start log shipper to: " + follower, e );
        }

        try
        {
            sendSingle( raftLog.appendIndex(), lastLeaderContext );
        }
        catch ( RaftStorageException e )
        {
            log.error( "Exception during send: " + follower, e );
        }
    }

    public synchronized void stop()
    {
        log.info( "Stopping log shipper to: " + follower );

        try
        {
            timeoutService.stop();
            timeoutService.shutdown();
        }
        catch ( Throwable e )
        {
            log.error( "Failed to start log shipper to: " + follower, e );
        }
        abortTimeout();
    }

    public synchronized void onMismatch( long lastRemoteAppendIndex, LeaderContext leaderContext ) throws RaftStorageException
    {
        switch ( mode )
        {
        case MISMATCH:
            long logIndex = max( min( lastSentIndex - 1, lastRemoteAppendIndex ), 0 );
            sendSingle( logIndex, leaderContext );
            break;
        case PIPELINE:
        case CATCHUP:
            log.info( format( "Mismatch in mode %s from follower %s", mode, follower ) );
            mode = Mode.MISMATCH;
            sendSingle( lastSentIndex, leaderContext );
            break;
        }

        lastLeaderContext = leaderContext;
    }

    public synchronized void onMatch( long newMatchIndex, LeaderContext leaderContext ) throws RaftStorageException
    {
        boolean progress = newMatchIndex > matchIndex;
        matchIndex = max ( newMatchIndex, matchIndex );

        switch ( mode )
        {
        case MISMATCH:
            if( sendNextBatchAfterMatch( leaderContext ) )
            {
                log.info( format( "Caught up after mismatch: %s", follower ) );
                mode = Mode.PIPELINE;
            }
            else
            {
                log.info( format( "Starting catch up after mismatch: %s", follower ) );
                mode = Mode.CATCHUP;
            }
            break;
        case CATCHUP:
            if ( matchIndex >= lastSentIndex )
            {
                if ( sendNextBatchAfterMatch( leaderContext ) )
                {
                    log.info( format( "Caught up: %s", follower ) );
                    mode = Mode.PIPELINE;
                }
            }
            break;
        case PIPELINE:
            if ( matchIndex == lastSentIndex )
            {
                abortTimeout();
            }
            else if ( progress )
            {
                scheduleTimeout( retryTimeMillis );
            }
            break;
        }

        lastLeaderContext = leaderContext;
    }

    public synchronized void onNewEntry( long prevLogIndex, long prevLogTerm, RaftLogEntry newLogEntry, LeaderContext leaderContext ) throws RaftStorageException
    {
        switch ( mode )
        {
        case PIPELINE:
            while( lastSentIndex <= prevLogIndex )
            {
                if ( prevLogIndex - matchIndex <= maxAllowedShippingLag )
                {
                    sendNewEntry( prevLogIndex, prevLogTerm, newLogEntry, leaderContext ); // all sending functions update lastSentIndex
                }
                else
                {
                    /* The timer is still set at this point. Either we will send the next batch
                     * as soon as the follower has caught up with the last pipelined entry,
                     * or when we timeout and resend. */
                    mode = Mode.CATCHUP;
                    break;
                }
            }
            break;
        }

        lastLeaderContext = leaderContext;
    }

    public synchronized void onCommitUpdate( LeaderContext leaderContext ) throws RaftStorageException
    {
        switch ( mode )
        {
        case PIPELINE:
            sendCommitUpdate( leaderContext );
            break;
        }

        lastLeaderContext = leaderContext;
    }

    public synchronized void onScheduledTimeoutExpiry()
    {
        try
        {
            if ( timedOut() )
            {
                onTimeout();
            }
            else if ( timeoutAbsoluteMillis != 0 )
            {
                long timeLeft = timeoutAbsoluteMillis - clock.currentTimeMillis();

                if ( timeLeft > 0 )
                {
                    scheduleTimeout( timeLeft );
                }
                else
                {
                    onTimeout();
                }
            }
        }
        catch ( RaftStorageException e )
        {
            log.error( "Exception during timeout handling: " + follower, e );
        }
    }

    private void onTimeout() throws RaftStorageException
    {
        switch ( mode )
        {
        case PIPELINE:
            /* we leave pipelined mode here, because the follower seems
             * unresponsive and we do not want to spam it with new entries */
            mode = Mode.CATCHUP;
            /* fallthrough */
        case CATCHUP:
        case MISMATCH:
            if ( lastLeaderContext != null )
            {
                sendSingle( lastSentIndex, lastLeaderContext );
            }
            break;
        }
    }

    private boolean timedOut()
    {
        return timeoutAbsoluteMillis != 0 && (clock.currentTimeMillis() - timeoutAbsoluteMillis) >= 0;
    }

    private void scheduleTimeout( long deltaMillis )
    {
        // TODO: This cancel/create dance is a bit inefficient... consider something better.

        timeoutAbsoluteMillis = clock.currentTimeMillis() + deltaMillis;

        if ( timeout != null )
        {
            timeout.cancel();
        }
        timeout = timeoutService.create( timeoutName, deltaMillis, 0, timeout -> onScheduledTimeoutExpiry() );
    }

    private void abortTimeout()
    {
        if ( timeout != null )
        {
            timeout.cancel();
        }
        timeoutAbsoluteMillis = 0;
    }

    /** Returns true if this sent the last batch. */
    private boolean sendNextBatchAfterMatch( LeaderContext leaderContext ) throws RaftStorageException
    {
        long lastIndex = raftLog.appendIndex();

        if ( lastIndex > matchIndex )
        {
            long endIndex = min( lastIndex, matchIndex + catchupBatchSize );

            scheduleTimeout( retryTimeMillis );
            sendRange( matchIndex + 1, endIndex, leaderContext );
            return endIndex == lastIndex;
        }
        else
        {
            return true;
        }
    }

    private void sendCommitUpdate( LeaderContext leaderContext ) throws RaftStorageException
    {
        /*
         * This is a commit update. That means that we just received enough success responses to an append
         * request to allow us to send a commit. By Raft invariants, this means that the term for the committed
         * entry is the current term.
         */
        RaftMessages.Heartbeat<MEMBER> appendRequest = new RaftMessages.Heartbeat<>(
                leader, leaderContext.term, leaderContext.commitIndex, leaderContext.term );

        outbound.send( follower, appendRequest );
    }

    private void sendSingle( long logIndex, LeaderContext leaderContext ) throws RaftStorageException
    {
        scheduleTimeout( retryTimeMillis );

        lastSentIndex = logIndex;

        long prevLogIndex = logIndex - 1;
        long prevLogTerm = raftLog.readEntryTerm( prevLogIndex );

        if ( prevLogTerm > leaderContext.term )
        {
            log.warn( format( "Aborting send. Not leader anymore? %s, prevLogTerm=%d", leaderContext, prevLogTerm ) );
            return;
        }

        RaftLogEntry logEntry = null;
        if ( raftLog.entryExists( logIndex ) )
        {
            logEntry = raftLog.readLogEntry( logIndex );
        }

        RaftMessages.AppendEntries.Request<MEMBER> appendRequest = new RaftMessages.AppendEntries.Request<>(
                leader, leaderContext.term, prevLogIndex, prevLogTerm, new RaftLogEntry[] { logEntry }, leaderContext.commitIndex );

        outbound.send( follower, appendRequest );

    }

    private void sendNewEntry( long prevLogIndex, long prevLogTerm, RaftLogEntry newEntry, LeaderContext leaderContext ) throws RaftStorageException
    {
        scheduleTimeout( retryTimeMillis );

        lastSentIndex = prevLogIndex + 1;

        RaftMessages.AppendEntries.Request<MEMBER> appendRequest = new RaftMessages.AppendEntries.Request<>(
                leader, leaderContext.term, prevLogIndex, prevLogTerm, new RaftLogEntry[] { newEntry }, leaderContext.commitIndex );

        outbound.send( follower, appendRequest );

    }

    private void sendRange( long startIndex, long endIndex, LeaderContext leaderContext ) throws RaftStorageException
    {
        if ( startIndex > endIndex )
            return;

        lastSentIndex = endIndex;

        int batchSize = (int) (endIndex - startIndex + 1);
        RaftLogEntry[] entries = new RaftLogEntry[batchSize];

        long prevLogIndex = startIndex - 1;
        long prevLogTerm = raftLog.readEntryTerm( prevLogIndex );

        if ( prevLogTerm > leaderContext.term )
        {
            log.warn( format( "Aborting send. Not leader anymore? %s, prevLogTerm=%d", leaderContext, prevLogTerm ) );
            return;
        }

        RaftMessages.AppendEntries.Request<MEMBER> appendRequest = new RaftMessages.AppendEntries.Request<>(
                leader, leaderContext.term, prevLogIndex, prevLogTerm, entries, leaderContext.commitIndex );

        int offset = 0;
        while( offset < batchSize )
        {
            entries[offset] = raftLog.readLogEntry( startIndex + offset );

            if( entries[offset].term() > leaderContext.term )
            {
                log.warn( format( "Aborting send. Not leader anymore? %s, entryTerm=%d", leaderContext, entries[offset].term() ) );
                return;
            }

            offset++;
        }

        outbound.send( follower, appendRequest );
    }
}
