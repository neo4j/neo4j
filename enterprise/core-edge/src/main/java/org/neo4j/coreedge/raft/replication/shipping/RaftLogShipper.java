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
package org.neo4j.coreedge.raft.replication.shipping;

import java.io.IOException;
import java.time.Clock;

import org.neo4j.coreedge.catchup.storecopy.LocalDatabase;
import org.neo4j.coreedge.raft.DelayedRenewableTimeoutService;
import org.neo4j.coreedge.raft.LeaderContext;
import org.neo4j.coreedge.raft.RaftMessages;
import org.neo4j.coreedge.raft.RenewableTimeoutService;
import org.neo4j.coreedge.raft.log.RaftLogCursor;
import org.neo4j.coreedge.raft.log.RaftLogEntry;
import org.neo4j.coreedge.raft.log.ReadableRaftLog;
import org.neo4j.coreedge.raft.log.segmented.InFlightMap;
import org.neo4j.coreedge.raft.net.Outbound;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.lang.Long.max;
import static java.lang.Long.min;
import static java.lang.String.format;
import static org.neo4j.coreedge.raft.RenewableTimeoutService.RenewableTimeout;
import static org.neo4j.coreedge.raft.replication.shipping.RaftLogShipper.Timeouts.RESEND;

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
    private final LocalDatabase localDatabase;
    private final Log log;
    private final ReadableRaftLog raftLog;
    private final Clock clock;

    private final MEMBER follower;
    private final MEMBER leader;

    private DelayedRenewableTimeoutService timeoutService;

    public enum Timeouts implements RenewableTimeoutService.TimeoutName
    {
        RESEND
    }

    private final long retryTimeMillis;
    private final int catchupBatchSize;
    private final int maxAllowedShippingLag;
    private RenewableTimeout timeout;

    private long timeoutAbsoluteMillis;
    private long lastSentIndex;

    private long matchIndex = -1;

    InFlightMap<Long, RaftLogEntry> inFlightMap;

    private LeaderContext lastLeaderContext;

    private Mode mode = Mode.MISMATCH;

    RaftLogShipper( Outbound<MEMBER> outbound, LogProvider logProvider, ReadableRaftLog raftLog, Clock clock,
                    MEMBER leader, MEMBER follower, long leaderTerm, long leaderCommit, long retryTimeMillis,
                    int catchupBatchSize, int maxAllowedShippingLag, InFlightMap<Long, RaftLogEntry> inFlightMap,
                    LocalDatabase localDatabase)
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
        this.inFlightMap = inFlightMap;
        this.localDatabase = localDatabase;
    }

    public Object identity()
    {
        return follower;
    }

    public synchronized void start()
    {
        log.info( "Starting log shipper: %s", statusAsString() );

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

            log.error( "Failed to start log shipper " + statusAsString(), e );
        }

        sendSingle( raftLog.appendIndex(), lastLeaderContext );
    }

    public synchronized void stop()
    {
        log.info( "Stopping log shipper %s", statusAsString() );

        try
        {
            timeoutService.stop();
            timeoutService.shutdown();
        }
        catch ( Throwable e )
        {
            log.error( "Failed to start log shipper " + statusAsString(), e );
        }
        abortTimeout();
    }

    public synchronized void onMismatch( long lastRemoteAppendIndex, LeaderContext leaderContext )
    {
        switch ( mode )
        {
        case MISMATCH:
            long logIndex = max( min( lastSentIndex - 1, lastRemoteAppendIndex ), 0 );
            sendSingle( logIndex, leaderContext );
            break;
        case PIPELINE:
        case CATCHUP:
            log.info( "%s: mismatch in mode %s from follower %s, moving to MISMATCH mode",
                    statusAsString(), mode, follower );
            mode = Mode.MISMATCH;
            sendSingle( lastSentIndex, leaderContext );
            break;
        }

        lastLeaderContext = leaderContext;
    }

    public synchronized void onMatch( long newMatchIndex, LeaderContext leaderContext )
    {
        boolean progress = newMatchIndex > matchIndex;
        matchIndex = max( newMatchIndex, matchIndex );

        switch ( mode )
        {
        case MISMATCH:
            if ( sendNextBatchAfterMatch( leaderContext ) )
            {
                log.info( "%s: caught up after mismatch, moving to PIPELINE mode", statusAsString() );
                mode = Mode.PIPELINE;
            }
            else
            {
                log.info( "%s: starting catch up after mismatch, moving to CATCHUP mode", statusAsString() );
                mode = Mode.CATCHUP;
            }
            break;
        case CATCHUP:
            if ( matchIndex >= lastSentIndex )
            {
                if ( sendNextBatchAfterMatch( leaderContext ) )
                {
                    log.info( "%s: caught up, moving to PIPELINE mode", statusAsString() );
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

    public synchronized void onNewEntries( long prevLogIndex, long prevLogTerm, RaftLogEntry[] newLogEntries, LeaderContext leaderContext )
    {
        switch ( mode )
        {
        case PIPELINE:
            while ( lastSentIndex <= prevLogIndex )
            {
                if ( prevLogIndex - matchIndex <= maxAllowedShippingLag )
                {
                    sendNewEntries( prevLogIndex, prevLogTerm, newLogEntries, leaderContext ); // all sending functions update lastSentIndex
                }
                else
                {
                    /* The timer is still set at this point. Either we will send the next batch
                     * as soon as the follower has caught up with the last pipelined entry,
                     * or when we timeout and resend. */
                    log.info("%s: follower has fallen behind (target prevLogIndex was %d, maxAllowedShippingLag is %d), moving to CATCHUP mode", statusAsString(), prevLogIndex, maxAllowedShippingLag );
                    mode = Mode.CATCHUP;
                    break;
                }
            }
            break;
        }

        lastLeaderContext = leaderContext;
    }

    public synchronized void onCommitUpdate( LeaderContext leaderContext )
    {
        switch ( mode )
        {
        case PIPELINE:
            sendCommitUpdate( leaderContext );
            break;
        }

        lastLeaderContext = leaderContext;
    }

    private synchronized void onScheduledTimeoutExpiry()
    {
        try
        {
            if ( timedOut() )
            {
                onTimeout();
            }
            else if ( timeoutAbsoluteMillis != 0 )
            {
                long timeLeft = timeoutAbsoluteMillis - clock.millis();

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
        catch ( Throwable e )
        {
            log.error( "Exception during timeout handling: " + statusAsString(), e );
        }
    }

    private void onTimeout() throws IOException
    {
        switch ( mode )
        {
        case PIPELINE:
            /* we leave pipelined mode here, because the follower seems
             * unresponsive and we do not want to spam it with new entries */
            log.info( "%s: timed out, moving to CATCHUP mode", statusAsString() );
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
        return timeoutAbsoluteMillis != 0 && (clock.millis() - timeoutAbsoluteMillis) >= 0;
    }

    private void scheduleTimeout( long deltaMillis )
    {
        // TODO: This cancel/create dance is a bit inefficient... consider something better.

        timeoutAbsoluteMillis = clock.millis() + deltaMillis;

        if ( timeout != null )
        {
            timeout.cancel();
        }
        timeout = timeoutService.create( RESEND, deltaMillis, 0, timeout -> onScheduledTimeoutExpiry() );
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
    private boolean sendNextBatchAfterMatch( LeaderContext leaderContext )
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

    private void sendCommitUpdate( LeaderContext leaderContext )
    {
        /*
         * This is a commit update. That means that we just received enough success responses to an append
         * request to allow us to send a commit. By Raft invariants, this means that the term for the committed
         * entry is the current term.
         */
        RaftMessages.Heartbeat<MEMBER> appendRequest =
                new RaftMessages.Heartbeat<>( leader, leaderContext.term, leaderContext.commitIndex,
                        leaderContext.term, localDatabase.storeId() );

        outbound.send( follower, appendRequest );
    }

    private void sendSingle( long logIndex, LeaderContext leaderContext )
    {
        logIndex = max( raftLog.prevIndex() + 1, logIndex );

        scheduleTimeout( retryTimeMillis );

        lastSentIndex = logIndex;

        try
        {
            long prevLogIndex = logIndex - 1;
            long prevLogTerm = raftLog.readEntryTerm( prevLogIndex );

            if ( prevLogTerm > leaderContext.term )
            {
                log.warn( "%s aborting send. Not leader anymore? %s, prevLogTerm=%d",
                        statusAsString(), leaderContext, prevLogTerm );
                return;
            }

            RaftLogEntry[] logEntries = RaftLogEntry.empty;

            RaftLogEntry toSend = inFlightMap.retrieve( logIndex );
            if ( toSend == null ) // this is from mismatch most likely
            {
                try ( RaftLogCursor cursor = raftLog.getEntryCursor( logIndex ) )
                {
                    if ( cursor.next() )
                    {
                        toSend = cursor.get();
                    }
                }
            }
            if ( toSend != null )
            {
                logEntries = new RaftLogEntry[]{toSend};
            }

            RaftMessages.AppendEntries.Request<MEMBER> appendRequest =
                    new RaftMessages.AppendEntries.Request<>( leader, leaderContext.term, prevLogIndex, prevLogTerm,
                            logEntries, leaderContext.commitIndex, localDatabase.storeId() );

            outbound.send( follower, appendRequest );
        }
        catch ( IOException e )
        {
            log.warn(
                    "%s tried to send entry at index %d that can't be found in the raft log; it has likely been pruned. " +
                            "This is a temporary state and the system should recover automatically in a short while.",
                    statusAsString(), logIndex );
        }
    }

    private void sendNewEntries( long prevLogIndex, long prevLogTerm, RaftLogEntry[] newEntries, LeaderContext leaderContext )
    {
        scheduleTimeout( retryTimeMillis );

        lastSentIndex = prevLogIndex + 1;

        RaftMessages.AppendEntries.Request<MEMBER> appendRequest = new RaftMessages.AppendEntries.Request<>(
                leader, leaderContext.term, prevLogIndex, prevLogTerm, newEntries, leaderContext.commitIndex,
                localDatabase.storeId() );

        outbound.send( follower, appendRequest );

    }

    private void sendRange( long startIndex, long endIndex, LeaderContext leaderContext )
    {
        if ( startIndex > endIndex )
        {
            return;
        }

        lastSentIndex = endIndex;

        try
        {
            int batchSize = (int) (endIndex - startIndex + 1);
            RaftLogEntry[] entries = new RaftLogEntry[batchSize];

            long prevLogIndex = startIndex - 1;
            long prevLogTerm = raftLog.readEntryTerm( prevLogIndex );

            if ( prevLogTerm > leaderContext.term )
            {
                log.warn( "%s aborting send. Not leader anymore? %s, prevLogTerm=%d",
                        statusAsString(), leaderContext, prevLogTerm );
                return;
            }

            if ( prevLogTerm < 0 )
            {
                log.warn( "%s aborting append entry request since someone has pruned away the entries we needed." +
                        "Sending a LogCompactionInfo instead. Leader context=%s, prevLogTerm=%d",
                        statusAsString(), leaderContext, prevLogTerm );
                outbound.send( follower, new RaftMessages.LogCompactionInfo<>( leader, leaderContext.term,
                        prevLogIndex, localDatabase.storeId() ) );
                return;
            }

            RaftMessages.AppendEntries.Request<MEMBER> appendRequest =
                    new RaftMessages.AppendEntries.Request<>( leader, leaderContext.term, prevLogIndex, prevLogTerm,
                            entries, leaderContext.commitIndex, localDatabase.storeId() );

            int offset = 0;
            try ( RaftLogCursor cursor = raftLog.getEntryCursor( startIndex ) )
            {
                while ( offset < batchSize && cursor.next() )
                {
                    entries[offset] = cursor.get();
                    if ( entries[offset].term() > leaderContext.term )
                    {
                        log.warn( "%s aborting send. Not leader anymore? %s, entryTerm=%d",
                                statusAsString(), leaderContext, entries[offset].term() );
                        return;
                    }
                    offset++;
                }
            }

            outbound.send( follower, appendRequest );
        }
        catch ( IOException e )
        {
            log.warn( statusAsString() + " exception during batch send", e );
        }
    }

    private String statusAsString()
    {
        return format( "%s[matchIndex: %d, lastSentIndex: %d, localAppendIndex: %d, mode: %s]", follower, matchIndex,
                lastSentIndex, raftLog.appendIndex(), mode );
    }
}
