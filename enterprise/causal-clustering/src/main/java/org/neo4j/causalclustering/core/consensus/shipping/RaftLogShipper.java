/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.causalclustering.core.consensus.shipping;

import java.io.IOException;
import java.time.Clock;

import org.neo4j.causalclustering.core.consensus.log.cache.InFlightCache;
import org.neo4j.causalclustering.core.consensus.LeaderContext;
import org.neo4j.causalclustering.core.consensus.RaftMessages;
import org.neo4j.causalclustering.core.consensus.schedule.Timer;
import org.neo4j.causalclustering.core.consensus.schedule.TimerService;
import org.neo4j.causalclustering.core.consensus.log.RaftLogEntry;
import org.neo4j.causalclustering.core.consensus.log.ReadableRaftLog;
import org.neo4j.causalclustering.messaging.Outbound;
import org.neo4j.causalclustering.core.state.InFlightLogEntryReader;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.lang.Long.max;
import static java.lang.Long.min;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.neo4j.causalclustering.core.consensus.schedule.TimeoutFactory.fixedTimeout;
import static org.neo4j.causalclustering.core.consensus.schedule.Timer.CancelMode.ASYNC;
import static org.neo4j.causalclustering.core.consensus.schedule.Timer.CancelMode.SYNC_WAIT;
import static org.neo4j.causalclustering.core.consensus.shipping.RaftLogShipper.Mode.CATCHUP;
import static org.neo4j.causalclustering.core.consensus.shipping.RaftLogShipper.Mode.PIPELINE;
import static org.neo4j.causalclustering.core.consensus.shipping.RaftLogShipper.Timeouts.RESEND;
import static org.neo4j.scheduler.JobScheduler.Groups.raft;

/// Optimizations
// TODO: Have several outstanding batches in catchup mode, to bridge the latency gap.
// TODO: Bisect search for mismatch.
// TODO: Maximum bound on size of batch in bytes, not just entry count.

// Production ready

// Core functionality
// TODO: Consider making even CommitUpdate a raft-message of its own.

/**
 * This class handles the shipping of raft logs from this node when it is the leader to the followers.
 * Each instance handles a single follower and acts on events and associated state updates originating
 * within the main raft state machine.
 * <p>
 * It is crucial that all actions happen within the context of the leaders state at some point in time.
 */
public class RaftLogShipper
{
    private static final long MIN_INDEX = 1L; // we never ship entry zero, which must be bootstrapped or received as part of a snapshot
    private final int TIMER_INACTIVE = 0;

    enum Mode
    {
        /**
         * In the mismatch mode we are unsure about the follower state, thus
         * we tread with caution, going backwards trying to find the point where
         * our logs match. We send empty append entries to minimize the cost of
         * this mode.
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

    public enum Timeouts implements TimerService.TimerName
    {
        RESEND
    }

    private final Outbound<MemberId, RaftMessages.RaftMessage> outbound;
    private final Log log;
    private final ReadableRaftLog raftLog;
    private final Clock clock;
    private final MemberId follower;
    private final MemberId leader;
    private final long retryTimeMillis;
    private final int catchupBatchSize;
    private final int maxAllowedShippingLag;
    private final InFlightCache inFlightCache;

    private TimerService timerService;
    private Timer timer;
    private long timeoutAbsoluteMillis;
    private long lastSentIndex;
    private long matchIndex = -1;
    private LeaderContext lastLeaderContext;
    private Mode mode = Mode.MISMATCH;

    RaftLogShipper( Outbound<MemberId, RaftMessages.RaftMessage> outbound, LogProvider logProvider,
                    ReadableRaftLog raftLog, Clock clock, TimerService timerService,
                    MemberId leader, MemberId follower, long leaderTerm, long leaderCommit, long retryTimeMillis,
                    int catchupBatchSize, int maxAllowedShippingLag, InFlightCache inFlightCache )
    {
        this.outbound = outbound;
        this.timerService = timerService;
        this.catchupBatchSize = catchupBatchSize;
        this.maxAllowedShippingLag = maxAllowedShippingLag;
        this.log = logProvider.getLog( getClass() );
        this.raftLog = raftLog;
        this.clock = clock;
        this.follower = follower;
        this.leader = leader;
        this.retryTimeMillis = retryTimeMillis;
        this.lastLeaderContext = new LeaderContext( leaderTerm, leaderCommit );
        this.inFlightCache = inFlightCache;
    }

    public Object identity()
    {
        return follower;
    }

    public synchronized void start()
    {
        log.info( "Starting log shipper: %s", statusAsString() );
        sendEmpty( raftLog.appendIndex(), lastLeaderContext );
   }

    public synchronized void stop()
    {
        log.info( "Stopping log shipper %s", statusAsString() );
        abortTimeout();
    }

    public synchronized void onMismatch( long lastRemoteAppendIndex, LeaderContext leaderContext )
    {
        switch ( mode )
        {
            case MISMATCH:
                long logIndex = max( min( lastSentIndex - 1, lastRemoteAppendIndex ), MIN_INDEX );
                sendEmpty( logIndex, leaderContext );
                break;
            case PIPELINE:
            case CATCHUP:
                log.info( "%s: mismatch in mode %s from follower %s, moving to MISMATCH mode",
                        statusAsString(), mode, follower );
                mode = Mode.MISMATCH;
                sendEmpty( lastSentIndex, leaderContext );
                break;

            default:
                throw new IllegalStateException( "Unknown mode: " + mode );
        }

        lastLeaderContext = leaderContext;
    }

    public synchronized void onMatch( long newMatchIndex, LeaderContext leaderContext )
    {
        boolean progress = newMatchIndex > matchIndex;
        if ( newMatchIndex > matchIndex )
        {
            matchIndex = newMatchIndex;
        }
        else
        {
            log.warn( "%s: match index not progressing. This should be transient.", statusAsString() );
        }

        switch ( mode )
        {
            case MISMATCH:
                if ( sendNextBatchAfterMatch( leaderContext ) )
                {
                    log.info( "%s: caught up after mismatch, moving to PIPELINE mode", statusAsString() );
                    mode = PIPELINE;
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
                        mode = PIPELINE;
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

            default:
                throw new IllegalStateException( "Unknown mode: " + mode );
        }

        lastLeaderContext = leaderContext;
    }

    public synchronized void onNewEntries( long prevLogIndex, long prevLogTerm, RaftLogEntry[] newLogEntries,
                                           LeaderContext leaderContext )
    {
        if ( mode == Mode.PIPELINE )
        {
            while ( lastSentIndex <= prevLogIndex )
            {
                if ( prevLogIndex - matchIndex <= maxAllowedShippingLag )
                {
                    // all sending functions update lastSentIndex
                    sendNewEntries( prevLogIndex, prevLogTerm, newLogEntries, leaderContext );
                }
                else
                {
                    /* The timer is still set at this point. Either we will send the next batch
                     * as soon as the follower has caught up with the last pipelined entry,
                     * or when we timeout and resend. */
                    log.info( "%s: follower has fallen behind (target prevLogIndex was %d, maxAllowedShippingLag " +
                              "is %d), moving to CATCHUP mode", statusAsString(), prevLogIndex,
                            maxAllowedShippingLag );
                    mode = Mode.CATCHUP;
                    break;
                }
            }
        }

        lastLeaderContext = leaderContext;
    }

    public synchronized void onCommitUpdate( LeaderContext leaderContext )
    {
        if ( mode == Mode.PIPELINE )
        {
            sendCommitUpdate( leaderContext );
        }

        lastLeaderContext = leaderContext;
    }

    /**
     * Callback invoked by the external timer service.
     */
    private synchronized void onScheduledTimeoutExpiry()
    {
        if ( timedOut() )
        {
            onTimeout();
        }
        else if ( timeoutAbsoluteMillis != TIMER_INACTIVE )
        {
            /* Timer was moved, so we need to reschedule. */
            long timeLeft = timeoutAbsoluteMillis - clock.millis();
            if ( timeLeft > 0 )
            {
                scheduleTimeout( timeLeft );
            }
            else
            {
                /* However it managed to expire, so we can just handle it immediately. */
                onTimeout();
            }
        }
    }

    void onTimeout()
    {
        if ( mode == PIPELINE )
        {
            /* The follower seems unresponsive and we do not want to spam it with new entries.
             * The catchup will pick-up when the last sent pipelined entry matches. */
            log.info( "%s: timed out, moving to CATCHUP mode", statusAsString() );
            mode = Mode.CATCHUP;
            scheduleTimeout( retryTimeMillis );
        }
        else if ( mode == CATCHUP )
        {
            /* The follower seems unresponsive so we move back to mismatch mode to
             * slowly poke it and figure out what is going on. Catchup will resume
             * on the next match. */
            log.info( "%s: timed out, moving to MISMATCH mode", statusAsString() );
            mode = Mode.MISMATCH;
        }

        if ( lastLeaderContext != null )
        {
            sendEmpty( lastSentIndex, lastLeaderContext );
        }
    }

    /**
     * This function is necessary because the scheduled callback blocks on the monitor before
     * entry and the expiry time of the timer might have been moved or even cancelled before
     * the entry is granted.
     *
     * @return True if we actually timed out, otherwise false.
     */
    private boolean timedOut()
    {
        return timeoutAbsoluteMillis != TIMER_INACTIVE && (clock.millis() - timeoutAbsoluteMillis) >= 0;
    }

    private void scheduleTimeout( long deltaMillis )
    {
        timeoutAbsoluteMillis = clock.millis() + deltaMillis;

        if ( timer == null )
        {
            timer = timerService.create( RESEND, raft, timeout -> onScheduledTimeoutExpiry() );
        }

        timer.set( fixedTimeout( deltaMillis, MILLISECONDS ) );
    }

    private void abortTimeout()
    {
        if ( timer != null )
        {
            timer.cancel( ASYNC );
        }
        timeoutAbsoluteMillis = TIMER_INACTIVE;
    }

    /**
     * Returns true if this sent the last batch.
     */
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
        RaftMessages.Heartbeat appendRequest =
                new RaftMessages.Heartbeat( leader, leaderContext.term, leaderContext.commitIndex,
                        leaderContext.term );

        outbound.send( follower, appendRequest );
    }

    private void sendNewEntries( long prevLogIndex, long prevLogTerm, RaftLogEntry[] newEntries,
                                 LeaderContext leaderContext )
    {
        scheduleTimeout( retryTimeMillis );

        lastSentIndex = prevLogIndex + 1;

        RaftMessages.AppendEntries.Request appendRequest = new RaftMessages.AppendEntries.Request(
                leader, leaderContext.term, prevLogIndex, prevLogTerm, newEntries, leaderContext.commitIndex
        );

        outbound.send( follower, appendRequest );
    }

    private void sendEmpty( long logIndex, LeaderContext leaderContext )
    {
        scheduleTimeout( retryTimeMillis );

        logIndex = max( raftLog.prevIndex() + 1, logIndex );
        lastSentIndex = logIndex;

        try
        {
            long prevLogIndex = logIndex - 1;
            long prevLogTerm = raftLog.readEntryTerm( prevLogIndex );

            if ( prevLogTerm > leaderContext.term )
            {
                log.warn( "%s: aborting send. Not leader anymore? %s, prevLogTerm=%d",
                        statusAsString(), leaderContext, prevLogTerm );
                return;
            }

            if ( doesNotExistInLog( prevLogIndex, prevLogTerm ) )
            {
                log.warn( "%s: Entry was pruned when sending empty (prevLogIndex=%d, prevLogTerm=%d)",
                        statusAsString(), prevLogIndex, prevLogTerm );
                return;
            }

            RaftMessages.AppendEntries.Request appendRequest = new RaftMessages.AppendEntries.Request(
                    leader, leaderContext.term, prevLogIndex, prevLogTerm, RaftLogEntry.empty, leaderContext.commitIndex );
            outbound.send( follower, appendRequest );
        }
        catch ( IOException e )
        {
            log.warn( statusAsString() + " exception during empty send", e );
        }
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

            boolean entryMissing = false;
            try ( InFlightLogEntryReader logEntrySupplier = new InFlightLogEntryReader( raftLog, inFlightCache, false ) )
            {
                for ( int offset = 0; offset < batchSize; offset++ )
                {
                    entries[offset] = logEntrySupplier.get( startIndex + offset );
                    if ( entries[offset] == null )
                    {
                        entryMissing = true;
                        break;
                    }
                    if ( entries[offset].term() > leaderContext.term )
                    {
                        log.warn( "%s aborting send. Not leader anymore? %s, entryTerm=%d",
                                statusAsString(), leaderContext, entries[offset].term() );
                        return;
                    }
                }
            }

            if ( entryMissing || doesNotExistInLog( prevLogIndex, prevLogTerm ) )
            {
                if ( raftLog.prevIndex() >= prevLogIndex )
                {
                    sendLogCompactionInfo( leaderContext );
                }
                else
                {
                    log.error( "%s: Could not send compaction info and entries were missing, but log is not behind.",
                            statusAsString() );
                }
            }
            else
            {
                RaftMessages.AppendEntries.Request appendRequest = new RaftMessages.AppendEntries.Request(
                        leader, leaderContext.term, prevLogIndex, prevLogTerm, entries, leaderContext.commitIndex );

                outbound.send( follower, appendRequest );
            }
        }
        catch ( IOException e )
        {
            log.warn( statusAsString() + " exception during batch send", e );
        }
    }

    private boolean doesNotExistInLog( long logIndex, long logTerm )
    {
        return logTerm == -1 && logIndex != -1;
    }

    private void sendLogCompactionInfo( LeaderContext leaderContext )
    {
        log.warn( "Sending log compaction info. Log pruned? Status=%s, LeaderContext=%s",
                statusAsString(), leaderContext );

        outbound.send( follower, new RaftMessages.LogCompactionInfo(
                leader, leaderContext.term, raftLog.prevIndex() ) );
    }

    private String statusAsString()
    {
        return format( "%s[matchIndex: %d, lastSentIndex: %d, localAppendIndex: %d, mode: %s]", follower, matchIndex,
                lastSentIndex, raftLog.appendIndex(), mode );
    }
}
