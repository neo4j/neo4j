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
package org.neo4j.causalclustering.core.replication;

import org.junit.Test;

import java.util.UUID;

import org.neo4j.causalclustering.core.consensus.ReplicatedInteger;
import org.neo4j.causalclustering.core.replication.session.GlobalSession;
import org.neo4j.causalclustering.core.replication.session.LocalOperationId;
import org.neo4j.causalclustering.core.state.Result;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class ProgressTrackerImplTest
{
    private final int DEFAULT_TIMEOUT_MS = 15_000;

    private GlobalSession session = new GlobalSession( UUID.randomUUID(), null );
    private DistributedOperation operationA = new DistributedOperation(
            ReplicatedInteger.valueOf( 0 ), session, new LocalOperationId( 0, 0 ) );
    private DistributedOperation operationB = new DistributedOperation(
            ReplicatedInteger.valueOf( 1 ), session, new LocalOperationId( 1, 0 ) );
    private ProgressTrackerImpl tracker = new ProgressTrackerImpl( session );

    @Test
    public void shouldReportThatOperationIsNotReplicatedInitially()
    {
        // when
        Progress progress = tracker.start( operationA );

        // then
        assertEquals( false, progress.isReplicated() );
    }

    @Test
    public void shouldWaitForReplication() throws Exception
    {
        // given
        Progress progress = tracker.start( operationA );

        // when
        long time = System.currentTimeMillis();
        progress.awaitReplication( 10L );

        // then
        time = System.currentTimeMillis() - time ;
        assertThat( time, greaterThanOrEqualTo( 10L ) );
    }

    @Test
    public void shouldStopWaitingWhenReplicated() throws Exception
    {
        // given
        Progress progress = tracker.start( operationA );

        // when
        Thread waiter = replicationEventWaiter( progress );

        // then
        assertEquals( true, waiter.isAlive() );
        assertEquals( false, progress.isReplicated() );

        // when
        tracker.trackReplication( operationA );

        // then
        assertEquals( true, progress.isReplicated() );
        waiter.join( DEFAULT_TIMEOUT_MS );
        assertEquals( false, waiter.isAlive() );
    }

    @Test
    public void shouldBeAbleToAbortTracking()
    {
        // when
        tracker.start( operationA );
        // then
        assertEquals( 1L, tracker.inProgressCount() );

        // when
        tracker.abort( operationA );
        // then
        assertEquals( 0L, tracker.inProgressCount() );
    }

    @Test
    public void shouldCheckThatOneOperationDoesNotAffectProgressOfOther()
    {
        // given
        Progress progressA = tracker.start( operationA );
        Progress progressB = tracker.start( operationB );

        // when
        tracker.trackReplication( operationA );

        // then
        assertEquals( true, progressA.isReplicated() );
        assertEquals( false, progressB.isReplicated() );
    }

    @Test
    public void shouldTriggerReplicationEvent() throws Exception
    {
        // given
        Progress progress = tracker.start( operationA );
        Thread waiter = replicationEventWaiter( progress );

        // when
        tracker.triggerReplicationEvent();

        // then
        assertEquals( false, progress.isReplicated() );
        waiter.join();
        assertEquals( false, waiter.isAlive() );
    }

    @Test
    public void shouldGetTrackedResult() throws Exception
    {
        // given
        Progress progress = tracker.start( operationA );

        // when
        String result = "result";
        tracker.trackResult( operationA, Result.of( result ) );

        // then
        assertEquals( result, progress.futureResult().get( DEFAULT_TIMEOUT_MS, MILLISECONDS ) );
    }

    @Test
    public void shouldIgnoreOtherSessions()
    {
        // given
        GlobalSession sessionB = new GlobalSession( UUID.randomUUID(), null );
        DistributedOperation aliasUnderSessionB =
                new DistributedOperation( ReplicatedInteger.valueOf( 0 ), sessionB,
                        new LocalOperationId(
                                /* same id/sequence number as operationA */
                                operationA.operationId().localSessionId(),
                                operationA.operationId().sequenceNumber() ) );

        Progress progressA = tracker.start( operationA );

        // when
        tracker.trackReplication( aliasUnderSessionB );
        tracker.trackResult( aliasUnderSessionB, Result.of( "result" ) );

        // then
        assertEquals( false, progressA.isReplicated() );
        assertEquals( false, progressA.futureResult().isDone() );
    }

    private Thread replicationEventWaiter( Progress progress )
    {
        Thread waiter = new Thread( () ->
        {
            try
            {
                progress.awaitReplication( DEFAULT_TIMEOUT_MS );
            }
            catch ( InterruptedException e )
            {
                throw new RuntimeException( e );
            }
        } );

        waiter.start();
        return waiter;
    }
}
