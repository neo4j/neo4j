package org.neo4j.causalclustering.core.consensus;

import org.junit.Test;
import org.mockito.Mockito;

import java.util.UUID;

import org.neo4j.causalclustering.core.consensus.log.RaftLogEntry;
import org.neo4j.causalclustering.identity.ClusterId;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.identity.RaftTestMemberSetBuilder;
import org.neo4j.causalclustering.messaging.Inbound;
import org.neo4j.logging.NullLogProvider;

public class RaftMessagesPreHandlerTest
{
    @SuppressWarnings( "unchecked" )
    private Inbound.MessageHandler<RaftMessages.ClusterIdAwareMessage> delegate = Mockito.mock( Inbound.MessageHandler.class );
    private ElectionTiming electionTiming = Mockito.mock( ElectionTiming.class );
    private MemberId myself = new MemberId( UUID.randomUUID() );
    private ClusterId clusterId = new ClusterId( UUID.randomUUID() );
    private long term = 3;
    private RaftMachine raftMachine = new RaftMachineBuilder( myself, 3, RaftTestMemberSetBuilder.INSTANCE )
            .term( term )
            .build();

    private RaftMessagesPreHandler handler = new RaftMessagesPreHandler( delegate, electionTiming, raftMachine, NullLogProvider.getInstance() );

    private MemberId leader = new MemberId( UUID.randomUUID() );
    private RaftMessages.ClusterIdAwareMessage heartbeat =
            new RaftMessages.ClusterIdAwareMessage( clusterId, new RaftMessages.Heartbeat( leader, term, 0, 0 ) );
    private RaftMessages.ClusterIdAwareMessage appendEntries =
            new RaftMessages.ClusterIdAwareMessage( clusterId,
                    new RaftMessages.AppendEntries.Request( leader, term, 0, 0, RaftLogEntry.empty, 0 )
            );
    private RaftMessages.ClusterIdAwareMessage voteResponse =
            new RaftMessages.ClusterIdAwareMessage( clusterId, new RaftMessages.Vote.Response( leader, term, false ) );

    @Test
    public void shouldDropMessagesIfHasNotBeenStarted() throws Exception
    {
        // when
        handler.handle( heartbeat );

        // then
        Mockito.verify( delegate, Mockito.never() ).handle( heartbeat );
    }

    @Test
    public void shouldDropMessagesIfHasBeenStopped() throws Exception
    {
        // given
        handler.start( clusterId );
        handler.stop();

        // when
        handler.handle( heartbeat );

        // then
        Mockito.verify( delegate, Mockito.never() ).handle( heartbeat );
    }

    @Test
    public void shouldDropMessagesIfForDifferentClusterId() throws Exception
    {
        // given
        handler.start( clusterId );

        // when
        handler.handle( new RaftMessages.ClusterIdAwareMessage(
                new ClusterId( UUID.randomUUID() ), new RaftMessages.Heartbeat( leader, term, 0, 0 )
        ) );

        // then
        Mockito.verify( delegate, Mockito.never() ).handle( heartbeat );
    }

    @Test
    public void shouldDelegateMessages() throws Exception
    {
        // given
        handler.start( clusterId );

        // when
        handler.handle( heartbeat );

        // then
        Mockito.verify( delegate ).handle( heartbeat );
    }

    @Test
    public void shouldRenewElectionForHeartbeats() throws Exception
    {
        // given
        handler.start( clusterId );

        // when
        handler.handle( heartbeat );

        // then
        Mockito.verify( electionTiming ).renewElection();
    }

    @Test
    public void shouldRenewElectionForAppendEntriesRequests() throws Exception
    {
        // given
        handler.start( clusterId );

        // when
        handler.handle( appendEntries );

        // then
        Mockito.verify( electionTiming ).renewElection();
    }

    @Test
    public void shouldNotRenewElectionForOtherMessages() throws Exception
    {
        // given
        handler.start( clusterId );

        // when
        handler.handle( voteResponse );

        // then
        Mockito.verify( electionTiming, Mockito.never() ).renewElection();
    }

    @Test
    public void shouldNotRenewElectionTimeoutsForHeartbeatsFromEarlierTerm() throws Exception
    {
        // given
        RaftMessages.ClusterIdAwareMessage heartbeat =
                new RaftMessages.ClusterIdAwareMessage( clusterId, new RaftMessages.Heartbeat( leader, term - 1, 0, 0 ) );

        handler.start( clusterId );

        // when
        handler.handle( heartbeat );

        // then
        Mockito.verify( electionTiming, Mockito.never() ).renewElection();
    }

    @Test
    public void shouldNotRenewElectionTimeoutsForAppendEntriesRequestsFromEarlierTerms() throws Exception
    {
        RaftMessages.ClusterIdAwareMessage appendEntries =
                new RaftMessages.ClusterIdAwareMessage( clusterId,
                        new RaftMessages.AppendEntries.Request( leader, term - 1, 0, 0, RaftLogEntry.empty, 0 )
                );

        handler.start( clusterId );

        // when
        handler.handle( appendEntries );

        // then
        Mockito.verify( electionTiming, Mockito.never() ).renewElection();
    }
}