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
package org.neo4j.causalclustering.core.consensus;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;

import org.neo4j.causalclustering.core.consensus.log.RaftLog;
import org.neo4j.causalclustering.core.consensus.log.RaftLogEntry;
import org.neo4j.causalclustering.core.consensus.log.segmented.InFlightMap;
import org.neo4j.causalclustering.core.consensus.membership.RaftMembershipManager;
import org.neo4j.causalclustering.core.consensus.outcome.ConsensusOutcome;
import org.neo4j.causalclustering.core.consensus.outcome.Outcome;
import org.neo4j.causalclustering.core.consensus.roles.Role;
import org.neo4j.causalclustering.core.consensus.schedule.RenewableTimeoutService;
import org.neo4j.causalclustering.core.consensus.shipping.RaftLogShippingManager;
import org.neo4j.causalclustering.core.consensus.state.ExposedRaftState;
import org.neo4j.causalclustering.core.consensus.state.RaftState;
import org.neo4j.causalclustering.core.consensus.term.TermState;
import org.neo4j.causalclustering.core.consensus.vote.VoteState;
import org.neo4j.causalclustering.core.state.snapshot.RaftCoreState;
import org.neo4j.causalclustering.core.state.storage.StateStorage;
import org.neo4j.causalclustering.helper.VolatileFuture;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.messaging.Outbound;
import org.neo4j.kernel.impl.util.Listener;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.lang.String.format;

import static org.neo4j.causalclustering.core.consensus.roles.Role.LEADER;

/**
 * Implements the Raft Consensus Algorithm.
 *
 * The algorithm is driven by incoming messages provided to {@link #handle}.
 */
public class RaftMachine implements LeaderLocator, CoreMetaData
{
    private final LeaderNotFoundMonitor leaderNotFoundMonitor;
    private RenewableTimeoutService.RenewableTimeout heartbeatTimer;

    public enum Timeouts implements RenewableTimeoutService.TimeoutName
    {
        ELECTION, HEARTBEAT
    }

    private final RaftState state;
    private final MemberId myself;

    private final RenewableTimeoutService renewableTimeoutService;
    private final long heartbeatInterval;
    private RenewableTimeoutService.RenewableTimeout electionTimer;
    private RaftMembershipManager membershipManager;

    private final long electionTimeout;

    private final VolatileFuture<MemberId> volatileLeader = new VolatileFuture<>( null );

    private final Outbound<MemberId, RaftMessages.RaftMessage> outbound;
    private final Log log;
    private Role currentRole = Role.FOLLOWER;

    private RaftLogShippingManager logShipping;

    public RaftMachine( MemberId myself, StateStorage<TermState> termStorage,
                        StateStorage<VoteState> voteStorage, RaftLog entryLog,
                        long electionTimeout, long heartbeatInterval,
                        RenewableTimeoutService renewableTimeoutService,
                        Outbound<MemberId, RaftMessages.RaftMessage> outbound,
                        LogProvider logProvider, RaftMembershipManager membershipManager,
                        RaftLogShippingManager logShipping,
                        InFlightMap<RaftLogEntry> inFlightMap,
                        Monitors monitors )
    {
        this.myself = myself;
        this.electionTimeout = electionTimeout;
        this.heartbeatInterval = heartbeatInterval;

        this.renewableTimeoutService = renewableTimeoutService;

        this.outbound = outbound;
        this.logShipping = logShipping;
        this.log = logProvider.getLog( getClass() );

        this.membershipManager = membershipManager;

        this.state = new RaftState( myself, termStorage, membershipManager, entryLog, voteStorage, inFlightMap, logProvider );

        leaderNotFoundMonitor = monitors.newMonitor( LeaderNotFoundMonitor.class );

        initTimers();
    }

    private void initTimers()
    {
        electionTimer = renewableTimeoutService.create( Timeouts.ELECTION, electionTimeout, randomTimeoutRange(),
                timeout -> {
                    try
                    {
                        handle( new RaftMessages.Timeout.Election( myself ) );
                    }
                    catch ( IOException e )
                    {
                        log.error( "Failed to process election timeout.", e );
                    }
                    timeout.renew();
                } );
        heartbeatTimer = renewableTimeoutService.create( Timeouts.HEARTBEAT, heartbeatInterval, 0,
                timeout -> {
                    try
                    {
                        handle( new RaftMessages.Timeout.Heartbeat( myself ) );
                    }
                    catch ( IOException e )
                    {
                        log.error( "Failed to process heartbeat timeout.", e );
                    }
                    timeout.renew();
                } );
    }

    public void triggerElection() throws IOException
    {
        handle( new RaftMessages.Timeout.Election( myself ) );
    }

    public void panic()
    {
        heartbeatTimer.cancel();
        electionTimer.cancel();
    }

    public synchronized RaftCoreState coreState()
    {
        return new RaftCoreState( membershipManager.getCommitted() );
    }

    public synchronized void installCoreState( RaftCoreState coreState ) throws IOException
    {
        membershipManager.install( coreState.committed() );
    }

    public synchronized void setTargetMembershipSet( Set<MemberId> targetMembers )
    {
        membershipManager.setTargetMembershipSet( targetMembers );

        if ( currentRole == LEADER )
        {
            membershipManager.onFollowerStateChange( state.followerStates() );
        }
    }

    @Override
    public MemberId getLeader() throws NoLeaderFoundException
    {
        return waitForLeader( 0, member -> member != null );
    }

    private MemberId waitForLeader( long timeoutMillis, Predicate<MemberId> predicate ) throws NoLeaderFoundException
    {
        try
        {
            return volatileLeader.get( timeoutMillis, predicate );
        }
        catch ( InterruptedException e )
        {
            Thread.currentThread().interrupt();

            leaderNotFoundMonitor.increment();
            throw new NoLeaderFoundException( e );
        }
        catch ( TimeoutException e )
        {
            leaderNotFoundMonitor.increment();
            throw new NoLeaderFoundException( e );
        }
    }

    private Collection<Listener<MemberId>> leaderListeners = new ArrayList<>();

    @Override
    public synchronized void registerListener( Listener<MemberId> listener )
    {
        leaderListeners.add( listener );
        listener.receive( state.leader() );
    }

    @Override
    public synchronized void unregisterListener( Listener listener )
    {
        leaderListeners.remove( listener );
    }

    /**
     * Every call to state() gives you an immutable copy of the current state.
     *
     * @return A fresh view of the state.
     */
    public synchronized ExposedRaftState state()
    {
        return state.copy();
    }

    private void notifyLeaderChanges( Outcome outcome )
    {
        for ( Listener<MemberId> listener : leaderListeners )
        {
            listener.receive( outcome.getLeader() );
        }
    }

    private void handleLogShipping( Outcome outcome ) throws IOException
    {
        LeaderContext leaderContext = new LeaderContext( outcome.getTerm(), outcome.getLeaderCommit() );
        if ( outcome.isElectedLeader() )
        {
            logShipping.resume( leaderContext );
        }
        else if ( outcome.isSteppingDown() )
        {
            logShipping.pause();
        }

        if ( outcome.getRole() == LEADER )
        {
            logShipping.handleCommands( outcome.getShipCommands(), leaderContext );
        }
    }

    private boolean leaderChanged( Outcome outcome, MemberId oldLeader )
    {
        if ( oldLeader == null && outcome.getLeader() != null )
        {
            return true;
        }
        else if ( oldLeader != null && !oldLeader.equals( outcome.getLeader() ) )
        {
            return true;
        }

        return false;
    }

    public synchronized ConsensusOutcome handle( RaftMessages.RaftMessage incomingMessage ) throws IOException
    {
        Outcome outcome = currentRole.handler.handle( incomingMessage, state, log );

        boolean newLeaderWasElected = leaderChanged( outcome, state.leader() );

        state.update( outcome ); // updates to raft log happen within
        sendMessages( outcome );

        handleTimers( outcome );
        handleLogShipping( outcome );

        driveMembership( outcome );

        volatileLeader.set( outcome.getLeader() );

        if ( newLeaderWasElected )
        {
            notifyLeaderChanges( outcome );
        }
        return outcome;
    }

    private void driveMembership( Outcome outcome ) throws IOException
    {
        membershipManager.processLog( outcome.getCommitIndex(), outcome.getLogCommands() );

        currentRole = outcome.getRole();
        membershipManager.onRole( currentRole );

        if ( currentRole == LEADER )
        {
            membershipManager.onFollowerStateChange( state.followerStates() );
        }
    }

    private void handleTimers( Outcome outcome )
    {
        if ( outcome.electionTimeoutRenewed() )
        {
            electionTimer.renew();
        }
    }

    private void sendMessages( Outcome outcome )
    {
        for ( RaftMessages.Directed outgoingMessage : outcome.getOutgoingMessages() )
        {
            try
            {
                outbound.send( outgoingMessage.to(), outgoingMessage.message() );
            }
            catch ( Exception e )
            {
                log.warn( format( "Failed to send message %s.", outgoingMessage ), e );
            }
        }
    }

    @Override
    public boolean isLeader()
    {
        return currentRole == LEADER;
    }

    public Role currentRole()
    {
        return currentRole;
    }

    public MemberId identity()
    {
        return myself;
    }

    public RaftLogShippingManager logShippingManager()
    {
        return logShipping;
    }

    @Override
    public String toString()
    {
        return format( "RaftInstance{role=%s, term=%d, currentMembers=%s}", currentRole, term(), votingMembers() );
    }

    public static class BootstrapException extends Exception
    {
        BootstrapException( Throwable cause )
        {
            super( cause );
        }
    }

    public long term()
    {
        return state.term();
    }

    private long randomTimeoutRange()
    {
        return electionTimeout;
    }

    public Set<MemberId> votingMembers()
    {
        return membershipManager.votingMembers();
    }

    public Set<MemberId> replicationMembers()
    {
        return membershipManager.replicationMembers();
    }
}
