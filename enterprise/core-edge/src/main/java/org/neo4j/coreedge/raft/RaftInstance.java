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
package org.neo4j.coreedge.raft;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.neo4j.coreedge.helper.VolatileFuture;
import org.neo4j.coreedge.network.Message;
import org.neo4j.coreedge.raft.log.RaftLog;
import org.neo4j.coreedge.raft.log.RaftLogEntry;
import org.neo4j.coreedge.raft.log.RaftStorageException;
import org.neo4j.coreedge.raft.membership.RaftGroup;
import org.neo4j.coreedge.raft.membership.RaftMembershipManager;
import org.neo4j.coreedge.raft.net.Inbound;
import org.neo4j.coreedge.raft.net.Outbound;
import org.neo4j.coreedge.raft.outcome.AppendLogEntry;
import org.neo4j.coreedge.raft.outcome.CommitCommand;
import org.neo4j.coreedge.raft.outcome.LogCommand;
import org.neo4j.coreedge.raft.outcome.Outcome;
import org.neo4j.coreedge.raft.replication.ReplicatedContent;
import org.neo4j.coreedge.raft.replication.shipping.RaftLogShippingManager;
import org.neo4j.coreedge.raft.roles.Role;
import org.neo4j.coreedge.raft.state.LastAppliedTrackingStateMachine;
import org.neo4j.coreedge.raft.state.RaftState;
import org.neo4j.coreedge.raft.state.ReadableRaftState;
import org.neo4j.coreedge.raft.state.StateMachine;
import org.neo4j.coreedge.raft.state.StateStorage;
import org.neo4j.coreedge.raft.state.term.TermState;
import org.neo4j.coreedge.raft.state.vote.VoteState;
import org.neo4j.kernel.impl.util.Listener;
import org.neo4j.kernel.internal.DatabaseHealth;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.lang.String.format;
import static java.util.Arrays.asList;

import static org.neo4j.coreedge.raft.roles.Role.LEADER;

/**
 * The core raft class representing a member of the raft group. The main interactions are
 * with the network and the entry log.
 * <p/>
 * The network is represented by the inbound and outbound classes, and inbound messages are
 * handled by the core state machine, which in turn can generate outbound messages in
 * response.
 * <p/>
 * The raft entry log persists the user data which the raft system safely replicates. The raft
 * algorithm ensures that these logs eventually are fed with the exact same entries, even in
 * the face of failures.
 * <p/>
 * The main entry point for adding a new entry is the sendToLeader() function, which starts of
 * the process of safe replication. The new entry will be safely replicated and eventually
 * added to the local log through a call to the append() function of the entry log. Eventually
 * the leader will have replicated it safely, and at a later point in time the commit() function
 * of the entry log will be called.
 *
 * @param <MEMBER> The membership type.
 */
public class RaftInstance<MEMBER> implements LeaderLocator<MEMBER>, Inbound.MessageHandler, CoreMetaData
{
    private final LeaderNotFoundMonitor leaderNotFoundMonitor;
    private int flushAfter;

    public enum Timeouts implements RenewableTimeoutService.TimeoutName
    {
        ELECTION, HEARTBEAT
    }

    private final RaftState<MEMBER> raftState;
    private final MEMBER myself;
    private final RaftLog entryLog;

    private final RenewableTimeoutService renewableTimeoutService;
    private final long heartbeatInterval;
    private RenewableTimeoutService.RenewableTimeout electionTimer;
    private RaftMembershipManager<MEMBER> membershipManager;

    private final LastAppliedTrackingStateMachine stateMachine;
    private final long electionTimeout;
    private final long leaderWaitTimeout;

    private final Supplier<DatabaseHealth> databaseHealthSupplier;
    private final VolatileFuture<MEMBER> volatileLeader = new VolatileFuture<>( null );

    private final Outbound<MEMBER> outbound;
    private final Log log;
    private volatile boolean handlingMessage = false;
    private Role currentRole = Role.FOLLOWER;

    private RaftLogShippingManager<MEMBER> logShipping;

    public RaftInstance( MEMBER myself, StateStorage<TermState> termStorage,
                         StateStorage<VoteState<MEMBER>> voteStorage, RaftLog entryLog,
                         LastAppliedTrackingStateMachine stateMachine, long electionTimeout, long heartbeatInterval,
                         RenewableTimeoutService renewableTimeoutService,
                         final Inbound inbound, final Outbound<MEMBER> outbound, long leaderWaitTimeout,
                         LogProvider logProvider, RaftMembershipManager<MEMBER> membershipManager,
                         RaftLogShippingManager<MEMBER> logShipping,
                         Supplier<DatabaseHealth> databaseHealthSupplier,
                         Monitors monitors, int flushAfter )
    {
        this.myself = myself;
        this.entryLog = entryLog;
        this.stateMachine = stateMachine;
        this.electionTimeout = electionTimeout;
        this.heartbeatInterval = heartbeatInterval;

        this.renewableTimeoutService = renewableTimeoutService;

        this.leaderWaitTimeout = leaderWaitTimeout;
        this.outbound = outbound;
        this.logShipping = logShipping;
        this.databaseHealthSupplier = databaseHealthSupplier;
        this.flushAfter = flushAfter;
        this.log = logProvider.getLog( getClass() );

        this.membershipManager = membershipManager;

        this.raftState = new RaftState<>( myself, termStorage, membershipManager, entryLog, voteStorage );

        leaderNotFoundMonitor = monitors.newMonitor( LeaderNotFoundMonitor.class );

        initTimers();

        inbound.registerHandler( this );
    }

    private void initTimers()
    {
        electionTimer = renewableTimeoutService.create(
                Timeouts.ELECTION, electionTimeout, randomTimeoutRange(), timeout -> {
                    handle( new RaftMessages.Timeout.Election<>( myself ) );
                    timeout.renew();
                } );
        renewableTimeoutService.create(
                Timeouts.HEARTBEAT, heartbeatInterval, 0, timeout -> {
                    handle( new RaftMessages.Timeout.Heartbeat<>( myself ) );
                    timeout.renew();
                } );
    }

    /**
     * All members must be bootstrapped with the exact same set of initial members. Bootstrapping
     * requires an empty log as input and will seed it with the initial group entry in term 0.
     *
     * @param memberSet The other members.
     */
    public synchronized void bootstrapWithInitialMembers( RaftGroup<MEMBER> memberSet ) throws BootstrapException
    {
        if ( entryLog.appendIndex() >= 0 )
        {
            return;
        }

        RaftLogEntry membershipLogEntry = new RaftLogEntry( 0, memberSet );

        try
        {
            List<LogCommand> logCommands = asList(
                    new AppendLogEntry( 0, membershipLogEntry ),
                    new CommitCommand( 0 )
            );
            for ( LogCommand logCommand : logCommands )
            {
                logCommand.applyTo( entryLog );
            }
            membershipManager.processLog( logCommands );
        }
        catch ( RaftStorageException e )
        {
            databaseHealthSupplier.get().panic( e );
            throw new BootstrapException( e );
        }
    }

    public void setTargetMembershipSet( Set<MEMBER> targetMembers )
    {
        membershipManager.setTargetMembershipSet( targetMembers );

        if ( currentRole == LEADER )
        {
            membershipManager.onFollowerStateChange( raftState.followerStates() );
        }
    }

    @Override
    public MEMBER getLeader() throws NoLeaderFoundException
    {
        return getLeader( leaderWaitTimeout, member -> member != null );
    }

    public MEMBER getLeader( long timeoutMillis, Predicate<MEMBER> predicate ) throws NoLeaderFoundException
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

    private Collection<Listener<MEMBER>> leaderListeners = new ArrayList<>();

    @Override
    public synchronized void registerListener( Listener<MEMBER> listener )
    {
        leaderListeners.add( listener );
        listener.receive( raftState.leader() );
    }

    @Override
    public synchronized void unregisterListener( Listener<MEMBER> listener )
    {
        leaderListeners.remove( listener );
    }

    public ReadableRaftState<MEMBER> state()
    {
        return raftState;
    }

    private void handleOutcome( Outcome<MEMBER> outcome ) throws RaftStorageException, IOException
    {
        adjustLogShipping( outcome );
        notifyLeaderChanges( outcome );

        raftState.update( outcome );
        membershipManager.processLog( outcome.getLogCommands() );

        for ( long index = stateMachine.lastApplied() + 1; index <= raftState.entryLog().commitIndex(); index++ )
        {
            ReplicatedContent content = raftState.entryLog().readEntryContent( index );
            stateMachine.applyCommand( content, index );
            if ( index % this.flushAfter == 0 )
            {
                stateMachine.flush();
            }
        }
        volatileLeader.set( outcome.getLeader() );
    }

    private void notifyLeaderChanges( Outcome<MEMBER> outcome )
    {
        if ( leaderChanged( outcome, raftState.leader() ) )
        {
            for ( Listener<MEMBER> listener : leaderListeners )
            {
                listener.receive( outcome.getLeader() );
            }
        }
    }

    private void adjustLogShipping( Outcome<MEMBER> outcome ) throws RaftStorageException
    {
        MEMBER oldLeader = raftState.leader();

        if ( myself.equals( outcome.getLeader() ) )
        {
            LeaderContext leaderContext = new LeaderContext( outcome.getTerm(), outcome.getLeaderCommit() );

            if ( !myself.equals( oldLeader ) )
            {
                // We became leader, start the log shipping.
                logShipping.start( leaderContext );
            }

            logShipping.handleCommands( outcome.getShipCommands(), leaderContext );
        }
        else if ( myself.equals( oldLeader ) && !myself.equals( outcome.getLeader() ) )
        {
            logShipping.stop();
        }
    }

    private boolean leaderChanged( Outcome<MEMBER> outcome, MEMBER oldLeader )
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

    public synchronized void handle( Message incomingMessage )
    {
        if ( handlingMessage )
        {
            throw new IllegalStateException( "recursive use" );
        }

        try
        {
            handlingMessage = true;

            Outcome<MEMBER> outcome = currentRole.handler.handle( (RaftMessages.RaftMessage<MEMBER>) incomingMessage,
                    raftState, log );

            handleOutcome( outcome );
            currentRole = outcome.getNewRole();

            for ( RaftMessages.Directed<MEMBER> outgoingMessage : outcome.getOutgoingMessages() )
            {
                outbound.send( outgoingMessage.to(), outgoingMessage.message() );
            }
            if ( outcome.electionTimeoutRenewed() )
            {
                electionTimer.renew();
            }

            membershipManager.onRole( currentRole );

            if ( currentRole == LEADER )
            {
                membershipManager.onFollowerStateChange( raftState.followerStates() );
            }
        }
        catch ( RaftStorageException | IOException e )
        {
            log.error( "Failed to process RAFT message " + incomingMessage, e );
            databaseHealthSupplier.get().panic( e );
        }
        finally
        {
            handlingMessage = false;
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

    public MEMBER identity()
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
        public BootstrapException( Throwable cause )
        {
            super( cause );
        }
    }

    public long term()
    {
        return raftState.term();
    }

    private long randomTimeoutRange()
    {
        return electionTimeout;
    }

    public Set<MEMBER> votingMembers()
    {
        return membershipManager.votingMembers();
    }

    public Set<MEMBER> replicationMembers()
    {
        return membershipManager.replicationMembers();
    }
}