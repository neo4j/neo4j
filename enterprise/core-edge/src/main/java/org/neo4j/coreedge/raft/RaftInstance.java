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
package org.neo4j.coreedge.raft;

import java.io.Serializable;
import java.util.Set;
import java.util.concurrent.locks.LockSupport;

import org.neo4j.coreedge.raft.log.RaftLog;
import org.neo4j.coreedge.raft.log.RaftLogEntry;
import org.neo4j.coreedge.raft.log.RaftStorageException;
import org.neo4j.coreedge.raft.membership.RaftGroup;
import org.neo4j.coreedge.raft.membership.RaftMembershipManager;
import org.neo4j.coreedge.raft.net.Inbound;
import org.neo4j.coreedge.raft.net.Outbound;
import org.neo4j.coreedge.raft.outcome.Outcome;
import org.neo4j.coreedge.raft.replication.shipping.RaftLogShippingManager;
import org.neo4j.coreedge.raft.roles.Role;
import org.neo4j.coreedge.raft.state.RaftState;
import org.neo4j.coreedge.raft.state.ReadableRaftState;
import org.neo4j.coreedge.raft.state.TermStore;
import org.neo4j.coreedge.raft.state.VoteStore;
import org.neo4j.coreedge.server.core.RaftStorageExceptionHandler;
import org.neo4j.helpers.Clock;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

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
public class RaftInstance<MEMBER> implements LeaderLocator<MEMBER>, Inbound.MessageHandler
{
    public enum Timeouts implements RenewableTimeoutService.TimeoutName
    {
        ELECTION, HEARTBEAT
    }

    private final RaftState<MEMBER> state;
    private final MEMBER myself;
    private final RaftLog entryLog;

    private final RenewableTimeoutService renewableTimeoutService;
    private final long heartbeatInterval;
    private RenewableTimeoutService.RenewableTimeout electionTimer;
    private RaftMembershipManager<MEMBER> membershipManager;

    private final long electionTimeout;
    private final long leaderWaitTimeout;

    private final RaftStorageExceptionHandler raftStorageExceptionHandler;
    private Clock clock;

    private final Outbound<MEMBER> outbound;
    private final Log log;
    private volatile boolean handlingMessage = false;
    private Role currentRole = Role.FOLLOWER;

    private RaftLogShippingManager<MEMBER> logShipping;

    public RaftInstance( MEMBER myself, TermStore termStore, VoteStore<MEMBER> voteStore, RaftLog entryLog,
                         long electionTimeout, long heartbeatInterval, RenewableTimeoutService renewableTimeoutService,
                         final Inbound inbound, final Outbound<MEMBER> outbound, long leaderWaitTimeout,
                         LogProvider logProvider, RaftMembershipManager<MEMBER> membershipManager,
                         RaftLogShippingManager<MEMBER> logShipping,
                         RaftStorageExceptionHandler raftStorageExceptionHandler,
                         Clock clock )

    {
        this.myself = myself;
        this.entryLog = entryLog;
        this.electionTimeout = electionTimeout;
        this.heartbeatInterval = heartbeatInterval;

        this.renewableTimeoutService = renewableTimeoutService;

        this.leaderWaitTimeout = leaderWaitTimeout;
        this.outbound = outbound;
        this.logShipping = logShipping;
        this.raftStorageExceptionHandler = raftStorageExceptionHandler;
        this.clock = clock;
        this.log = logProvider.getLog( getClass() );

        this.membershipManager = membershipManager;

        this.state = new RaftState<>( myself, termStore, membershipManager, entryLog, voteStore );

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
            entryLog.append( membershipLogEntry );
            entryLog.commit( 0 );
        }
        catch ( RaftStorageException e )
        {
            raftStorageExceptionHandler.panic( e );
            throw new BootstrapException( e );
        }
    }

    public void setTargetMembershipSet( Set<MEMBER> targetMembers )
    {
        membershipManager.setTargetMembershipSet( targetMembers );

        if ( currentRole == LEADER )
        {
            membershipManager.onFollowerStateChange( state.followerStates() );
        }
    }

    @Override
    public MEMBER getLeader() throws NoLeaderTimeoutException
    {
        long leaderWaitEndTime = leaderWaitTimeout + clock.currentTimeMillis();
        while ( state.leader() == null && (clock.currentTimeMillis() < leaderWaitEndTime) )
        {
            LockSupport.parkNanos( MILLISECONDS.toNanos( electionTimeout ) / 2 );
        }

        if ( state.leader() == null )
        {
            throw new NoLeaderTimeoutException();
        }

        return state.leader();
    }

    public ReadableRaftState<MEMBER> state()
    {
        return state;
    }

    protected void handleOutcome( Outcome<MEMBER> outcome ) throws RaftStorageException
    {
        // Save interesting pre-state
        MEMBER oldLeader = state.leader();

        if( outcome.getLeader() != null && outcome.getLeader().equals( myself ) )
        {
            LeaderContext leaderContext = new LeaderContext( outcome.getTerm(), outcome.getLeaderCommit() );

            if ( oldLeader == null || !oldLeader.equals( myself ) )
            {
                // We became leader, start the log shipping.
                logShipping.start( leaderContext );
            }

            logShipping.handleCommands( outcome.getShipCommands(), leaderContext );
        }
        else if ( oldLeader != null && oldLeader.equals( myself ) && !outcome.getLeader().equals( myself ) ) // TODO: inspect the reported issue
        {
            logShipping.stop();
        }

        // Update state
        state.update( outcome );
    }

    public synchronized void handle( Serializable incomingMessage )
    {
        if ( handlingMessage )
        {
            throw new IllegalStateException( "recursive use" );
        }

        try
        {
            handlingMessage = true;
            Outcome<MEMBER> outcome = currentRole.role.handle( (RaftMessages.Message<MEMBER>) incomingMessage, state, log );

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
                membershipManager.onFollowerStateChange( state.followerStates() );
            }
        }
        catch ( RaftStorageException e )
        {
            log.error( "Failed to process RAFT message " + incomingMessage, e );
            raftStorageExceptionHandler.panic( e );
        }
        finally
        {
            handlingMessage = false;
        }
    }

    public boolean isLeader()
    {
        return currentRole == LEADER;
    }

    public Role currentRole()
    {
        return currentRole;
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
        return state.term();
    }

    private long randomTimeoutRange()
    {
        return electionTimeout / 3;
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
