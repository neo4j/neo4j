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
package org.neo4j.coreedge.raft.membership;

import java.io.IOException;
import java.time.Clock;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.neo4j.coreedge.raft.log.RaftLogCursor;
import org.neo4j.coreedge.raft.log.RaftLogEntry;
import org.neo4j.coreedge.raft.log.ReadableRaftLog;
import org.neo4j.coreedge.raft.outcome.RaftLogCommand;
import org.neo4j.coreedge.raft.replication.SendToMyself;
import org.neo4j.coreedge.raft.roles.Role;
import org.neo4j.coreedge.raft.state.StateStorage;
import org.neo4j.coreedge.raft.state.follower.FollowerStates;
import org.neo4j.coreedge.raft.state.membership.RaftMembershipState;
import org.neo4j.coreedge.server.CoreMember;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.util.Collections.emptySet;
import static org.neo4j.helpers.collection.Iterables.first;

/**
 * This class drives raft membership changes by glueing together various components:
 * - target membership from hazelcast
 * - raft membership state machine
 * - raft log events
 */
public class RaftMembershipManager extends LifecycleAdapter implements RaftMembership, RaftLogCommand.Handler
{
    private RaftMembershipChanger membershipChanger;

    private Set<CoreMember> targetMembers = null;

    private final SendToMyself sendToMyself;
    private final RaftGroup.Builder<CoreMember> memberSetBuilder;
    private final ReadableRaftLog raftLog;
    private final Log log;
    private final long recoverFromIndex;

    private final StateStorage<RaftMembershipState> storage;
    private final RaftMembershipState state;

    private final int expectedClusterSize;

    private volatile Set<CoreMember> votingMembers = new HashSet<>();
    private volatile Set<CoreMember> replicationMembers = new HashSet<>(); // votingMembers + additionalReplicationMembers

    private Set<Listener> listeners = new HashSet<>();
    private Set<CoreMember> additionalReplicationMembers = new HashSet<>();

    public RaftMembershipManager( SendToMyself sendToMyself, RaftGroup.Builder<CoreMember> memberSetBuilder,
            ReadableRaftLog raftLog, LogProvider logProvider, int expectedClusterSize, long electionTimeout,
            Clock clock, long catchupTimeout, StateStorage<RaftMembershipState> membershipStorage, long recoverFromIndex )
    {
        this.sendToMyself = sendToMyself;
        this.memberSetBuilder = memberSetBuilder;
        this.raftLog = raftLog;
        this.expectedClusterSize = expectedClusterSize;
        this.storage = membershipStorage;
        this.state = membershipStorage.getInitialState();

        this.log = logProvider.getLog( getClass() );
        this.recoverFromIndex = recoverFromIndex;
        this.membershipChanger = new RaftMembershipChanger( raftLog, clock,
                electionTimeout, logProvider, catchupTimeout, this );
    }

    @Override
    public void start() throws Throwable
    {
        log.info( "Membership state before recovery: " + state );
        log.info( "Recovering from: " + recoverFromIndex + " to: " + raftLog.appendIndex() );

        try ( RaftLogCursor cursor = raftLog.getEntryCursor( recoverFromIndex ) )
        {
            while ( cursor.next() )
            {
                append( cursor.index(), cursor.get() );
            }
        }

        log.info( "Membership state after recovery: " + state );
        updateMemberSets();
    }

    public void setTargetMembershipSet( Set<CoreMember> targetMembers )
    {
        this.targetMembers = new HashSet<>( targetMembers );

        log.info( "Target membership: " + targetMembers );
        membershipChanger.onTargetChanged( targetMembers );

        checkForStartCondition();
    }

    private Set<CoreMember> missingMembers()
    {
        if ( targetMembers == null || votingMembers() == null )
        {
            return emptySet();
        }
        Set<CoreMember> missingMembers = new HashSet<>( targetMembers );
        missingMembers.removeAll( votingMembers() );

        return missingMembers;
    }

    /**
     * All the externally published sets are derived from the committed and appended sets.
     */
    private void updateMemberSets()
    {
        votingMembers = state.getLatest();

        HashSet<CoreMember> newReplicationMembers = new HashSet<>( votingMembers );
        newReplicationMembers.addAll( additionalReplicationMembers );

        replicationMembers = newReplicationMembers;
        notifyListeners();
    }

    /**
     * Adds an additional member to replicate to. Members that are joining need to
     * catch up sufficiently before they become part of the voting group.
     *
     * @param member The member which will be added to the replication group.
     */
    void addAdditionalReplicationMember( CoreMember member )
    {
        additionalReplicationMembers.add( member );
        updateMemberSets();
    }

    /**
     * Removes a member previously part of the additional replication member group.
     *
     * This either happens because they caught up sufficiently and became part of the
     * voting group or because they failed to catch up in time.
     *
     * @param member The member to remove from the replication group.
     */
    void removeAdditionalReplicationMember( CoreMember member )
    {
        additionalReplicationMembers.remove( member );
        updateMemberSets();
    }

    private boolean isSafeToRemoveMember()
    {
        return votingMembers() != null && votingMembers().size() > expectedClusterSize;
    }

    private Set<CoreMember> superfluousMembers()
    {
        if ( targetMembers == null || votingMembers() == null )
        {
            return emptySet();
        }
        Set<CoreMember> superfluousMembers = new HashSet<>( votingMembers() );
        superfluousMembers.removeAll( targetMembers );

        return superfluousMembers;
    }

    private void checkForStartCondition()
    {
        if ( missingMembers().size() > 0 )
        {
            membershipChanger.onMissingMember( first( missingMembers() ) );
        }
        else if ( isSafeToRemoveMember() && superfluousMembers().size() > 0 )
        {
            membershipChanger.onSuperfluousMember( first( superfluousMembers() ) );
        }
    }

    /**
     * Used by the membership changer for getting consensus on a new set of members.
     *
     * @param newVotingMemberSet The new set of members.
     */
    void doConsensus( Set<CoreMember> newVotingMemberSet )
    {
        sendToMyself.replicate( memberSetBuilder.build( newVotingMemberSet ) );
    }

    /**
     * Called by the membership changer when it has changed state and in response
     * the membership manager potentially feeds it back with an event to start
     * a new membership change operation.
     */
    void stateChanged()
    {
        checkForStartCondition();
    }

    public void onFollowerStateChange( FollowerStates<CoreMember> followerStates )
    {
        membershipChanger.onFollowerStateChange( followerStates );
    }

    public void onRole( Role role )
    {
        membershipChanger.onRole( role );
    }

    @Override
    public Set<CoreMember> votingMembers()
    {
        return votingMembers;
    }

    @Override
    public Set<CoreMember> replicationMembers()
    {
        return replicationMembers;
    }

    @Override
    public synchronized void registerListener( Listener listener )
    {
        listeners.add( listener );
    }

    private synchronized void notifyListeners()
    {
        listeners.forEach( Listener::onMembershipChanged );
    }

    boolean uncommittedMemberChangeInLog()
    {
        return state.uncommittedMemberChangeInLog();
    }

    public void processLog( long commitIndex, Collection<RaftLogCommand> logCommands ) throws IOException
    {
        for ( RaftLogCommand logCommand : logCommands )
        {
            logCommand.dispatch( this );
        }

        if ( state.commit( commitIndex ) )
        {
            membershipChanger.onRaftGroupCommitted();
            storage.persistStoreData( state );
            updateMemberSets();
        }
    }

    @Override
    public void append( long baseIndex, RaftLogEntry... entries ) throws IOException
    {
        /* The warnings in this method are rarely expected occurrences which warrant to be logged with significance. */

        for ( RaftLogEntry entry : entries )
        {
            if ( entry.content() instanceof RaftGroup )
            {
                RaftGroup<CoreMember> raftGroup = (RaftGroup<CoreMember>) entry.content();

                if ( state.uncommittedMemberChangeInLog() )
                {
                    log.warn( "Appending with uncommitted membership change in log" );
                }

                if ( state.append( baseIndex, new HashSet<>( raftGroup.getMembers() ) ) )
                {
                    storage.persistStoreData( state );
                    updateMemberSets();
                }
                else
                {
                    log.warn( "Appending member set was ignored. Current state: %s, Appended set: %s, Log index: %d%n", state, raftGroup, baseIndex );
                }
            }
            baseIndex++;
        }
    }

    @Override
    public void truncate( long fromIndex ) throws IOException
    {
        if ( state.truncate( fromIndex ) )
        {
            storage.persistStoreData( state );
            updateMemberSets();
        }
    }
}
