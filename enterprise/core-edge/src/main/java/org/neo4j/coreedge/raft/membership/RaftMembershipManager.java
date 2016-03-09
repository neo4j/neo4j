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
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.neo4j.coreedge.raft.log.RaftLog;
import org.neo4j.coreedge.raft.log.RaftLogEntry;
import org.neo4j.coreedge.raft.log.ReadableRaftLog;
import org.neo4j.coreedge.raft.outcome.AppendLogEntry;
import org.neo4j.coreedge.raft.outcome.BatchAppendLogEntries;
import org.neo4j.coreedge.raft.outcome.CommitCommand;
import org.neo4j.coreedge.raft.outcome.LogCommand;
import org.neo4j.coreedge.raft.outcome.TruncateLogCommand;
import org.neo4j.coreedge.raft.replication.ReplicatedContent;
import org.neo4j.coreedge.raft.replication.Replicator;
import org.neo4j.coreedge.raft.roles.Role;
import org.neo4j.coreedge.raft.state.StateStorage;
import org.neo4j.coreedge.raft.state.follower.FollowerStates;
import org.neo4j.coreedge.raft.state.membership.RaftMembershipState;
import org.neo4j.cursor.IOCursor;
import org.neo4j.helpers.Clock;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.util.Collections.emptySet;

/**
 * This class drives raft membership changes by glueing together various components:
 * - target membership from hazelcast
 * - raft membership state machine
 * - raft log events
 */
public class RaftMembershipManager<MEMBER> implements RaftMembership<MEMBER>, MembershipDriver<MEMBER>
{
    private RaftMembershipStateMachine<MEMBER> membershipStateMachine;

    private Set<MEMBER> targetMembers = null;

    private int uncommittedMemberChanges = 0;

    private final Replicator replicator;
    private final RaftGroup.Builder<MEMBER> memberSetBuilder;
    private final ReadableRaftLog entryLog;
    private final Log log;
    private final int expectedClusterSize;
    private final StateStorage<RaftMembershipState<MEMBER>> stateStorage;
    private final RaftMembershipState<MEMBER> raftMembershipState;
    private long lastApplied = -1;

    public RaftMembershipManager( Replicator replicator, RaftGroup.Builder<MEMBER> memberSetBuilder, RaftLog entryLog,
                                  LogProvider logProvider, int expectedClusterSize, long electionTimeout,
                                  Clock clock, long catchupTimeout,
                                  StateStorage<RaftMembershipState<MEMBER>> stateStorage )
    {
        this.replicator = replicator;
        this.memberSetBuilder = memberSetBuilder;
        this.entryLog = entryLog;
        this.expectedClusterSize = expectedClusterSize;
        this.stateStorage = stateStorage;
        this.raftMembershipState = stateStorage.getInitialState();
        this.log = logProvider.getLog( getClass() );

        this.membershipStateMachine = new RaftMembershipStateMachine<>( entryLog, clock, electionTimeout, this,
                logProvider, catchupTimeout, raftMembershipState );
    }

    public void processLog( Collection<LogCommand> logCommands ) throws IOException
    {
        for ( LogCommand logCommand : logCommands )
        {
            if ( logCommand instanceof TruncateLogCommand )
            {
                onTruncated();
            }
            if ( logCommand instanceof AppendLogEntry )
            {
                AppendLogEntry command = (AppendLogEntry) logCommand;
                onAppended( command.entry.content(), command.index );
            }
            if ( logCommand instanceof BatchAppendLogEntries )
            {
                BatchAppendLogEntries command = (BatchAppendLogEntries) logCommand;
                for ( int i = command.offset; i < command.entries.length; i++ )
                {
                    onAppended( command.entries[i].content(), command.baseIndex + i );
                }
            }
            if ( logCommand instanceof CommitCommand )
            {
                long index = lastApplied + 1;
                try ( IOCursor<RaftLogEntry> entryCursor = entryLog.getEntryCursor( index ) )
                {
                    while ( entryCursor.next() )
                    {
                        if ( index == entryLog.commitIndex() + 1 )
                        {
                            break;
                        }
                        ReplicatedContent content = entryCursor.get().content();
                        onCommitted( content, index );
                        index++;
                    }
                }
                lastApplied = entryLog.commitIndex();
            }
        }
    }

    private void onAppended( ReplicatedContent content, long logIndex )
    {
        if ( content instanceof RaftGroup )
        {
            if ( logIndex > raftMembershipState.logIndex() )
            {
                assert uncommittedMemberChanges >= 0;

                uncommittedMemberChanges++;

                RaftGroup<MEMBER> raftGroup = (RaftGroup) content;
                raftMembershipState.setVotingMembers( raftGroup.getMembers() );
            }
            else
            {
                log.info( "Ignoring content at index %d, since already appended up to %d",
                        logIndex, raftMembershipState.logIndex() );
            }
        }
    }

    private void onCommitted( ReplicatedContent content, long logIndex ) throws IOException
    {
        if ( content instanceof RaftGroup )
        {
            if ( logIndex > raftMembershipState.logIndex() )
            {
                assert uncommittedMemberChanges > 0;

                uncommittedMemberChanges--;

                if ( uncommittedMemberChanges == 0 )
                {
                    membershipStateMachine.onRaftGroupCommitted();
                }
                raftMembershipState.logIndex( logIndex );
                stateStorage.persistStoreData( raftMembershipState );
            }
            else
            {
                log.info( "Ignoring content at index %d, since already committed up to %d",
                        logIndex, raftMembershipState.logIndex() );
            }
        }
    }

    private void onTruncated() throws IOException
    {
        Long logIndex = findLastMembershipEntry();

        if ( logIndex != null )
        {
            RaftGroup<MEMBER> lastMembershipEntry = (RaftGroup<MEMBER>) entryLog.readLogEntry( logIndex ).content();
            raftMembershipState.setVotingMembers( lastMembershipEntry.getMembers() );
            raftMembershipState.logIndex( logIndex );
            stateStorage.persistStoreData( raftMembershipState );
        }
        else
        {
            raftMembershipState.setVotingMembers( Collections.<MEMBER>emptySet() );
        }

        uncommittedMemberChanges = 0;
        for ( long i = entryLog.commitIndex() + 1; i <= entryLog.appendIndex(); i++ )
        {
            ReplicatedContent content = entryLog.readLogEntry( i ).content();
            if ( content instanceof RaftGroup )
            {
                uncommittedMemberChanges++;
            }
        }
    }

    private Long findLastMembershipEntry() throws IOException
    {
        for ( long logIndex = entryLog.appendIndex(); logIndex >= 0; logIndex-- )
        {
            RaftLogEntry raftLogEntry = entryLog.readLogEntry( logIndex );

            ReplicatedContent content = raftLogEntry.content();

            if ( content instanceof RaftGroup )
            {
                return logIndex;
            }
        }
        return null;
    }

    public void setTargetMembershipSet( Set<MEMBER> targetMembers )
    {
        this.targetMembers = new HashSet<>( targetMembers );

        log.info( "Target membership: " + targetMembers );
        membershipStateMachine.onTargetChanged( targetMembers );

        checkForStartCondition();
    }

    private Set<MEMBER> missingMembers()
    {
        if ( targetMembers == null || votingMembers() == null )
        {
            return emptySet();
        }
        Set<MEMBER> missingMembers = new HashSet<>( targetMembers );
        missingMembers.removeAll( votingMembers() );

        return missingMembers;
    }

    private boolean isSafeToRemoveMember()
    {
        return votingMembers() != null && votingMembers().size() > expectedClusterSize;
    }

    private Set<MEMBER> superfluousMembers()
    {
        if ( targetMembers == null || votingMembers() == null )
        {
            return emptySet();
        }
        Set<MEMBER> superfluousMembers = new HashSet<>( votingMembers() );
        superfluousMembers.removeAll( targetMembers );

        return superfluousMembers;
    }

    private void checkForStartCondition()
    {
        if ( missingMembers().size() > 0 )
        {
            membershipStateMachine.onMissingMember( Iterables.first( missingMembers() ) );
        }
        else if ( isSafeToRemoveMember() && superfluousMembers().size() > 0 )
        {
            membershipStateMachine.onSuperfluousMember( Iterables.first( superfluousMembers() ) );
        }
    }

    @Override
    public void doConsensus( Set<MEMBER> newVotingMemberSet )
    {
        try
        {
            replicator.replicate( memberSetBuilder.build( newVotingMemberSet ) );
        }
        catch ( Replicator.ReplicationFailedException e )
        {
            // TODO: log
        }
    }

    @Override
    public boolean uncommittedMemberChangeInLog()
    {
        return uncommittedMemberChanges > 0;
    }

    @Override
    public void stateChanged()
    {
        checkForStartCondition();
    }

    public void onFollowerStateChange( FollowerStates<MEMBER> followerStates )
    {
        membershipStateMachine.onFollowerStateChange( followerStates );
    }

    public void onRole( Role role )
    {
        membershipStateMachine.onRole( role );
    }

    @Override
    public Set<MEMBER> votingMembers()
    {
        return raftMembershipState.votingMembers();
    }

    @Override
    public Set<MEMBER> replicationMembers()
    {
        return raftMembershipState.replicationMembers();
    }

    @Override
    public long logIndex()
    {
        return raftMembershipState.logIndex();
    }

    @Override
    public void registerListener( Listener listener )
    {
        raftMembershipState.registerListener( listener );
    }

    @Override
    public void deregisterListener( Listener listener )
    {
        raftMembershipState.deregisterListener( listener );
    }
}
