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
package org.neo4j.coreedge.raft.membership;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.neo4j.coreedge.raft.log.RaftLog;
import org.neo4j.coreedge.raft.log.RaftLogEntry;
import org.neo4j.coreedge.raft.log.RaftStorageException;
import org.neo4j.coreedge.raft.log.ReadableRaftLog;
import org.neo4j.coreedge.raft.roles.Role;
import org.neo4j.coreedge.raft.state.FollowerStates;
import org.neo4j.coreedge.raft.replication.ReplicatedContent;
import org.neo4j.coreedge.raft.replication.Replicator;
import org.neo4j.helpers.Clock;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.util.Collections.emptySet;
import static org.neo4j.helpers.collection.IteratorUtil.first;

/**
 * This class drives raft membership changes by glueing together various components:
 *  - target membership from hazelcast
 *  - raft membership state machine
 *  - raft log events
 */
public class RaftMembershipManager<MEMBER> extends RaftMembershipImpl<MEMBER> implements RaftLog.Listener, MembershipDriver<MEMBER>
{
    private RaftMembershipStateMachine<MEMBER> membershipStateMachine;

    private Set<MEMBER> targetMembers = null;

    private int uncommittedMemberChanges = 0;

    private final Replicator replicator;
    private final RaftGroup.Builder<MEMBER> memberSetBuilder;
    private final ReadableRaftLog entryLog;
    private final Log log;
    private final int expectedClusterSize;

    public RaftMembershipManager( Replicator replicator, RaftGroup.Builder<MEMBER> memberSetBuilder, RaftLog entryLog,
            LogProvider logProvider, int expectedClusterSize, long electionTimeout,
            Clock clock, long catchupTimeout )
    {
        super( logProvider );
        this.replicator = replicator;
        this.memberSetBuilder = memberSetBuilder;
        this.entryLog = entryLog;
        this.expectedClusterSize = expectedClusterSize;
        this.log = logProvider.getLog( getClass() );

        this.membershipStateMachine = new RaftMembershipStateMachine<>( entryLog, clock, electionTimeout, this,
                logProvider, catchupTimeout, this );

        entryLog.registerListener( this );
    }

    @Override
    public void onAppended( ReplicatedContent content )
    {
        if ( content instanceof RaftGroup )
        {
            assert uncommittedMemberChanges >= 0;

            uncommittedMemberChanges++;

            RaftGroup<MEMBER> raftGroup = (RaftGroup) content;
            setVotingMembers( raftGroup.getMembers() );
        }
    }

    @Override
    public void onCommitted( ReplicatedContent content, long index )
    {
        if ( content instanceof RaftGroup )
        {
            assert uncommittedMemberChanges > 0;

            uncommittedMemberChanges--;

            if ( uncommittedMemberChanges == 0 )
            {
                membershipStateMachine.onRaftGroupCommitted();
            }
        }
    }

    @Override
    public void onTruncated( long fromIndex )
    {
        try
        {
            Long logIndex = findLastMembershipEntry();

            if ( logIndex != null )
            {
                RaftGroup<MEMBER> lastMembershipEntry = (RaftGroup<MEMBER>) entryLog.readEntryContent( logIndex );
                setVotingMembers( lastMembershipEntry.getMembers() );
            }
            else
            {
                setVotingMembers( Collections.<MEMBER>emptySet() );
            }

            uncommittedMemberChanges = 0;
            for ( long i = entryLog.commitIndex() + 1; i <= entryLog.appendIndex(); i++ )
            {
                ReplicatedContent content = entryLog.readEntryContent( i );
                if ( content instanceof RaftGroup )
                {
                    uncommittedMemberChanges++;
                }
            }
        }
        catch ( RaftStorageException e )
        {
            log.error( "Unable to find last membership entry after RAFT log truncation", e );
        }
    }

    private Long findLastMembershipEntry() throws RaftStorageException
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
            membershipStateMachine.onMissingMember( first( missingMembers() ) );
        }
        else if ( isSafeToRemoveMember() && superfluousMembers().size() > 0 )
        {
            membershipStateMachine.onSuperfluousMember( first( superfluousMembers() ) );
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
}
