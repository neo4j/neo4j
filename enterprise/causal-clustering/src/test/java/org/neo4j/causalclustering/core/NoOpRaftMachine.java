package org.neo4j.causalclustering.core;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.neo4j.causalclustering.core.consensus.NoLeaderFoundException;
import org.neo4j.causalclustering.core.consensus.RaftMachineIface;
import org.neo4j.causalclustering.core.consensus.RaftMessages;
import org.neo4j.causalclustering.core.consensus.membership.MembershipEntry;
import org.neo4j.causalclustering.core.consensus.outcome.ConsensusOutcome;
import org.neo4j.causalclustering.core.consensus.roles.Role;
import org.neo4j.causalclustering.core.consensus.shipping.RaftLogShippingManager;
import org.neo4j.causalclustering.core.consensus.state.ExposedRaftState;
import org.neo4j.causalclustering.core.state.snapshot.RaftCoreState;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.kernel.impl.util.Listener;

class NoOpRaftMachine implements RaftMachineIface
{

    private Set<MemberId> memberIds = new HashSet<>();

    @Override
    public void postRecoveryActions()
    {
    }

    @Override
    public void stopTimers()
    {
    }

    @Override
    public void triggerElection() throws IOException
    {
    }

    @Override
    public void panic()
    {
    }

    @Override
    public RaftCoreState coreState()
    {
        return new RaftCoreState( new MembershipEntry( 1, memberIds ) );
    }

    @Override
    public void installCoreState( RaftCoreState coreState ) throws IOException
    {
    }

    @Override
    public void setTargetMembershipSet( Set<MemberId> targetMembers )
    {
        memberIds = targetMembers;
    }

    @Override
    public ExposedRaftState state()
    {
        return new ExposedRaftState()
        {
            @Override
            public long lastLogIndexBeforeWeBecameLeader()
            {
                return -1; // TODO could be issue
            }

            @Override
            public long leaderCommit()
            {
                return 1;
            }

            @Override
            public long commitIndex()
            {
                return 1;
            }

            @Override
            public long appendIndex()
            {
                System.out.println( "REQUESTED INDEX" ); // TODO verify this is ever happening
                return -1;
//                    return 1;
            }

            @Override
            public long term()
            {
                return 1;
            }

            @Override
            public Set<MemberId> votingMembers()
            {
                return new HashSet<>();
            }
        };
    }

    @Override
    public ConsensusOutcome handle( RaftMessages.RaftMessage incomingMessage ) throws IOException
    {
        throw new RuntimeException( "Unimplemented" );
    }

    @Override
    public Role currentRole()
    {
        throw new RuntimeException( "Unimplemented" );
    }

    @Override
    public MemberId identity()
    {
        throw new RuntimeException( "Unimplemented" );
    }

    @Override
    public RaftLogShippingManager logShippingManager()
    {
        throw new RuntimeException( "Unimplemented" );
    }

    @Override
    public long term()
    {
        throw new RuntimeException( "Unimplemented" );
    }

    @Override
    public Set<MemberId> votingMembers()
    {
        return new HashSet<>();
    }

    @Override
    public Set<MemberId> replicationMembers()
    {
        throw new RuntimeException( "Unimplemented" );
    }

    @Override
    public boolean isLeader()
    {
        return true;
    }

    @Override
    public MemberId getLeader() throws NoLeaderFoundException
    {
        throw new RuntimeException( "Unimplemented" );
    }

    @Override
    public void registerListener( Listener<MemberId> listener )
    {
    }

    @Override
    public void unregisterListener( Listener<MemberId> listener )
    {
    }
}
