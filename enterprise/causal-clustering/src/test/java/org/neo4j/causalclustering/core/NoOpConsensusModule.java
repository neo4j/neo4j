package org.neo4j.causalclustering.core;

import org.neo4j.causalclustering.core.consensus.ConsensusModuleIface;
import org.neo4j.causalclustering.core.consensus.RaftMachineIface;
import org.neo4j.causalclustering.core.consensus.log.RaftLog;
import org.neo4j.causalclustering.core.consensus.log.RaftLogEntry;
import org.neo4j.causalclustering.core.consensus.log.segmented.InFlightMap;
import org.neo4j.causalclustering.core.consensus.membership.RaftMembershipManager;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

import static org.mockito.Mockito.mock;

class NoOpConsensusModule implements ConsensusModuleIface
{
    Lifecycle lifecycle;
    RaftMembershipManager raftMembershipManager;
    InFlightMap<RaftLogEntry> inFlightMap;

    NoOpConsensusModule()
    {
        lifecycle = new LifecycleAdapter();
        raftMembershipManager = mock( RaftMembershipManager.class );
    }

    @Override
    public RaftLog raftLog()
    {
        return new NoOpRaftLog();
    }

    @Override
    public RaftMachineIface raftMachine()
    {
        return new NoOpRaftMachine();
    }

    @Override
    public Lifecycle raftTimeoutService()
    {
        return lifecycle;
    }

    @Override
    public RaftMembershipManager raftMembershipManager()
    {
        return raftMembershipManager;
    }

    @Override
    public InFlightMap<RaftLogEntry> inFlightMap()
    {
        return inFlightMap;
    }
}
