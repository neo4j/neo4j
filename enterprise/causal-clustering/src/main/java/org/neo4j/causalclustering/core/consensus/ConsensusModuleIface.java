package org.neo4j.causalclustering.core.consensus;

import org.neo4j.causalclustering.core.consensus.log.RaftLog;
import org.neo4j.causalclustering.core.consensus.log.RaftLogEntry;
import org.neo4j.causalclustering.core.consensus.log.segmented.InFlightMap;
import org.neo4j.causalclustering.core.consensus.membership.RaftMembershipManager;
import org.neo4j.kernel.lifecycle.Lifecycle;

public interface ConsensusModuleIface
{
    RaftLog raftLog();

    RaftMachineIface raftMachine();

    Lifecycle raftTimeoutService();

    RaftMembershipManager raftMembershipManager();

    InFlightMap<RaftLogEntry> inFlightMap();
}
