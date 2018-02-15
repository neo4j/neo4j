package org.neo4j.causalclustering.discovery;

import java.util.Map;
import java.util.stream.Collectors;

import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

public abstract class AbstractTopologyService extends LifecycleAdapter implements TopologyService
{

    @Override
    public CoreTopology coreServers( String database )
    {
        return coreServers().filterTopologyByDb( database );
    }

    @Override
    public ReadReplicaTopology readReplicas( String database )
    {
        return readReplicas().filterTopologyByDb( database );
    }


}
