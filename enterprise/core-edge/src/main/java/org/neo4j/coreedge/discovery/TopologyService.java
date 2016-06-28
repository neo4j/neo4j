package org.neo4j.coreedge.discovery;

import org.neo4j.kernel.lifecycle.Lifecycle;

public interface TopologyService extends Lifecycle
{
    ClusterTopology currentTopology();
}
