package org.neo4j.coreedge.server.edge;

import java.util.Iterator;
import java.util.Random;

import org.neo4j.coreedge.discovery.ClusterTopology;
import org.neo4j.coreedge.discovery.EdgeDiscoveryService;
import org.neo4j.coreedge.server.AdvertisedSocketAddress;
import org.neo4j.coreedge.server.CoreMember;

public class ConnectToRandomCoreServer implements EdgeToCoreConnectionStrategy
{
    private final EdgeDiscoveryService discoveryService;

    public ConnectToRandomCoreServer( EdgeDiscoveryService discoveryService)
    {
        this.discoveryService = discoveryService;
    }


    @Override
    public AdvertisedSocketAddress coreServer()
    {
        final Random random = new Random();

        final ClusterTopology clusterTopology = discoveryService.currentTopology();
        int randomSize = random.nextInt( clusterTopology.getMembers().size() );

        final Iterator<CoreMember> iterator = clusterTopology.getMembers().iterator();
        AdvertisedSocketAddress result = null;
        for ( int i = 0; i <= randomSize; i++ )
        {
            result = iterator.next().getCoreAddress();
        }
        return result;
    }
}
