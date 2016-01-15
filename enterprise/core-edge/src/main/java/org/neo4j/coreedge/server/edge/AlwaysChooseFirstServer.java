package org.neo4j.coreedge.server.edge;

import org.neo4j.coreedge.discovery.EdgeDiscoveryService;
import org.neo4j.coreedge.server.AdvertisedSocketAddress;

public class AlwaysChooseFirstServer implements EdgeToCoreConnectionStrategy
{
    private final EdgeDiscoveryService discoveryService;

    public AlwaysChooseFirstServer( EdgeDiscoveryService discoveryService)
    {
        this.discoveryService = discoveryService;
    }

    @Override
    public AdvertisedSocketAddress coreServer()
    {
        return discoveryService.currentTopology().firstTransactionServer();
    }
}
