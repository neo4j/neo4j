package org.neo4j.coreedge.server.edge;

import org.neo4j.coreedge.server.AdvertisedSocketAddress;

public interface EdgeToCoreConnectionStrategy
{
    AdvertisedSocketAddress coreServer();
}
