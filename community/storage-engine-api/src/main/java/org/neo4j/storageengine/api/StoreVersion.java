package org.neo4j.storageengine.api;

import org.neo4j.storageengine.api.format.Capability;

public interface StoreVersion
{
    String storeVersion();

    boolean hasCapability( Capability capability );
}
