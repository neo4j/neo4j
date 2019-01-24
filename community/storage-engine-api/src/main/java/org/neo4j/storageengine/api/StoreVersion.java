package org.neo4j.storageengine.api;

import org.neo4j.storageengine.api.format.Capability;
import org.neo4j.storageengine.api.format.CapabilityType;

public interface StoreVersion
{
    String storeVersion();

    boolean hasCapability( Capability capability );

    boolean hasCompatibleCapabilities( StoreVersion otherVersion, CapabilityType type );
}
