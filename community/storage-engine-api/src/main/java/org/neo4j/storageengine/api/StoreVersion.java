package org.neo4j.storageengine.api;

import java.util.Optional;

import org.neo4j.storageengine.api.format.Capability;
import org.neo4j.storageengine.api.format.CapabilityType;

public interface StoreVersion
{
    String storeVersion();

    boolean hasCapability( Capability capability );

    boolean hasCompatibleCapabilities( StoreVersion otherVersion, CapabilityType type );

    /**
     * @return the neo4j version where this format was introduced. It is almost certainly NOT the only version of
     * neo4j where this format is used.
     */
    String introductionNeo4jVersion();

    Optional<StoreVersion> successor();

    boolean isCompatibleWith( StoreVersion otherVersion );
}
