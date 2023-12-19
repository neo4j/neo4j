/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.causalclustering.scenarios;

import java.util.function.Supplier;

import org.neo4j.causalclustering.discovery.DiscoveryServiceFactory;
import org.neo4j.causalclustering.discovery.HazelcastDiscoveryServiceFactory;
import org.neo4j.causalclustering.discovery.SharedDiscoveryService;
import org.neo4j.causalclustering.discovery.SharedDiscoveryServiceFactory;

public enum DiscoveryServiceType
{
    SHARED( SharedDiscoveryServiceFactory::new ),
    HAZELCAST( HazelcastDiscoveryServiceFactory::new );

    private final Supplier<DiscoveryServiceFactory> factory;

    DiscoveryServiceType( Supplier<DiscoveryServiceFactory> factory )
    {
        this.factory = factory;
    }

    public DiscoveryServiceFactory create()
    {
        return factory.get();
    }

}
