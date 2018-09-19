/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.causalclustering.discovery;

import java.util.Collection;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.kernel.configuration.Config;

public class InitialDiscoveryMembersResolver implements RemoteMembersResolver
{
    private final HostnameResolver hostnameResolver;
    private final List<AdvertisedSocketAddress> advertisedSocketAddresses;

    public InitialDiscoveryMembersResolver( HostnameResolver hostnameResolver, Config config )
    {
        this.hostnameResolver = hostnameResolver;
        advertisedSocketAddresses = config.get( CausalClusteringSettings.initial_discovery_members );
    }

    @Override
    public <T> Collection<T> resolve( Function<AdvertisedSocketAddress,T> transform )
    {
        return advertisedSocketAddresses
                .stream()
                .flatMap( raw -> hostnameResolver.resolve( raw ).stream() )
                .map( transform )
                .collect( Collectors.toSet() );
    }
}
