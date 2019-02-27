/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.procedure.builtin.routing;

import java.util.Collections;
import java.util.List;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.values.AnyValue;

import static java.util.Objects.requireNonNull;

public class CommunityGetRoutingTableProcedure extends BaseGetRoutingTableProcedure
{
    private static final String DESCRIPTION = "Returns endpoints of this instance.";

    private final ConnectorPortRegister portRegister;
    private final Config config;

    public CommunityGetRoutingTableProcedure( List<String> namespace, ConnectorPortRegister portRegister, Config config )
    {
        super( namespace );
        this.portRegister = portRegister;
        this.config = requireNonNull( config );
    }

    @Override
    protected final String description()
    {
        return DESCRIPTION;
    }

    @Override
    protected final RoutingResult invoke( AnyValue[] input )
    {
        return config.enabledBoltConnectors()
                .stream()
                .findFirst()
                .map( this::findAdvertisedAddress )
                .map( address -> createRoutingResult( address, configuredRoutingTableTtl() ) )
                .orElseGet( this::createEmptyRoutingResult );
    }

    protected RoutingResult createRoutingResult( AdvertisedSocketAddress address, long routingTableTtl )
    {
        List<AdvertisedSocketAddress> addresses = Collections.singletonList( address );
        return new RoutingResult( addresses, addresses, addresses, routingTableTtl );
    }

    private RoutingResult createEmptyRoutingResult()
    {
        List<AdvertisedSocketAddress> addresses = Collections.emptyList();
        return new RoutingResult( addresses, addresses, addresses, configuredRoutingTableTtl() );
    }

    private long configuredRoutingTableTtl()
    {
        return config.get( GraphDatabaseSettings.routing_ttl ).toMillis();
    }

    private AdvertisedSocketAddress findAdvertisedAddress( BoltConnector connector )
    {
        AdvertisedSocketAddress advertisedAddress = config.get( connector.advertised_address );
        if ( advertisedAddress.getPort() == 0 )
        {
            // advertised address with port zero is not useful for callers of the routing procedure
            // it is most likely inherited from the listen address
            // attempt to resolve the actual port using the port register
            HostnamePort localAddress = portRegister.getLocalAddress( connector.key() );
            if ( localAddress != null )
            {
                advertisedAddress = new AdvertisedSocketAddress( advertisedAddress.getHostname(), localAddress.getPort() );
            }
        }
        return advertisedAddress;
    }
}
