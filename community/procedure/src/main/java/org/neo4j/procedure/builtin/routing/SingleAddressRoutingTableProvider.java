/*
 * Copyright (c) "Neo4j"
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
import java.util.Optional;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.values.virtual.MapValue;

import static org.neo4j.procedure.builtin.routing.RoutingTableProcedureHelpers.findClientProvidedAddress;

/**
 * This routing table provider returns a routing table containing a single address.
 * <p>
 * It will prefer to return the address provided by the client in the routingContext. If it cannot find that address it will use the address of the instance's
 * bolt server.
 */
public class SingleAddressRoutingTableProvider implements ClientSideRoutingTableProvider, ServerSideRoutingTableProvider
{
    private final ConnectorPortRegister portRegister;
    private final RoutingOption routingOption;
    private final RoutingTableTTLProvider routingTableTTLProvider;
    private final Config config;
    private final Log log;

    public SingleAddressRoutingTableProvider( ConnectorPortRegister portRegister, RoutingOption routingOption, Config config, LogProvider logProvider,
            RoutingTableTTLProvider ttlProvider )
    {
        this.portRegister = portRegister;
        this.routingOption = routingOption;
        this.routingTableTTLProvider = ttlProvider;
        this.config = config;
        this.log = logProvider.getLog( getClass() );
    }

    @Override
    public RoutingResult getRoutingResultForClientSideRouting( NamedDatabaseId databaseId, MapValue routingContext ) throws ProcedureException
    {
        return createSingleAddressRoutingResult( findBoltAddressToUse( routingContext ), routingTableTTLProvider.nextTTL().toMillis(), routingOption );
    }

    private static RoutingResult createSingleAddressRoutingResult( SocketAddress address, long routingTableTtl, RoutingOption option )
    {
        var addresses = List.of( address );
        List<SocketAddress> routeEndpoints = option.route ? addresses : Collections.emptyList();
        List<SocketAddress> writeEndpoints = option.write ? addresses : Collections.emptyList();
        List<SocketAddress> readEndpoints = option.read ? addresses : Collections.emptyList();
        return new RoutingResult( routeEndpoints, writeEndpoints, readEndpoints, routingTableTtl );
    }

    private SocketAddress findBoltAddressToUse( MapValue routingContext ) throws ProcedureException
    {
        var addressToUse = findClientProvidedAddress( routingContext, BoltConnector.DEFAULT_PORT, log );

        return ensureBoltAddressIsUsable( addressToUse );
    }

    private SocketAddress ensureBoltAddressIsUsable( Optional<SocketAddress> address )
    {
        var addressToUse = address.filter( c -> c.getPort() > 0 )
                                  .orElse( config.get( BoltConnector.advertised_address ) );

        if ( addressToUse.getPort() <= 0 )
        {
            // advertised address with a negative or zero port is not useful for callers of the routing procedure
            // attempt to resolve the actual port using the port register
            var localAddress = portRegister.getLocalAddress( BoltConnector.NAME );
            if ( localAddress != null )
            {
                addressToUse = new SocketAddress( addressToUse.getHostname(), localAddress.getPort() );
            }
        }
        return addressToUse;
    }

    @Override
    public RoutingResult getServerSideRoutingTable( Optional<SocketAddress> clientProvidedAddress )
    {
        SocketAddress address = ensureBoltAddressIsUsable( clientProvidedAddress );
        return createSingleAddressRoutingResult( address, routingTableTTLProvider.nextTTL().toMillis(), routingOption );
    }
}
