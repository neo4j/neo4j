/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.configuration.helpers.SocketAddressParser;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.logging.LogProvider;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.virtual.MapValue;

import static org.neo4j.kernel.api.exceptions.Status.Database.DatabaseUnavailable;
import static org.neo4j.kernel.api.exceptions.Status.Procedure.ProcedureCallFailed;
import static org.neo4j.values.storable.Values.NO_VALUE;

public class SingleInstanceGetRoutingTableProcedure extends BaseGetRoutingTableProcedure
{
    private static final String DESCRIPTION = "Returns endpoints of this instance.";
    public static final String ADDRESS_CONTEXT_KEY = "address";

    private final ConnectorPortRegister portRegister;

    public SingleInstanceGetRoutingTableProcedure( List<String> namespace, DatabaseManager<?> databaseManager, ConnectorPortRegister portRegister,
            Config config, LogProvider logProvider )
    {
        super( namespace, databaseManager, config, logProvider );
        this.portRegister = portRegister;
    }

    @Override
    protected final String description()
    {
        return DESCRIPTION;
    }

    @Override
    protected RoutingResult invoke( NamedDatabaseId namedDatabaseId, MapValue routingContext ) throws ProcedureException
    {
        assertDatabaseIsOperational( namedDatabaseId );
        if ( config.get( BoltConnector.enabled ) )
        {
            return createRoutingResult( findAdvertisedBoltAddress( routingContext ), configuredRoutingTableTtl() );
        }
        throw new ProcedureException( ProcedureCallFailed, "Cannot get routing table for " + namedDatabaseId.name() +
                                                           " because Bolt is not enabled. Please update your configuration for '" +
                                                           BoltConnector.enabled.name() + "'" );
    }

    private void assertDatabaseIsOperational( NamedDatabaseId namedDatabaseId ) throws ProcedureException
    {
        Optional<Database> database = getDatabase( namedDatabaseId );
        if ( database.isEmpty() )
        {
            throw databaseNotFoundException( namedDatabaseId.name() );
        }
        if ( !database.get().getDatabaseAvailabilityGuard().isAvailable() )
        {
            throw new ProcedureException( DatabaseUnavailable,
                                          "Unable to get a routing table for database '" + namedDatabaseId.name() + "' because this database is unavailable" );
        }
    }

    protected RoutingResult createRoutingResult( SocketAddress address, long routingTableTtl )
    {
        var addresses = Collections.singletonList( address );
        return new RoutingResult( addresses, addresses, addresses, routingTableTtl );
    }

    private long configuredRoutingTableTtl()
    {
        return config.get( GraphDatabaseSettings.routing_ttl ).toMillis();
    }

    private SocketAddress findAdvertisedBoltAddress( MapValue routingContext ) throws ProcedureException
    {
        var clientProvidedAddress = findClientProvidedAddress( routingContext );
        var advertisedAddress = clientProvidedAddress.orElse( config.get( BoltConnector.advertised_address ) );

        if ( advertisedAddress.getPort() <= 0 )
        {
            // advertised address with a negative or zero port is not useful for callers of the routing procedure
            // attempt to resolve the actual port using the port register
            var localAddress = portRegister.getLocalAddress( BoltConnector.NAME );
            if ( localAddress != null )
            {
                advertisedAddress = new SocketAddress( advertisedAddress.getHostname(), localAddress.getPort() );
            }
        }
        return advertisedAddress;
    }

    private Optional<SocketAddress> findClientProvidedAddress( MapValue routingContext ) throws ProcedureException
    {
        var address = routingContext.get( ADDRESS_CONTEXT_KEY );
        if ( address == null  || address == NO_VALUE )
        {
            return Optional.empty();
        }

        if ( address instanceof TextValue )
        {
            try
            {
                return Optional.of( SocketAddressParser.socketAddress( ((TextValue) address).stringValue(), SocketAddress::new ) );
            }
            catch ( Exception e )
            { // Do nothing but warn
            }
        }

        throw new ProcedureException( Status.Procedure.ProcedureCallFailed, "An address key is included in the query string provided to the " +
                                                                            "GetRoutingTableProcedure, but its value could not be parsed." );
    }
}
