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
package org.neo4j.server.rest.discovery;

import java.net.URISyntaxException;
import java.util.Optional;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.ConnectorPortRegister;
import org.neo4j.server.NeoServer;
import org.neo4j.server.configuration.ServerSettings;
import org.neo4j.server.rest.repr.DiscoveryRepresentation;
import org.neo4j.server.rest.repr.OutputFormat;

/**
 * Used to discover the rest of the server URIs through a HTTP GET request to
 * the server root (/).
 */
@Path( "/" )
public class DiscoveryService
{
    private final Config config;
    private final OutputFormat outputFormat;
    private final ConnectorPortRegister connectorPortRegister;

    // Your IDE might tell you to make this less visible than public. Don't. JAX-RS demands is to be public.
    public DiscoveryService( @Context Config config, @Context OutputFormat outputFormat, @Context NeoServer neoServer )
    {
        this.config = config;
        this.outputFormat = outputFormat;
        connectorPortRegister = neoServer.getDatabase().getGraph().getDependencyResolver()
                .resolveDependency( ConnectorPortRegister.class );
    }

    @GET
    @Produces( MediaType.APPLICATION_JSON )
    public Response getDiscoveryDocument( @Context UriInfo uriInfo )
    {
        String managementUri = config.get( ServerSettings.management_api_path ).getPath() + "/";
        String dataUri = config.get( ServerSettings.rest_api_path ).getPath() + "/";

        Optional<AdvertisedSocketAddress> boltAddress = config.enabledBoltConnectors().stream().findFirst()
                .map( boltConnector -> config.get( boltConnector.advertised_address ) );

        if ( boltAddress.isPresent() )
        {
            AdvertisedSocketAddress advertisedSocketAddress = boltAddress.get();

            // If port is 0 it's been assigned a random port from the OS, list this instead
            if ( advertisedSocketAddress.getPort() == 0 )
            {
                int boltPort = connectorPortRegister.getLocalAddress( "bolt" ).getPort();
                advertisedSocketAddress = new AdvertisedSocketAddress( advertisedSocketAddress.getHostname(), boltPort );
            }

            if ( advertisedSocketAddress.getHostname().equals( "localhost" ) )
            {
                // Use the port specified in the config, but not the host
                return outputFormat.ok( new DiscoveryRepresentation( managementUri, dataUri,
                        new AdvertisedSocketAddress( uriInfo.getBaseUri().getHost(), advertisedSocketAddress.getPort() ) ) );
            }
            else
            {
                // Use the config verbatim since it seems sane
                return outputFormat
                        .ok( new DiscoveryRepresentation( managementUri, dataUri, advertisedSocketAddress ) );
            }
        }
        else
        {
            // There's no config, compute possible endpoint using host header and default bolt port.
            return outputFormat.ok( new DiscoveryRepresentation( managementUri, dataUri,
                    new AdvertisedSocketAddress( uriInfo.getBaseUri().getHost(), 7687 ) ) );
        }
    }

    @GET
    @Produces( MediaType.WILDCARD )
    public Response redirectToBrowser()
    {
        return outputFormat.seeOther( config.get( ServerSettings.browser_path ) );
    }
}
