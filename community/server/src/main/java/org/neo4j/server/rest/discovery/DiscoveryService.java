/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.server.rest.discovery;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Request;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import javax.ws.rs.core.Variant;
import org.neo4j.configuration.Config;
import org.neo4j.server.NeoWebServer;
import org.neo4j.server.config.AuthConfigProvider;
import org.neo4j.server.configuration.ServerSettings;
import org.neo4j.server.rest.repr.DiscoveryRepresentation;

/**
 * Used to discover the rest of the server URIs through a HTTP GET request to the server root (/).
 */
@Path("/")
public class DiscoveryService {
    private final Config config;
    private final DiscoverableURIs uris;
    private final ServerVersionAndEdition serverInfo;
    private final AuthConfigProvider authConfigProvider;

    // Your IDE might tell you to make this less visible than public. Don't. JAX-RS demands is to be public.
    public DiscoveryService(
            @Context Config config,
            @Context DiscoverableURIs uris,
            @Context NeoWebServer neoWebServer,
            @Context AuthConfigProvider authConfigProvider) {
        this(config, uris, new ServerVersionAndEdition(neoWebServer), authConfigProvider);
    }

    // Used in internal unit test to avoid providing a neo server
    DiscoveryService(
            Config config,
            DiscoverableURIs uris,
            ServerVersionAndEdition serverInfo,
            AuthConfigProvider authConfigProvider) {
        this.config = config;
        this.uris = uris;
        this.serverInfo = serverInfo;
        this.authConfigProvider = authConfigProvider;
    }

    @GET
    @Produces(MediaType.WILDCARD)
    public Response get(@Context Request request, @Context UriInfo uriInfo) {

        Variant v = request.selectVariant(Variant.mediaTypes(MediaType.APPLICATION_JSON_TYPE, MediaType.TEXT_HTML_TYPE)
                .build());
        Response.ResponseBuilder responseBuilder;
        if (v == null) {
            responseBuilder = Response.serverError().status(Response.Status.NOT_ACCEPTABLE);
        } else if (v.getMediaType() == MediaType.APPLICATION_JSON_TYPE) {
            responseBuilder = Response.ok()
                    .entity(new DiscoveryRepresentation(
                            uris.update(uriInfo.getBaseUri()), serverInfo, authConfigProvider.getRepresentation()))
                    .variant(v);
        } else {
            responseBuilder = Response.seeOther(uriInfo.getBaseUri().resolve(config.get(ServerSettings.browser_path)))
                    .variant(v);
        }

        return responseBuilder.build();
    }
}
