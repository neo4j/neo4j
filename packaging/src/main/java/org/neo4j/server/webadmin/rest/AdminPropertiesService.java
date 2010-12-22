package org.neo4j.server.webadmin.rest;

/**
 * Copyright (c) 2002-2010 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import org.neo4j.server.NeoServer;
import org.neo4j.server.configuration.Configurator;
import org.neo4j.server.rest.repr.MappingRepresentation;
import org.neo4j.server.rest.repr.MappingSerializer;
import org.neo4j.server.rest.repr.OutputFormat;
import org.neo4j.server.rest.repr.ValueRepresentation;

@Path("/properties")
public class AdminPropertiesService {
    private final UriInfo uriInfo;
    private final OutputFormat output;
    private final NeoServer server;

    public AdminPropertiesService(@Context UriInfo uriInfo, @Context NeoServer server, @Context OutputFormat output) {
        this.uriInfo = uriInfo;
        this.server = server;
        this.output = output;
    }

    @GET
    @Path("/{key}")
    public Response getValue(@PathParam("key") String key) {
        String lowerCaseKey = key.toLowerCase();
        String value = null;

        if ("neo4j-servers".equals(lowerCaseKey)) {
            return output.ok(new MappingRepresentation("neo4j-servers") {
                @Override
                protected void serialize(MappingSerializer serializer) {
                    serializer.putMapping(uriInfo.getBaseUri().toString(), new MappingRepresentation("urls") {
                        @Override
                        protected void serialize(MappingSerializer serializer) {
                            serializer.putString("url", server.restApiUri().toString());
                            serializer.putString("manageUrl", server.managementApiUri().toString());
                        }
                    });
                }
            });
        } else if (Configurator.WEB_ADMIN_REST_API_PATH_PROPERTY_KEY.endsWith(lowerCaseKey)) {
            value = server.restApiUri().toString();
        } else if (Configurator.WEB_ADMIN_PATH_PROPERTY_KEY.endsWith(lowerCaseKey)) {
            value = server.managementApiUri().toString();
        } else {
            if(server.getConfiguration().containsKey(lowerCaseKey)) {
                value = (String) server.getConfiguration().getProperty(lowerCaseKey);
            }
        }

        if (value == null) {
            value = "undefined";
        }
        return output.ok(ValueRepresentation.string(value));
    }
}
