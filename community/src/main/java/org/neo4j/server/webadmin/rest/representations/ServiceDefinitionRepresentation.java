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

package org.neo4j.server.webadmin.rest.representations;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import javax.ws.rs.core.UriBuilder;

import org.neo4j.server.rest.repr.MappingRepresentation;
import org.neo4j.server.rest.repr.MappingSerializer;

public class ServiceDefinitionRepresentation extends MappingRepresentation
{
    private final String baseUri;
    private final HashMap<String, URI> uris;
    private final HashMap<String, String> templates;

    public ServiceDefinitionRepresentation( String uri )
    {
        super( "service-definition" );
        this.baseUri = uri;
        uris = new HashMap<String, URI>();
        templates = new HashMap<String, String>();
    }

    public void resourceUri( String name, String subPath )
    {
        uris.put( name, UriBuilder.fromUri( baseUri ).path( subPath ).build() );
    }

    public void resourceTemplate( String name, String subPath )
    {
        templates.put( name, baseUri + subPath );
    }

    @Override
    public void serialize( MappingSerializer serializer )
    {
        serializer.putMapping( "resources", new MappingRepresentation( "resources" )
        {
            @Override
            protected void serialize( MappingSerializer resourceSerializer )
            {
                for ( Map.Entry<String, URI> entry : uris.entrySet() )
                {
                    resourceSerializer.putAbsoluteUri( entry.getKey(), entry.getValue() );
                }

                for ( Map.Entry<String, String> entry : templates.entrySet() )
                {
                    resourceSerializer.putAbsoluteUriTemplate( entry.getKey(), entry.getValue() );
                }
            }
        } );
    }
}
