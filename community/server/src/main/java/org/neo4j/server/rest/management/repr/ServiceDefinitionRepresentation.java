/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.server.rest.management.repr;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.server.rest.repr.MappingRepresentation;
import org.neo4j.server.rest.repr.MappingSerializer;

public class ServiceDefinitionRepresentation extends MappingRepresentation
{
    private final HashMap<String, String> uris;
    private final HashMap<String, String> templates;
    private final String basePath;

    public ServiceDefinitionRepresentation( String basePath )
    {
        super( "service-definition" );
        this.basePath = basePath;
        uris = new HashMap<String, String>();
        templates = new HashMap<String, String>();
    }

    public void resourceUri( String name, String subPath )
    {
        uris.put( name, relative( subPath ) );
    }

    public void resourceTemplate( String name, String subPath )
    {
        templates.put( name, relative( subPath ) );
    }

    private String relative( String subPath )
    {
        if ( basePath.endsWith( "/" ) )
        {
            if ( subPath.startsWith( "/" ) ) return basePath + subPath.substring( 1 );
        }
        else if ( !subPath.startsWith( "/" ) ) return basePath + "/" + subPath;
        return basePath + subPath;
    }

    @Override
    public void serialize( MappingSerializer serializer )
    {
        serializer.putMapping( "resources", new MappingRepresentation( "resources" )
        {
            @Override
            protected void serialize( MappingSerializer resourceSerializer )
            {
                for ( Map.Entry<String, String> entry : uris.entrySet() )
                {
                    resourceSerializer.putUri( entry.getKey(), entry.getValue() );
                }

                for ( Map.Entry<String, String> entry : templates.entrySet() )
                {
                    resourceSerializer.putUriTemplate( entry.getKey(), entry.getValue() );
                }
            }
        } );
    }
}
