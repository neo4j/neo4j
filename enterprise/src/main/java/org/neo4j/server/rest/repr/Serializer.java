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

package org.neo4j.server.rest.repr;

import java.net.URI;
import java.util.Map;

abstract class Serializer
{
    private final URI baseUri;
    private final ExtensionInjector extensions;

    Serializer( URI baseUri, ExtensionInjector extensions )
    {
        this.baseUri = baseUri;
        this.extensions = extensions;
    }

    final void serialize( MappingWriter mapping, MappingRepresentation value )
    {
        if ( value instanceof ExtensibleRepresentation && extensions != null )
        {
            Map<String/*name*/, ExtensionUri> serviceData = extensions.getExensionsFor( value.type.extend );
            if ( serviceData != null )
            {
                MappingWriter services = mapping.newMapping( RepresentationType.SERVICES,
                        "services" );
                for ( Map.Entry<String, ExtensionUri> service : serviceData.entrySet() )
                {
                    service.getValue().serialize(
                            services.newMapping( RepresentationType.SERVICE, service.getKey() ),
                            baseUri, ( (ExtensibleRepresentation) value ).getIdentity() );
                }
            }
        }
        value.serialize( new MappingSerializer( mapping, baseUri, extensions ) );
        mapping.done();
    }

    final void serialize( ListWriter list, ListRepresentation value )
    {
        value.serialize( new ListSerializer( list, baseUri, extensions ) );
        list.done();
    }

    final URI relativeUri( String path )
    {
        return URI.create( relative( baseUri, path ) );
    }

    final String relativeTemplate( String path )
    {
        return relative( baseUri, path );
    }

    static String relative( URI baseUri, String path )
    {
        String base = baseUri.toString();
        if ( base.endsWith( "/" ) )
        {
            if ( path.startsWith( "/" ) )
            {
                return base + path.substring( 1 );
            }
        }
        else if ( !path.startsWith( "/" ) )
        {
            return base + "/" + path;
        }
        return base + path;
    }
}
