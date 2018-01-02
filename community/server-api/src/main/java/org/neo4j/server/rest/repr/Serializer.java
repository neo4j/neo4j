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
package org.neo4j.server.rest.repr;

import java.net.URI;
import java.util.List;
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
        injectExtensions( mapping, value, baseUri, extensions );
        value.serialize( new MappingSerializer( mapping, baseUri, extensions ) );
        mapping.done();
    }

    static void injectExtensions( MappingWriter mapping, MappingRepresentation value, URI baseUri,
            ExtensionInjector injector )
    {
        if ( value instanceof ExtensibleRepresentation && injector != null )
        {
            Map<String/*name*/, List<String/*method*/>> extData = injector.getExensionsFor( value.type.extend );
            String entityIdentity = ( (ExtensibleRepresentation) value ).getIdentity();
            if ( extData != null )
            {
                MappingWriter extensions = mapping.newMapping( RepresentationType.PLUGINS, "extensions" );
                for ( Map.Entry<String, List<String>> ext : extData.entrySet() )
                {
                    MappingWriter extension = extensions.newMapping( RepresentationType.PLUGIN, ext.getKey() );
                    for ( String method : ext.getValue() )
                    {
                        StringBuilder path = new StringBuilder( "/ext/" ).append( ext.getKey() );
                        path.append( "/" ).append( value.type.valueName );
                        if ( entityIdentity != null ) path.append( "/" ).append( entityIdentity );
                        path.append( "/" ).append( method );
                        extension.writeValue( RepresentationType.URI, method, joinBaseWithRelativePath( baseUri,
                                path.toString() ) );
                    }
                    extension.done();
                }
                extensions.done();
            }
        }
    }

    final void serialize( ListWriter list, ListRepresentation value )
    {
        value.serialize( new ListSerializer( list, baseUri, extensions ) );
        list.done();
    }

    final String relativeUri(String path)
    {
        return joinBaseWithRelativePath(baseUri, path);
    }

    final String relativeTemplate( String path )
    {
        return joinBaseWithRelativePath( baseUri, path );
    }

    static String joinBaseWithRelativePath( URI baseUri, String path )
    {
        String base = baseUri.toString();
        final StringBuilder result = new StringBuilder(base.length() + path.length() +1).append(base);
        if ( base.endsWith( "/" ) )
        {
            if ( path.startsWith( "/" ) )
            {
                return result.append(path.substring(1)).toString();
            }
        }
        else if ( !path.startsWith( "/" ) )
        {
            return result.append('/').append(path).toString();
        }
        return result.append(path).toString();
    }

    protected void checkThatItIsBuiltInType( Object value )
    {
        if ( !"java.lang".equals( value.getClass().getPackage().getName() ) )
        {
            throw new IllegalArgumentException( "Unsupported number type: " + value.getClass() );
        }
    }
}
