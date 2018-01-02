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

import java.util.Map;

import org.neo4j.server.rest.domain.URIHelper;

public abstract class IndexRepresentation extends MappingRepresentation implements EntityRepresentation
{
    private final String name;
    private final Map<String, String> type;

    public IndexRepresentation( String name, Map<String, String> type )
    {
        super( RepresentationType.INDEX );
        this.name = name;
        this.type = type;
    }

    @Override
    protected void serialize( final MappingSerializer serializer )
    {
        serializer.putUriTemplate( "template", path() + "{key}/{value}" );
        for ( Map.Entry<String, String> pair : type.entrySet() )
        {
            serializer.putString( pair.getKey(), pair.getValue() );
        }
    }

    public String relativeUriFor( String key, String value, long entityId )
    {
        return path() + URIHelper.encode( key ) + "/" + URIHelper.encode( value ) + "/" + Long.toString( entityId );
    }

    @Override
    public ValueRepresentation selfUri()
    {
        return ValueRepresentation.uri( path() );
    }

    protected String path()
    {
        return "index/" + propertyContainerType() + "/" + URIHelper.encode(name) + "/";
    }

    protected abstract String propertyContainerType();

}
