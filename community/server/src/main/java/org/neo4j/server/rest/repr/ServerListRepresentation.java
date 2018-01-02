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

public class ServerListRepresentation extends ListRepresentation
{
    public ServerListRepresentation( RepresentationType type, Iterable<? extends Representation> content )
    {
        super( type, content );
    }

    protected void serialize( ListSerializer serializer )
    {
        for ( Object val : content )
        {
            if (val instanceof Number) serializer.addNumber( (Number) val );
            else if (val instanceof String) serializer.addString(  (String) val );
            else if (val instanceof Iterable) serializer.addList( ObjectToRepresentationConverter.getListRepresentation( (Iterable) val ) );
            else if (val instanceof Map) serializer.addMapping( ObjectToRepresentationConverter.getMapRepresentation( (Map) val ) );
            else if (val instanceof MappingRepresentation) serializer.addMapping( (MappingRepresentation) val  );
            else if (val instanceof Representation) ((Representation)val).addTo( serializer );
            //default
            else serializer.addString( val.toString() );
        }
    }
}
