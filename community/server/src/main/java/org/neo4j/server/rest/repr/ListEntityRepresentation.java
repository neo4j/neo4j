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

public class ListEntityRepresentation extends Representation implements EntityRepresentation
{
    private ListRepresentation listRepresentation;
    private URI selfUfi;

    public ListEntityRepresentation( ListRepresentation listRepresentation, URI selfUfi )
    {
        super( "list" );
        this.listRepresentation = listRepresentation;
        this.selfUfi = selfUfi;
    }

    @Override
    public ValueRepresentation selfUri()
    {
        return ValueRepresentation.uri( selfUfi.getPath() );
    }

    @Override
    String serialize( RepresentationFormat format, URI baseUri, ExtensionInjector extensions )
    {
        return listRepresentation.serialize( format, baseUri, extensions );
    }

    @Override
    void addTo( ListSerializer serializer )
    {
        listRepresentation.addTo( serializer );
    }

    @Override
    void putTo( MappingSerializer serializer, String key )
    {
        listRepresentation.putTo( serializer, key );
    }
}
