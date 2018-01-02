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
package org.neo4j.test.server;

import static org.neo4j.server.rest.repr.RepresentationTestAccess.serialize;

import java.net.URI;
import java.util.List;
import java.util.Map;

import javax.ws.rs.core.Response;

import org.neo4j.server.rest.repr.ExtensionInjector;
import org.neo4j.server.rest.repr.OutputFormat;
import org.neo4j.server.rest.repr.Representation;
import org.neo4j.server.rest.repr.RepresentationFormat;

public class EntityOutputFormat extends OutputFormat
{
    private Representation representation;

    public EntityOutputFormat( RepresentationFormat format, URI baseUri, ExtensionInjector extensions )
    {
        super( format, baseUri, extensions );
    }

    @Override
    protected Response response( Response.ResponseBuilder response, Representation representation )
    {
        this.representation = representation;

        return super.response( response, representation );
    }

    @SuppressWarnings( "unchecked" )
    public Map<String, Object> getResultAsMap()
    {
        return (Map<String, Object>) serialize( representation );
    }

    public Representation getRepresentation()
    {
        return representation;
    }

    @SuppressWarnings( "unchecked" )
    public List<Object> getResultAsList()
    {
        return (List<Object>) serialize( representation );
    }
}
