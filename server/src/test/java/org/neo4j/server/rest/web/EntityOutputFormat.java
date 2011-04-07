/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.server.rest.web;

import org.neo4j.server.rest.repr.*;

import javax.ws.rs.core.Response;
import java.net.URI;
import java.util.List;
import java.util.Map;

public class EntityOutputFormat extends OutputFormat
{
    private Representation representation;

    public EntityOutputFormat(  RepresentationFormat format, URI baseUri, ExtensionInjector extensions  )
    {
        super( format, baseUri, extensions);
    }

    @Override
    protected Response response( Response.ResponseBuilder response, Representation representation )
    {
        this.representation = representation;

        return super.response(response, representation);
    }

    public Map<String,Object> getResultAsMap() throws BadInputException
    {
        return (Map<String, Object>)RepresentationTestBase.serialize( representation );
    }

    public Representation getRepresentation()
    {
        return representation;
    }

    public List<Object> getResultAsList() throws BadInputException
    {
        return (List<Object>)RepresentationTestBase.serialize( representation );
    }
}
