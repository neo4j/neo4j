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

package org.neo4j.server.rest.web;

import org.neo4j.server.rest.repr.OutputFormat;
import org.neo4j.server.rest.repr.Representation;

import javax.ws.rs.core.Response;

public class EntityOutputFormat extends OutputFormat
{
    private Representation representation;

    public EntityOutputFormat(  )
    {
        super( null, null, null );
    }

    @Override
    protected Response response( Response.ResponseBuilder response, Representation representation )
    {
        this.representation = representation;

        return response.build();
    }

    public Representation getRepresentation()
    {
        return representation;
    }
}
