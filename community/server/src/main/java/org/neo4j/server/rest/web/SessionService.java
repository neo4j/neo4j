/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import org.neo4j.server.database.SessionDispenser;
import org.neo4j.server.rest.repr.BadInputException;
import org.neo4j.server.rest.repr.OutputFormat;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;

import static javax.ws.rs.core.Response.Status.CREATED;
import static org.neo4j.server.rest.repr.ValueRepresentation.number;

@Path("/session")
public class SessionService
{
    private SessionDispenser sessionDispenser;
    private OutputFormat output;

    public SessionService( @Context SessionDispenser sessionDispenser, @Context OutputFormat output )
    {
        this.sessionDispenser = sessionDispenser;
        this.output = output;
    }

    @POST
    @SuppressWarnings({ "unchecked" })
    public Response newSession() throws BadInputException
    {
        return output.response(CREATED, number(sessionDispenser.newSession()));
    }

}
