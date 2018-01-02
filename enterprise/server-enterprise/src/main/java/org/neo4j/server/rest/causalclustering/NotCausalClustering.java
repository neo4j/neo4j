/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.server.rest.causalclustering;

import javax.ws.rs.core.Response;

import org.neo4j.server.rest.repr.OutputFormat;

import static javax.ws.rs.core.Response.Status.FORBIDDEN;
import static javax.ws.rs.core.Response.status;

class NotCausalClustering extends BaseStatus
{
    NotCausalClustering( OutputFormat output )
    {
        super( output );
    }

    @Override
    public Response discover()
    {
        return status( FORBIDDEN ).build();
    }

    @Override
    public Response available()
    {
        return status( FORBIDDEN ).build();
    }

    @Override
    public Response readonly()
    {
        return status( FORBIDDEN ).build();
    }

    @Override
    public Response writable()
    {
        return status( FORBIDDEN ).build();
    }
}
