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

import java.util.stream.Stream;
import javax.ws.rs.core.Response;

import org.neo4j.causalclustering.core.CoreGraphDatabase;
import org.neo4j.causalclustering.core.consensus.roles.Role;
import org.neo4j.server.rest.repr.OutputFormat;

import static org.neo4j.server.rest.causalclustering.CausalClusteringService.BASE_PATH;

class CoreStatus extends BaseStatus
{
    private final OutputFormat output;
    private final CoreGraphDatabase db;

    CoreStatus( OutputFormat output, CoreGraphDatabase db )
    {
        super( output );
        this.output = output;
        this.db = db;
    }

    @Override
    public Response discover()
    {
        return output.ok( new CausalClusteringDiscovery( BASE_PATH ) );
    }

    @Override
    public Response available()
    {
        return positiveResponse();
    }

    @Override
    public Response readonly()
    {
        Role role = db.getRole();
        return Stream.of( Role.FOLLOWER, Role.CANDIDATE ).anyMatch( r -> r.equals( role ) ) ? positiveResponse() : negativeResponse();
    }

    @Override
    public Response writable()
    {
        return db.getRole() == Role.LEADER ? positiveResponse() : negativeResponse();
    }
}
