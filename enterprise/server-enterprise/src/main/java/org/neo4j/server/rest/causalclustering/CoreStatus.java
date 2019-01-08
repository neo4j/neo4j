/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
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
