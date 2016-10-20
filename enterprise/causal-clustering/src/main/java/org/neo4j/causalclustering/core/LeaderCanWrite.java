/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.causalclustering.core;

import org.neo4j.causalclustering.core.consensus.RaftMachine;
import org.neo4j.causalclustering.core.consensus.roles.Role;
import org.neo4j.graphdb.security.WriteOperationsNotAllowedException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.factory.AccessCapability;

import static java.lang.String.format;

public class LeaderCanWrite implements AccessCapability
{
    private RaftMachine raftMachine;
    public static final String NOT_LEADER_ERROR_MSG =
            "No write operations are allowed directly on this database. Writes must pass through the leader. " +
                    "The role of this server is: %s";

    LeaderCanWrite( RaftMachine raftMachine )
    {
        this.raftMachine = raftMachine;
    }

    @Override
    public void assertCanWrite()
    {
        Role currentRole = raftMachine.currentRole();
        if ( !currentRole.equals( Role.LEADER ) )
        {
            throw new WriteOperationsNotAllowedException( format( NOT_LEADER_ERROR_MSG, currentRole ),
                    Status.Cluster.NotALeader );
        }
    }
}
