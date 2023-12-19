/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
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
