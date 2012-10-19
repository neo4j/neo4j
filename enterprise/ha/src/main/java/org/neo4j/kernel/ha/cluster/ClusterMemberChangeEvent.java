/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.kernel.ha.cluster;

import java.net.URI;

/**
 * This event represents a change in the cluster members internal state. The possible states
 * are enumerated in ClusterMemberState.
 */
public class ClusterMemberChangeEvent
{
    private final ClusterMemberState oldState;
    private final ClusterMemberState newState;
    private final URI serverClusterUri;
    private final URI serverHaUri;

    public ClusterMemberChangeEvent( ClusterMemberState oldState, ClusterMemberState newState,
                                     URI serverClusterUri, URI serverHaUri )
    {
        this.oldState = oldState;
        this.newState = newState;
        this.serverClusterUri = serverClusterUri;
        this.serverHaUri = serverHaUri;
    }

    public ClusterMemberState getOldState()
    {
        return oldState;
    }

    public ClusterMemberState getNewState()
    {
        return newState;
    }

    public URI getServerClusterUri()
    {
        return serverClusterUri;
    }

    public URI getServerHaUri()
    {
        return serverHaUri;
    }
}
