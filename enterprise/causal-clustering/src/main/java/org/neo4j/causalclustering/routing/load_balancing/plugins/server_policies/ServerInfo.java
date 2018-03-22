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
package org.neo4j.causalclustering.routing.load_balancing.plugins.server_policies;

import java.util.Objects;
import java.util.Set;

import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.helpers.AdvertisedSocketAddress;

/**
 * Hold the server information that is interesting for load balancing purposes.
 */
public class ServerInfo
{
    private final AdvertisedSocketAddress boltAddress;
    private MemberId memberId;
    private Set<String> groups;

    public ServerInfo( AdvertisedSocketAddress boltAddress, MemberId memberId, Set<String> groups )
    {
        this.boltAddress = boltAddress;
        this.memberId = memberId;
        this.groups = groups;
    }

    public MemberId memberId()
    {
        return memberId;
    }

    AdvertisedSocketAddress boltAddress()
    {
        return boltAddress;
    }

    Set<String> groups()
    {
        return groups;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        ServerInfo that = (ServerInfo) o;
        return Objects.equals( boltAddress, that.boltAddress ) && Objects.equals( memberId, that.memberId ) &&
                Objects.equals( groups, that.groups );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( boltAddress, memberId, groups );
    }

    @Override
    public String toString()
    {
        return "ServerInfo{" + "boltAddress=" + boltAddress + ", memberId=" + memberId + ", groups=" + groups + '}';
    }
}
