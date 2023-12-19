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
