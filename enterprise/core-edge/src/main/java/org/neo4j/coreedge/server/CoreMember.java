/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.coreedge.server;

import java.util.Objects;

import static java.lang.String.format;

public class CoreMember
{
    private final AdvertisedSocketAddress coreAddress;
    private final AdvertisedSocketAddress raftAddress;

    public CoreMember( AdvertisedSocketAddress coreAddress, AdvertisedSocketAddress raftAddress )
    {
        this.coreAddress = coreAddress;
        this.raftAddress = raftAddress;
    }

    @Override public String toString()
    {
        return format( "CoreMember{coreAddress=%s, raftAddress=%s}", coreAddress, raftAddress );
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
        CoreMember that = (CoreMember) o;
        return Objects.equals( coreAddress, that.coreAddress ) &&
                Objects.equals( raftAddress, that.raftAddress );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( coreAddress, raftAddress );
    }

    public AdvertisedSocketAddress getCoreAddress()
    {
        return coreAddress;
    }

    public AdvertisedSocketAddress getRaftAddress()
    {
        return raftAddress;
    }
}

