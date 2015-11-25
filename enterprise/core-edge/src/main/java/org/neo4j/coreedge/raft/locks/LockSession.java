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
package org.neo4j.coreedge.raft.locks;

import java.util.Objects;

import org.neo4j.coreedge.server.CoreMember;

import static java.lang.String.format;

public class LockSession
{
    public final CoreMember owner;
    public final int localSessionId;

    public LockSession( CoreMember owner, int localSessionId )
    {
        this.owner = owner;
        this.localSessionId = localSessionId;
    }

    @Override
    public String toString()
    {
        return format( "LockSession{owner=%s, localSessionId=%d}", owner, localSessionId );
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
        LockSession that = (LockSession) o;
        return Objects.equals( localSessionId, that.localSessionId ) &&
                Objects.equals( owner, that.owner );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( owner, localSessionId );
    }
}
