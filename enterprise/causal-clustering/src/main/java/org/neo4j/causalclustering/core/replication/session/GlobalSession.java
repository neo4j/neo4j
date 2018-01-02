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
package org.neo4j.causalclustering.core.replication.session;

import java.util.UUID;

import org.neo4j.causalclustering.identity.MemberId;

import static java.lang.String.format;

public class GlobalSession
{
    private final UUID sessionId;
    private final MemberId owner;

    public GlobalSession( UUID sessionId, MemberId owner )
    {
        this.sessionId = sessionId;
        this.owner = owner;
    }

    public UUID sessionId()
    {
        return sessionId;
    }

    public MemberId owner()
    {
        return owner;
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

        GlobalSession that = (GlobalSession) o;

        if ( !sessionId.equals( that.sessionId ) )
        {
            return false;
        }
        return owner.equals( that.owner );
    }

    @Override
    public int hashCode()
    {
        int result = sessionId.hashCode();
        result = 31 * result + owner.hashCode();
        return result;
    }

    @Override
    public String toString()
    {
        return format( "GlobalSession{sessionId=%s, owner=%s}", sessionId, owner );
    }
}
