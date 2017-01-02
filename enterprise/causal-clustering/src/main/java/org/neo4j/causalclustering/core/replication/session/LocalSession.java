/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import static java.lang.String.format;

/** Holds the state for a local session. */
public class LocalSession
{
    private long localSessionId;
    private long currentSequenceNumber;

    public LocalSession( long localSessionId )
    {
        this.localSessionId = localSessionId;
    }

    /** Consumes and returns an operation id under this session. */
    protected LocalOperationId nextOperationId()
    {
        return new LocalOperationId( localSessionId, currentSequenceNumber++ );
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        { return true; }
        if ( o == null || getClass() != o.getClass() )
        { return false; }

        LocalSession that = (LocalSession) o;

        return localSessionId == that.localSessionId;

    }

    @Override
    public int hashCode()
    {
        return (int) (localSessionId ^ (localSessionId >>> 32));
    }

    @Override
    public String toString()
    {
        return format( "LocalSession{localSessionId=%d, sequenceNumber=%d}", localSessionId, currentSequenceNumber );
    }
}
