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
package org.neo4j.coreedge.raft.replication.session;

import static java.lang.String.format;

/** Uniquely identifies an operation as performed under a global session. */
public class LocalOperationId
{
    final private long localSessionId;
    final private long sequenceNumber;

    public LocalOperationId( long localSessionId, long sequenceNumber )
    {
        this.localSessionId = localSessionId;
        this.sequenceNumber = sequenceNumber;
    }

    public long localSessionId()
    {
        return localSessionId;
    }

    public long sequenceNumber()
    {
        return sequenceNumber;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        { return true; }
        if ( o == null || getClass() != o.getClass() )
        { return false; }

        LocalOperationId that = (LocalOperationId) o;

        if ( localSessionId != that.localSessionId )
        { return false; }
        return sequenceNumber == that.sequenceNumber;

    }

    @Override
    public int hashCode()
    {
        int result = (int) (localSessionId ^ (localSessionId >>> 32));
        result = 31 * result + (int) (sequenceNumber ^ (sequenceNumber >>> 32));
        return result;
    }

    @Override
    public String toString()
    {
        return format( "LocalOperationId{localSessionId=%d, sequenceNumber=%d}", localSessionId, sequenceNumber );
    }
}
