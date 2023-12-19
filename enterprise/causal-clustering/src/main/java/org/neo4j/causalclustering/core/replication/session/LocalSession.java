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
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }

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
