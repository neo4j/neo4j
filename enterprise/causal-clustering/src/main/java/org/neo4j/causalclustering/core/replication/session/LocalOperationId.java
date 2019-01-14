/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.causalclustering.core.replication.session;

import static java.lang.String.format;

/** Uniquely identifies an operation as performed under a global session. */
public class LocalOperationId
{
    private final long localSessionId;
    private final long sequenceNumber;

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
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }

        LocalOperationId that = (LocalOperationId) o;

        if ( localSessionId != that.localSessionId )
        {
            return false;
        }
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
