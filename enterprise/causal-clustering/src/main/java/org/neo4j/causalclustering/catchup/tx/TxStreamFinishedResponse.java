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
package org.neo4j.causalclustering.catchup.tx;

import java.util.Objects;

import org.neo4j.causalclustering.catchup.CatchupResult;

import static java.lang.String.format;

public class TxStreamFinishedResponse
{
    private final CatchupResult status;
    private final long latestTxId;

    public CatchupResult status()
    {
        return status;
    }

    TxStreamFinishedResponse( CatchupResult status, long latestTxId )
    {
        this.status = status;
        this.latestTxId = latestTxId;
    }

    public long latestTxId()
    {
        return latestTxId;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        { return true; }
        if ( o == null || getClass() != o.getClass() )
        { return false; }
        TxStreamFinishedResponse that = (TxStreamFinishedResponse) o;
        return latestTxId == that.latestTxId &&
               status == that.status;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( status, latestTxId );
    }

    @Override
    public String toString()
    {
        return "TxStreamFinishedResponse{" +
               "status=" + status +
               ", latestTxId=" + latestTxId +
               '}';
    }
}
