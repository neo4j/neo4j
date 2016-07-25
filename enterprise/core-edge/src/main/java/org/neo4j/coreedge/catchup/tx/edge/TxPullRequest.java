/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.coreedge.catchup.tx.edge;

import org.neo4j.coreedge.network.Message;

import org.neo4j.coreedge.catchup.RequestMessageType;

import static java.lang.String.format;

public class TxPullRequest implements Message
{
    public static final RequestMessageType MESSAGE_TYPE = RequestMessageType.TX_PULL_REQUEST;

    private long txId;

    public TxPullRequest( long txId )
    {
        this.txId = txId;
    }

    public long txId()
    {
        return txId;
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

        TxPullRequest that = (TxPullRequest) o;

        return txId == that.txId;
    }

    @Override
    public int hashCode()
    {
        int result = (int) (txId ^ (txId >>> 32));
        result = 31 * result;
        return result;
    }

    @Override
    public String toString()
    {
        return format( "TxPullRequest{txId=%d}", txId );
    }
}
