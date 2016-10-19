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
package org.neo4j.causalclustering.catchup.tx;

import java.util.Objects;

import org.neo4j.causalclustering.catchup.RequestMessageType;
import org.neo4j.causalclustering.identity.StoreId;
import org.neo4j.causalclustering.messaging.CatchUpRequest;

public class TxPullRequest implements CatchUpRequest
{
    private long previousTxId;
    private final StoreId expectedStoreId;

    public TxPullRequest( long previousTxId, StoreId expectedStoreId )
    {
        this.previousTxId = previousTxId;
        this.expectedStoreId = expectedStoreId;
    }

    /**
     * Request is for transactions after this id
     */
    public long previousTxId()
    {
        return previousTxId;
    }

    public StoreId expectedStoreId()
    {
        return expectedStoreId;
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
        return previousTxId == that.previousTxId && Objects.equals( expectedStoreId, that.expectedStoreId );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( previousTxId, expectedStoreId );
    }

    @Override
    public String toString()
    {
        return String.format( "TxPullRequest{txId=%d, storeId=%s}", previousTxId, expectedStoreId );
    }

    @Override
    public RequestMessageType messageType()
    {
        return RequestMessageType.TX_PULL_REQUEST;
    }
}
