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
package org.neo4j.coreedge.catchup.tx;

import org.neo4j.coreedge.identity.StoreId;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;

public class TxPullResponse
{
    private final StoreId storeId;
    private final CommittedTransactionRepresentation tx;

    public TxPullResponse( StoreId storeId, CommittedTransactionRepresentation tx )
    {
        this.storeId = storeId;
        this.tx = tx;
    }

    public StoreId storeId()
    {
        return storeId;
    }

    public CommittedTransactionRepresentation tx()
    {
        return tx;
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

        TxPullResponse that = (TxPullResponse) o;

        return (storeId != null ? storeId.equals( that.storeId ) : that.storeId == null) &&
                (tx != null ? tx.equals( that.tx ) : that.tx == null);
    }

    @Override
    public int hashCode()
    {
        int result = storeId != null ? storeId.hashCode() : 0;
        result = 31 * result + (tx != null ? tx.hashCode() : 0);
        return result;
    }

    @Override
    public String toString()
    {
        return String.format( "TxPullResponse{storeId=%s, tx=%s}", storeId, tx );
    }
}
