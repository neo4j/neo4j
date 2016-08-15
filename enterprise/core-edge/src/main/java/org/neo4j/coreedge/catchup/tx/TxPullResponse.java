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

import java.util.Objects;

import org.neo4j.coreedge.identity.StoreId;
import org.neo4j.coreedge.messaging.BaseMessage;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;

public class TxPullResponse extends BaseMessage
{
    private final StoreId storeId;
    private final CommittedTransactionRepresentation tx;

    public TxPullResponse( byte version, StoreId storeId, CommittedTransactionRepresentation tx )
    {
        super( version );
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
        if ( !super.equals( o ) )
        {
            return false;
        }
        TxPullResponse that = (TxPullResponse) o;
        return Objects.equals( storeId, that.storeId ) && Objects.equals( tx, that.tx );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( super.hashCode(), storeId, tx );
    }

    @Override
    public String toString()
    {
        return String.format( "TxPullResponse{storeId=%s, tx=%s}", storeId, tx );
    }
}
