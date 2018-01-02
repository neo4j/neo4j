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
package org.neo4j.kernel.ha.transaction;

import java.util.function.LongSupplier;
import org.neo4j.kernel.impl.core.LastTxIdGetter;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;

public class OnDiskLastTxIdGetter implements LastTxIdGetter
{
    private final LongSupplier txIdSupplier;

    public OnDiskLastTxIdGetter( LongSupplier txIdSupplier )
    {
        this.txIdSupplier = txIdSupplier;
    }

    /** This method is used to construct credentials for election process.
      * And can be invoked at any moment of instance lifecycle.
      * It mean that its possible that we will be invoked when neo stores are stopped
      * (for example while we copy store) in that case we will return TransactionIdStore.BASE_TX_ID
      */
    @Override
    public long getLastTxId()
    {
        try
        {
            return txIdSupplier.getAsLong();
        }
        catch ( Throwable e )
        {
            return TransactionIdStore.BASE_TX_ID;
        }
    }
}
