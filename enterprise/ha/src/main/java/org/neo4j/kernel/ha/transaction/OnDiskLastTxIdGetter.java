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

import org.neo4j.function.Supplier;
import org.neo4j.kernel.impl.core.LastTxIdGetter;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;

public class OnDiskLastTxIdGetter implements LastTxIdGetter
{
    private final Supplier<NeoStores> neoStoresSupplier;

    public OnDiskLastTxIdGetter( Supplier<NeoStores> neoStoresSupplier )
    {
        this.neoStoresSupplier = neoStoresSupplier;
    }

    /* This method is used to construct credentials for election process.
     And can be invoked at any moment of instance lifecycle.
     It mean that its possible that we will be invoked when neo stores are stopped
     (for example while we copy store) in that case we will return TransactionIdStore.BASE_TX_ID */
    @Override
    public long getLastTxId()
    {
        try
        {
            TransactionIdStore neoStore = getNeoStores().getMetaDataStore();
            return neoStore.getLastCommittedTransactionId();
        }
        catch ( Throwable e )
        {
            return TransactionIdStore.BASE_TX_ID;
        }
    }

    private NeoStores getNeoStores()
    {
        // Note that it is important that we resolve the NeoStores dependency anew every
        // time we want to read the last transaction id.
        // The reason is that a mode switch can stop and restart the database innards,
        // leaving us with a stale NeoStores, not connected to a working page cache,
        // if we cache it.
        // We avoid this problem by simply not caching it, and instead looking it up
        // every time.
        return neoStoresSupplier.get();
    }
}
