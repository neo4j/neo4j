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
package org.neo4j.kernel.ha.transaction;

import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.impl.core.LastTxIdGetter;
import org.neo4j.kernel.impl.store.NeoStore;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.state.NeoStoreProvider;

public class OnDiskLastTxIdGetter implements LastTxIdGetter
{
    private final GraphDatabaseAPI graphdb;

    public OnDiskLastTxIdGetter( GraphDatabaseAPI graphdb )
    {
        this.graphdb = graphdb;
    }

    @Override
    public long getLastTxId()
    {
        TransactionIdStore neoStore = getNeoStore();
        return neoStore.getLastCommittedTransactionId();
    }

    private NeoStore getNeoStore()
    {
        // Note that it is important that we resolve the NeoStore dependency anew every
        // time we want to read the last transaction id.
        // The reason is that a mode switch can stop and restart the database innards,
        // leaving us with a stale NeoStore, not connected to a working page cache,
        // if we cache it.
        // We avoid this problem by simply not caching it, and instead looking it up
        // every time.
        NeoStoreProvider neoStoreProvider =
                graphdb.getDependencyResolver().resolveDependency( NeoStoreProvider.class );
        return neoStoreProvider.evaluate();
    }
}
