/*
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.api;

import java.io.IOException;
import java.util.Optional;

import org.neo4j.kernel.impl.locking.LockGroup;
import org.neo4j.kernel.impl.store.counts.CountsTracker;

public class CountsStoreBatchTransactionApplier implements BatchTransactionApplier
{

    private final CountsTracker countsTracker;
    private CountsTracker.Updater countsUpdater;
    private final TransactionApplicationMode mode;

    public CountsStoreBatchTransactionApplier( CountsTracker countsTracker, TransactionApplicationMode mode )
    {
        this.countsTracker = countsTracker;
        this.mode = mode;
    }

    @Override
    public TransactionApplier startTx( TransactionToApply transaction ) throws IOException
    {
        Optional<CountsAccessor.Updater> result = countsTracker.apply( transaction.transactionId() );
        if ( result.isPresent() )
        {
            this.countsUpdater = result.get();
        }
        assert this.countsUpdater != null || mode == TransactionApplicationMode.RECOVERY;

        return new CountsStoreTransactionApplier( mode, countsUpdater );
    }

    @Override
    public TransactionApplier startTx( TransactionToApply transaction, LockGroup lockGroup ) throws IOException
    {
        return startTx( transaction );
    }

    @Override
    public void close() throws Exception
    {
        // Nothing to close, CountsUpdater is closed for each transaction
    }
}
