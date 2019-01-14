/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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

import org.neo4j.kernel.impl.store.counts.CountsTracker;
import org.neo4j.storageengine.api.CommandsToApply;
import org.neo4j.storageengine.api.TransactionApplicationMode;

public class CountsStoreBatchTransactionApplier extends BatchTransactionApplier.Adapter
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
    public TransactionApplier startTx( CommandsToApply transaction )
    {
        Optional<CountsAccessor.Updater> result = countsTracker.apply( transaction.transactionId() );
        result.ifPresent( updater -> this.countsUpdater = updater );
        assert this.countsUpdater != null || mode == TransactionApplicationMode.RECOVERY;

        return new CountsStoreTransactionApplier( mode, countsUpdater );
    }
}
