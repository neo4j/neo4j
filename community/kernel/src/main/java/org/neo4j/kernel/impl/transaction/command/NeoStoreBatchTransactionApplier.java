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
package org.neo4j.kernel.impl.transaction.command;

import java.io.IOException;

import org.neo4j.kernel.impl.api.BatchTransactionApplier;
import org.neo4j.kernel.impl.api.TransactionApplier;
import org.neo4j.kernel.impl.core.CacheAccessBackDoor;
import org.neo4j.kernel.impl.locking.LockGroup;
import org.neo4j.kernel.impl.locking.LockService;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.transaction.command.Command.Version;
import org.neo4j.storageengine.api.CommandsToApply;

/**
 * Visits commands targeted towards the {@link NeoStores} and update corresponding stores. What happens in here is what
 * will happen in a "internal" transaction, i.e. a transaction that has been forged in this database, with transaction
 * state, a KernelTransaction and all that and is now committing. <p> For other modes of application, like recovery or
 * external there are other, added functionality, decorated outside this applier.
 */
public class NeoStoreBatchTransactionApplier extends BatchTransactionApplier.Adapter
{
    private final Version version;
    private final NeoStores neoStores;
    // Ideally we don't want any cache access in here, but it is how it is. At least we try to minimize use of it
    private final CacheAccessBackDoor cacheAccess;
    private final LockService lockService;

    public NeoStoreBatchTransactionApplier( NeoStores store, CacheAccessBackDoor cacheAccess, LockService lockService )
    {
        this( Version.AFTER, store, cacheAccess, lockService );
    }

    public NeoStoreBatchTransactionApplier( Version version, NeoStores store, CacheAccessBackDoor cacheAccess, LockService lockService )
    {
        this.version = version;
        this.neoStores = store;
        this.cacheAccess = cacheAccess;
        this.lockService = lockService;
    }

    @Override
    public TransactionApplier startTx( CommandsToApply transaction )
    {
        throw new RuntimeException( "NeoStoreTransactionApplier requires a LockGroup" );
    }

    @Override
    public TransactionApplier startTx( CommandsToApply transaction, LockGroup lockGroup )
    {
        return new NeoStoreTransactionApplier( version, neoStores, cacheAccess, lockService, transaction.transactionId(), lockGroup );
    }
}
