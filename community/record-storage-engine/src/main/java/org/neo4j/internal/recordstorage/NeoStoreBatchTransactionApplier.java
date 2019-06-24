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
package org.neo4j.internal.recordstorage;

import java.util.EnumMap;
import java.util.Map;

import org.neo4j.internal.id.IdGenerator;
import org.neo4j.internal.id.IdType;
import org.neo4j.kernel.impl.store.IdUpdateListener;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.lock.LockGroup;
import org.neo4j.lock.LockService;
import org.neo4j.storageengine.api.CommandVersion;
import org.neo4j.storageengine.api.CommandsToApply;
import org.neo4j.storageengine.api.TransactionApplicationMode;
import org.neo4j.util.concurrent.WorkSync;

/**
 * Visits commands targeted towards the {@link NeoStores} and update corresponding stores. What happens in here is what
 * will happen in a "internal" transaction, i.e. a transaction that has been forged in this database, with transaction
 * state, a KernelTransaction and all that and is now committing. <p> For other modes of application, like recovery or
 * external there are other, added functionality, decorated outside this applier.
 */
public class NeoStoreBatchTransactionApplier extends BatchTransactionApplier.Adapter
{
    private final CommandVersion version;
    private final NeoStores neoStores;
    // Ideally we don't want any cache access in here, but it is how it is. At least we try to minimize use of it
    private final CacheAccessBackDoor cacheAccess;
    private final LockService lockService;
    private final Map<IdType,WorkSync<IdGenerator,IdGeneratorUpdateWork>> idGeneratorWorkSyncs;
    private final IdUpdateListener idUpdateListener;
    private final EnumMap<IdType,ChangedIds> idUpdatesMap = new EnumMap<>( IdType.class );

    NeoStoreBatchTransactionApplier( TransactionApplicationMode mode, NeoStores store, CacheAccessBackDoor cacheAccess, LockService lockService,
            Map<IdType,WorkSync<IdGenerator,IdGeneratorUpdateWork>> idGeneratorWorkSyncs )
    {
        this.version = mode.version();
        this.neoStores = store;
        this.cacheAccess = cacheAccess;
        this.lockService = lockService;
        this.idGeneratorWorkSyncs = idGeneratorWorkSyncs;

        // There's no need to update the id generators when recovery is on its way back
        this.idUpdateListener = mode == TransactionApplicationMode.REVERSE_RECOVERY ? IdUpdateListener.IGNORE : new EnqueuingIdUpdateListener( idUpdatesMap );
    }

    @Override
    public TransactionApplier startTx( CommandsToApply transaction )
    {
        throw new RuntimeException( "NeoStoreTransactionApplier requires a LockGroup" );
    }

    @Override
    public TransactionApplier startTx( CommandsToApply transaction, LockGroup lockGroup )
    {
        return new NeoStoreTransactionApplier( version, neoStores, cacheAccess, lockService, transaction.transactionId(), lockGroup, idUpdateListener );
    }

    @Override
    public void close() throws Exception
    {
        try
        {
            // Run through the id changes and apply them, or rather apply them asynchronously.
            // This allows multiple concurrent threads applying batches of transactions to help each other out so that
            // there's a higher chance that changes to different id types can be applied in parallel.
            for ( Map.Entry<IdType,ChangedIds> idChanges : idUpdatesMap.entrySet() )
            {
                ChangedIds unit = idChanges.getValue();
                unit.applyAsync( idGeneratorWorkSyncs.get( idChanges.getKey() ) );
            }

            // Wait for all id updates to complete
            for ( Map.Entry<IdType,ChangedIds> idChanges : idUpdatesMap.entrySet() )
            {
                ChangedIds unit = idChanges.getValue();
                unit.awaitApply();
            }
        }
        finally
        {
            super.close();
        }
    }
}
