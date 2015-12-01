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
package org.neo4j.com.storecopy;

import org.neo4j.function.Supplier;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.kernel.DatabaseHealth;
import org.neo4j.kernel.impl.api.BatchingTransactionRepresentationStoreApplier;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.kernel.impl.api.index.IndexUpdatesValidator;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.index.OnlineIndexUpdatesValidator;
import org.neo4j.kernel.impl.index.IndexConfigStore;
import org.neo4j.kernel.impl.locking.LockService;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.storageengine.StorageEngine;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.transaction.log.LogFile;
import org.neo4j.kernel.impl.transaction.log.TransactionAppender;
import org.neo4j.kernel.impl.transaction.log.rotation.LogRotation;
import org.neo4j.kernel.impl.transaction.state.NeoStoresSupplier;
import org.neo4j.kernel.impl.transaction.state.PropertyLoader;
import org.neo4j.kernel.impl.util.IdOrderingQueue;

public class DefaultUnpackerDependencies implements TransactionCommittingResponseUnpacker.Dependencies
{
    private final DependencyResolver resolver;

    public DefaultUnpackerDependencies( DependencyResolver resolver )
    {
        this.resolver = resolver;
    }

    @Override
    public BatchingTransactionRepresentationStoreApplier transactionRepresentationStoreApplier()
    {
        return new BatchingTransactionRepresentationStoreApplier(
                resolver.resolveDependency( LockService.class ),
                resolver.resolveDependency( IndexConfigStore.class ),

                // Ideally we don't want/need a real IdOrderingQueue here because we know that
                // we only have a single thread applying updates as a slave anyway. But the thing
                // is that it's hard to change a TransactionAppender depending on role, so we
                // use a real one, or rather, whatever is available through the dependency resolver.
                resolver.resolveDependency( IdOrderingQueue.class ),
                resolver.resolveDependency( StorageEngine.class ) );
    }

    @Override
    public IndexUpdatesValidator indexUpdatesValidator()
    {
        NeoStores neoStore = resolver.resolveDependency( NeoStoresSupplier.class ).get();
        DatabaseHealth databaseHealth = resolver.resolveDependency( DatabaseHealth.class );
        return new OnlineIndexUpdatesValidator( neoStore, databaseHealth, new PropertyLoader( neoStore ),
                resolver.resolveDependency( IndexingService.class ), IndexUpdateMode.BATCHED );
    }

    @Override
    public LogFile logFile()
    {
        return resolver.resolveDependency( LogFile.class );
    }

    @Override
    public LogRotation logRotation()
    {
        return resolver.resolveDependency( LogRotation.class );
    }

    @Override
    public DatabaseHealth kernelHealth()
    {
        return resolver.resolveDependency( DatabaseHealth.class );
    }

    @Override
    public Supplier<TransactionObligationFulfiller> transactionObligationFulfiller()
    {
        return new Supplier<TransactionObligationFulfiller>()
        {
            @Override
            public TransactionObligationFulfiller get()
            {
                return resolver.resolveDependency( TransactionObligationFulfiller.class );
            }
        };
    }

    @Override
    public Supplier<TransactionAppender> transactionAppender()
    {
        return new Supplier<TransactionAppender>()
        {
            @Override
            public TransactionAppender get()
            {
                return resolver.resolveDependency( TransactionAppender.class );
            }
        };
    }

    @Override
    public LogService logService()
    {
        return resolver.resolveDependency( LogService.class );
    }
}
