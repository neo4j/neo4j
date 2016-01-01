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
package org.neo4j.com.storecopy;

import org.neo4j.function.Function;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.kernel.KernelHealth;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.impl.api.BatchingTransactionRepresentationStoreApplier;
import org.neo4j.kernel.impl.api.LegacyIndexApplierLookup;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.core.CacheAccessBackDoor;
import org.neo4j.kernel.impl.index.IndexConfigStore;
import org.neo4j.kernel.impl.locking.LockService;
import org.neo4j.kernel.impl.transaction.state.NeoStoreProvider;
import org.neo4j.kernel.impl.util.IdOrderingQueue;

class DefaultBatchingStoreApplierCreator implements
        Function<DependencyResolver,BatchingTransactionRepresentationStoreApplier>
{
    @Override
    public BatchingTransactionRepresentationStoreApplier apply( DependencyResolver resolver )
    {
        return new BatchingTransactionRepresentationStoreApplier(
                resolver.resolveDependency( IndexingService.class ),
                resolver.resolveDependency( LabelScanStore.class ),
                resolver.resolveDependency( NeoStoreProvider.class ).evaluate(),
                resolver.resolveDependency( CacheAccessBackDoor.class ),
                resolver.resolveDependency( LockService.class ),
                resolver.resolveDependency( LegacyIndexApplierLookup.class ),
                resolver.resolveDependency( IndexConfigStore.class ),
                resolver.resolveDependency( KernelHealth.class ),
                // Ideally we don't want/need a real IdOrderingQueue here because we know that
                // we only have a single thread applying updates as a slave anyway. But the thing
                // is that it's hard to change a TransactionAppender depending on role, so we
                // use a real one, or rather, whatever is available through the dependency resolver.
                resolver.resolveDependency( IdOrderingQueue.class ) );
    }
}
