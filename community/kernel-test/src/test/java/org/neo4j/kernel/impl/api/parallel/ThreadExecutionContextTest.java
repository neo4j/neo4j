/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.api.parallel;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.neo4j.collection.Dependencies.dependenciesOf;
import static org.neo4j.io.pagecache.context.FixedVersionContextSupplier.EMPTY_CONTEXT_SUPPLIER;

import java.util.List;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.Config;
import org.neo4j.internal.kernel.api.IndexMonitor;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.internal.kernel.api.security.SecurityAuthorizationHandler;
import org.neo4j.internal.schema.SchemaState;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.KernelVersionProvider;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.procedure.ProcedureView;
import org.neo4j.kernel.impl.api.KernelTransactionResourceFactory;
import org.neo4j.kernel.impl.api.OverridableSecurityContext;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.index.stats.IndexStatisticsStore;
import org.neo4j.kernel.impl.locking.LockManager;
import org.neo4j.kernel.impl.newapi.DefaultPooledCursors;
import org.neo4j.kernel.impl.security.URIAccessRules;
import org.neo4j.lock.LockTracer;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.values.ElementIdMapper;

class ThreadExecutionContextTest {
    @Test
    void closeResourcesOnContextClose() {
        var pageCacheTracer = PageCacheTracer.NULL;
        var contextFactory = new CursorContextFactory(pageCacheTracer, EMPTY_CONTEXT_SUPPLIER);
        var storageReader = mock(StorageReader.class);
        var lockClient = mock(LockManager.Client.class);

        var storeCursors = mock(StoreCursors.class);
        var engine = mock(StorageEngine.class);
        doReturn(storeCursors).when(engine).createStorageCursors(any());
        var resourceFactory = mock(KernelTransactionResourceFactory.class);
        doReturn(mock(DefaultPooledCursors.class))
                .when(resourceFactory)
                .createCursors(any(), any(), any(), any(), anyBoolean(), anyBoolean());

        try (var executionContext = new ThreadExecutionContext(
                engine,
                contextFactory.create("tag"),
                mock(OverridableSecurityContext.class),
                new ExecutionContextCursorTracer(mock(PageCacheTracer.class), "test"),
                contextFactory.create("tx-tag"),
                mock(TokenRead.class),
                mock(IndexMonitor.class),
                mock(MemoryTracker.class),
                mock(SecurityAuthorizationHandler.class),
                mock(StorageReader.class),
                mock(SchemaState.class),
                mock(IndexingService.class),
                mock(IndexStatisticsStore.class),
                dependenciesOf(mock(URIAccessRules.class)),
                mock(LockManager.Client.class),
                mock(LockTracer.class),
                mock(ElementIdMapper.class),
                mock(KernelTransaction.class),
                mock(KernelVersionProvider.class),
                mock(Supplier.class),
                List.of(storageReader, lockClient),
                mock(ProcedureView.class),
                false,
                NullLogProvider.getInstance(),
                resourceFactory,
                mock(Config.class))) {
            executionContext.complete();
        }

        verify(storeCursors).close();
        verify(storageReader).close();
        verify(lockClient).close();
    }
}
