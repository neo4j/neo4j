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

import static org.neo4j.io.IOUtils.closeAllUnchecked;

import java.lang.invoke.VarHandle;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import org.neo4j.collection.Dependencies;
import org.neo4j.internal.kernel.api.IndexMonitor;
import org.neo4j.internal.kernel.api.Locks;
import org.neo4j.internal.kernel.api.Procedures;
import org.neo4j.internal.kernel.api.QueryContext;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.SchemaRead;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.internal.kernel.api.security.SecurityAuthorizationHandler;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.internal.schema.SchemaState;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.api.ExecutionContext;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.procedure.ProcedureView;
import org.neo4j.kernel.impl.api.ClockContext;
import org.neo4j.kernel.impl.api.CloseableResourceManager;
import org.neo4j.kernel.impl.api.OverridableSecurityContext;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.index.stats.IndexStatisticsStore;
import org.neo4j.kernel.impl.locking.LockManager.Client;
import org.neo4j.kernel.impl.newapi.AllStoreHolder;
import org.neo4j.kernel.impl.newapi.DefaultPooledCursors;
import org.neo4j.lock.LockTracer;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.StorageLocks;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.values.ElementIdMapper;

public class ThreadExecutionContext implements ExecutionContext, AutoCloseable {
    private final CloseableResourceManager resourceManager = new CloseableResourceManager();
    private final DefaultPooledCursors cursors;
    private final CursorContext context;
    private final OverridableSecurityContext overridableSecurityContext;
    private final ExecutionContextCursorTracer cursorTracer;
    private final CursorContext ktxContext;
    private final AllStoreHolder.ForThreadExecutionContextScope allStoreHolder;
    private final TokenRead tokenRead;
    private final StoreCursors storageCursors;
    private final IndexMonitor monitor;
    private final MemoryTracker contextTracker;
    private final SecurityAuthorizationHandler securityAuthorizationHandler;
    private final ElementIdMapper elementIdMapper;
    private final List<AutoCloseable> otherResources;
    private final ExecutionContextProcedureKernelTransaction ktx;

    public ThreadExecutionContext(
            DefaultPooledCursors cursors,
            CursorContext context,
            OverridableSecurityContext overridableSecurityContext,
            ExecutionContextCursorTracer cursorTracer,
            CursorContext ktxContext,
            TokenRead tokenRead,
            StoreCursors storageCursors,
            IndexMonitor monitor,
            MemoryTracker contextTracker,
            SecurityAuthorizationHandler securityAuthorizationHandler,
            StorageReader storageReader,
            SchemaState schemaState,
            IndexingService indexingService,
            IndexStatisticsStore indexStatisticsStore,
            Dependencies databaseDependencies,
            StorageLocks storageLocks,
            Client lockClient,
            LockTracer lockTracer,
            ElementIdMapper elementIdMapper,
            KernelTransaction ktx,
            Supplier<ClockContext> clockContextSupplier,
            List<AutoCloseable> otherResources,
            ProcedureView procedureView,
            boolean multiVersioned) {
        this.cursors = cursors;
        this.context = context;
        this.overridableSecurityContext = overridableSecurityContext;
        this.cursorTracer = cursorTracer;
        this.ktxContext = ktxContext;
        this.tokenRead = tokenRead;
        this.storageCursors = storageCursors;
        this.monitor = monitor;
        this.contextTracker = contextTracker;
        this.securityAuthorizationHandler = securityAuthorizationHandler;
        this.otherResources = otherResources;
        this.elementIdMapper = elementIdMapper;
        this.ktx = new ExecutionContextProcedureKernelTransaction(ktx, this);
        this.allStoreHolder = new AllStoreHolder.ForThreadExecutionContextScope(
                this,
                storageReader,
                schemaState,
                indexingService,
                indexStatisticsStore,
                databaseDependencies,
                cursors,
                storageCursors,
                context,
                storageLocks,
                lockClient,
                lockTracer,
                overridableSecurityContext,
                this.ktx,
                securityAuthorizationHandler,
                clockContextSupplier,
                procedureView,
                multiVersioned);
    }

    @Override
    public CursorContext cursorContext() {
        return context;
    }

    @Override
    public DefaultPooledCursors cursors() {
        return cursors;
    }

    @Override
    public SecurityContext securityContext() {
        return overridableSecurityContext.currentSecurityContext();
    }

    @Override
    public Read dataRead() {
        return allStoreHolder;
    }

    @Override
    public TokenRead tokenRead() {
        return tokenRead;
    }

    @Override
    public SchemaRead schemaRead() {
        return allStoreHolder;
    }

    @Override
    public Procedures procedures() {
        return allStoreHolder;
    }

    @Override
    public void complete() {
        List<AutoCloseable> resources = new ArrayList<>(otherResources);
        resources.add(resourceManager::closeAllCloseableResources);
        resources.add(cursors::release);
        resources.add(storageCursors);
        closeAllUnchecked(resources);
        cursorTracer.complete();
    }

    @Override
    public void report() {
        mergeBlocked(cursorTracer, contextTracker);
    }

    @Override
    public ElementIdMapper elementIdMapper() {
        return elementIdMapper;
    }

    @Override
    public void performCheckBeforeOperation() {
        ktx.assertOpen();
    }

    @Override
    public boolean isTransactionOpen() {
        return ktx.isOpen();
    }

    @Override
    public QueryContext queryContext() {
        return allStoreHolder;
    }

    @Override
    public MemoryTracker memoryTracker() {
        return contextTracker;
    }

    @Override
    public Locks locks() {
        return allStoreHolder;
    }

    @Override
    public SecurityAuthorizationHandler securityAuthorizationHandler() {
        return securityAuthorizationHandler;
    }

    IndexMonitor monitor() {
        return monitor;
    }

    @Override
    public void close() {
        if (!cursorTracer.isCompleted()) {
            // this indicates incorrect usage
            throw new IllegalStateException("Execution context closed before it was marked as completed.");
        }
        mergeBlocked(cursorTracer, contextTracker);
    }

    private void mergeBlocked(ExecutionContextCursorTracer cursorTracer, MemoryTracker contextTracker) {
        synchronized (ktxContext) {
            mergeUnblocked(cursorTracer, contextTracker);
        }
        VarHandle.fullFence();
    }

    private void mergeUnblocked(ExecutionContextCursorTracer cursorTracer, MemoryTracker contextTracker) {
        ktxContext.merge(cursorTracer.snapshot());
        contextTracker.reset();
    }

    @Override
    public void registerCloseableResource(AutoCloseable closeableResource) {
        resourceManager.registerCloseableResource(closeableResource);
    }

    @Override
    public void unregisterCloseableResource(AutoCloseable closeableResource) {
        resourceManager.unregisterCloseableResource(closeableResource);
    }
}
