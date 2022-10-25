/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.kernel.impl.api.parallel;

import static org.neo4j.io.IOUtils.closeAllUnchecked;

import java.lang.invoke.VarHandle;
import java.util.ArrayList;
import java.util.List;
import org.neo4j.internal.kernel.api.IndexMonitor;
import org.neo4j.internal.kernel.api.Locks;
import org.neo4j.internal.kernel.api.Procedures;
import org.neo4j.internal.kernel.api.QueryContext;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.internal.kernel.api.security.SecurityAuthorizationHandler;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.api.ExecutionContext;
import org.neo4j.kernel.impl.api.OverridableSecurityContext;
import org.neo4j.kernel.impl.newapi.AllStoreHolder;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.cursor.StoreCursors;

public class ThreadExecutionContext implements ExecutionContext, AutoCloseable {
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
    private final List<AutoCloseable> otherResources;

    public ThreadExecutionContext(
            CursorContext context,
            OverridableSecurityContext overridableSecurityContext,
            ExecutionContextCursorTracer cursorTracer,
            CursorContext ktxContext,
            AllStoreHolder.ForThreadExecutionContextScope allStoreHolder,
            TokenRead tokenRead,
            StoreCursors storageCursors,
            IndexMonitor monitor,
            MemoryTracker contextTracker,
            SecurityAuthorizationHandler securityAuthorizationHandler,
            List<AutoCloseable> otherResources) {
        this.context = context;
        this.overridableSecurityContext = overridableSecurityContext;
        this.cursorTracer = cursorTracer;
        this.ktxContext = ktxContext;
        this.allStoreHolder = allStoreHolder;
        this.tokenRead = tokenRead;
        this.storageCursors = storageCursors;
        this.monitor = monitor;
        this.contextTracker = contextTracker;
        this.securityAuthorizationHandler = securityAuthorizationHandler;
        this.otherResources = otherResources;
    }

    @Override
    public CursorContext cursorContext() {
        return context;
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
    public Procedures procedures() {
        return allStoreHolder;
    }

    @Override
    public void complete() {
        List<AutoCloseable> resources = new ArrayList<>(otherResources);
        resources.add(storageCursors);
        closeAllUnchecked(resources);
        cursorTracer.complete();
    }

    @Override
    public void report() {
        mergeBlocked(cursorTracer);
    }

    @Override
    public StoreCursors storeCursors() {
        return storageCursors;
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
        while (!cursorTracer.isCompleted()) {
            Thread.onSpinWait();
        }
        mergeUnblocked(cursorTracer);
    }

    private void mergeBlocked(ExecutionContextCursorTracer cursorTracer) {
        synchronized (ktxContext) {
            mergeUnblocked(cursorTracer);
        }
        VarHandle.fullFence();
    }

    private void mergeUnblocked(ExecutionContextCursorTracer cursorTracer) {
        ktxContext.merge(cursorTracer.snapshot());
    }
}
