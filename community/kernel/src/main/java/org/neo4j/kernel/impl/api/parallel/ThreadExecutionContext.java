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

import java.lang.invoke.VarHandle;

import org.neo4j.configuration.Config;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.api.ExecutionContext;
import org.neo4j.kernel.impl.api.KernelTransactionImplementation;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.cursor.StoreCursors;

import static org.neo4j.io.IOUtils.closeAllUnchecked;

public class ThreadExecutionContext implements ExecutionContext, AutoCloseable
{
    private static final String TRANSACTION_EXECUTION_TAG = "transactionExecution";
    private final CursorContext context;
    private final AccessMode accessMode;
    private final ExecutionContextCursorTracer cursorTracer;
    private final CursorContext ktxContext;
    private final ThreadExecutionContextRead contextRead;
    private final StoreCursors storageCursors;

    public ThreadExecutionContext( KernelTransactionImplementation ktx, CursorContextFactory contextFactory, StorageEngine storageEngine, Config config )
    {
        this.cursorTracer = new ExecutionContextCursorTracer( PageCacheTracer.NULL, TRANSACTION_EXECUTION_TAG );
        this.ktxContext = ktx.cursorContext();
        this.context = contextFactory.create( cursorTracer );
        this.accessMode = ktx.securityContext().mode();
        this.storageCursors = storageEngine.createStorageCursors( context );
        this.contextRead = new ThreadExecutionContextRead( this, ktx.dataRead(), ktx.newStorageReader(), storageCursors, config );
    }

    @Override
    public CursorContext cursorContext()
    {
        return context;
    }

    @Override
    public AccessMode accessMode()
    {
        return accessMode;
    }

    @Override
    public Read dataRead()
    {
        return contextRead;
    }

    @Override
    public void complete()
    {
        closeAllUnchecked( contextRead, storageCursors );
        context.getCursorTracer().reportEvents();
    }

    @Override
    public void report()
    {
        mergeBlocked( cursorTracer );
    }

    @Override
    public StoreCursors storeCursors()
    {
        return storageCursors;
    }

    @Override
    public void close()
    {
        while ( !cursorTracer.isCompleted() )
        {
            Thread.onSpinWait();
        }
        mergeUnblocked( cursorTracer );
    }

    private void mergeBlocked( ExecutionContextCursorTracer cursorTracer )
    {
        synchronized ( ktxContext )
        {
            mergeUnblocked( cursorTracer );
        }
        VarHandle.fullFence();
    }

    private void mergeUnblocked( ExecutionContextCursorTracer cursorTracer )
    {
        ktxContext.merge( cursorTracer.snapshot() );
    }
}
