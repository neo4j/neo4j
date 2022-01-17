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

import org.junit.jupiter.api.Test;

import org.neo4j.configuration.Config;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.context.EmptyVersionContextSupplier;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.api.KernelTransactionImplementation;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.storageengine.api.cursor.StoreCursors;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ThreadExecutionContextTest
{
    @Test
    void closeResourcesOnContextClose()
    {
        var pageCacheTracer = PageCacheTracer.NULL;
        var contextFactory = new CursorContextFactory( pageCacheTracer, EmptyVersionContextSupplier.EMPTY );
        var cursorContext = contextFactory.create(  "tag" );
        var ktx = mock( KernelTransactionImplementation.class );
        var storageReader = mock( StorageReader.class );

        when( ktx.cursorContext() ).thenReturn( cursorContext );
        when( ktx.securityContext() ).thenReturn( SecurityContext.AUTH_DISABLED );
        when( ktx.newStorageReader() ).thenReturn( storageReader );

        var storageEngine = mock( StorageEngine.class );
        var storeCursors = mock( StoreCursors.class );
        when( storageEngine.createStorageCursors( any() ) ).thenReturn( storeCursors );

        try ( var executionContext = new ThreadExecutionContext( ktx, contextFactory, storageEngine, Config.defaults() ) )
        {
            executionContext.complete();
        }

        verify( storeCursors ).close();
        verify( storageReader ).close();
    }
}
