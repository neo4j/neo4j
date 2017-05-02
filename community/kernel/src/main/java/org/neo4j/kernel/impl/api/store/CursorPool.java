/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.kernel.impl.api.store;

import java.util.function.Consumer;
import java.util.function.Supplier;

import org.neo4j.collection.pool.LinkedQueuePool;
import org.neo4j.collection.pool.MarshlandPool;
import org.neo4j.function.Disposable;

public class CursorPool<T extends Disposable> implements AutoCloseable, Supplier<T>
{
    public interface CursorFactory<C extends Disposable>
    {
        C create( Consumer<C> consumer );
    }

    // Global pool of cursors, wrapped by the thread-local marshland pool and so is not used directly.
    private final LinkedQueuePool<T> globalPool;
    // Pool of unused cursors to be used to acquire cursors.
    private final MarshlandPool<T> localPool;

    CursorPool( int poolMinSize, CursorFactory<T> cursorFactory )
    {
        this.globalPool = new LinkedQueuePool<T>( poolMinSize )
        {
            @Override
            protected T create()
            {
                return cursorFactory.create( localPool::release );
            }

            @Override
            protected void dispose( T resource )
            {
                super.dispose( resource );
                resource.dispose();
            }
        };
        this.localPool = new MarshlandPool<>( globalPool );
    }

    public T get()
    {
        return localPool.acquire();
    }

    @Override
    public void close()
    {
        localPool.disposeAll();
        globalPool.disposeAll();
    }
}
