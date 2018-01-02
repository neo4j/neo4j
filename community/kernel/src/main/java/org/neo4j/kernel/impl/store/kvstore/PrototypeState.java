/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.impl.store.kvstore;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.locks.Lock;

import org.neo4j.kernel.impl.util.function.Optional;

import static org.neo4j.kernel.impl.util.function.Optionals.some;

public abstract class PrototypeState<Key> extends WritableState<Key>
{
    protected final ActiveState<Key> store;

    public PrototypeState( ActiveState<Key> store )
    {
        this.store = store;
    }

    protected abstract ActiveState<Key> create( ReadableState<Key> sub, File file );

    @Override
    protected final Headers headers()
    {
        return store.headers();
    }

    @Override
    protected final int storedEntryCount()
    {
        return store.storedEntryCount();
    }

    @Override
    protected final KeyFormat<Key> keyFormat()
    {
        return store.keyFormat();
    }

    @Override
    final EntryUpdater<Key> resetter( Lock lock, Runnable runnable )
    {
        throw new UnsupportedOperationException( "should never be invoked" );
    }

    @Override
    final void close() throws IOException
    {
        throw new UnsupportedOperationException( "should never be invoked" );
    }

    @Override
    final Optional<EntryUpdater<Key>> optionalUpdater( long version, Lock lock )
    {
        return some( updater( version, lock ) );
    }

    protected abstract EntryUpdater<Key> updater( long version, Lock lock );
}
