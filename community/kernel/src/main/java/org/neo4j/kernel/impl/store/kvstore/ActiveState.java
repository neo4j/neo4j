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

public abstract class ActiveState<Key> extends ProgressiveState<Key>
{
    public interface Factory
    {
        <Key> ActiveState<Key> open( ReadableState<Key> store, File file );
    }

    protected final ReadableState<Key> store;

    public ActiveState( ReadableState<Key> store )
    {
        this.store = store;
    }

    @Override
    protected final KeyFormat<Key> keyFormat()
    {
        return store.keyFormat();
    }

    @Override
    final String stateName()
    {
        return "active";
    }

    @Override
    protected abstract long storedVersion();

    @Override
    final RotationState.Rotation<Key> prepareRotation( long version )
    {
        version = Math.max( version, version() );
        return new RotationState.Rotation<>( this, prototype( version ), version );
    }

    @Override
    final Optional<EntryUpdater<Key>> optionalUpdater( long version, Lock lock )
    {
        return some( updater( version, lock ) );
    }

    protected abstract EntryUpdater<Key> updater( long version, Lock lock );

    @Override
    final EntryUpdater<Key> resetter( Lock lock, Runnable closeAction )
    {
        if ( hasChanges() )
        {
            throw new IllegalStateException( "Cannot reset while there are changes." );
        }
        return resettingUpdater( lock, closeAction );
    }

    @Override
    final ProgressiveState<Key> stop() throws IOException
    {
        if ( hasChanges() )
        {
            throw new IllegalStateException( "Cannot stop while there are changes." );
        }
        close();
        return new DeadState.Stopped<>( keyFormat(), factory() );
    }

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

    protected abstract EntryUpdater<Key> resettingUpdater( Lock lock, Runnable closeAction );

    protected abstract boolean hasChanges();

    protected abstract PrototypeState<Key> prototype( long version );

    @Override
    protected abstract void close() throws IOException;

    protected abstract Factory factory();

    protected abstract long applied();
}
