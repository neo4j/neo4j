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

abstract class ProgressiveState<Key> extends WritableState<Key>
{
    // state transitions

    ProgressiveState<Key> initialize( RotationStrategy rotation ) throws IOException
    {
        throw new IllegalStateException( "Cannot initialize in state: " + stateName() );
    }

    ActiveState<Key> start( DataInitializer<EntryUpdater<Key>> stateInitializer ) throws IOException
    {
        throw new IllegalStateException( "Cannot start in state: " + stateName() );
    }

    RotationState<Key> prepareRotation( long version )
    {
        throw new IllegalStateException( "Cannot rotate in state: " + stateName() );
    }

    ProgressiveState<Key> stop() throws IOException
    {
        throw new IllegalStateException( "Cannot stop in state: " + stateName() );
    }

    // methods of subtypes

    abstract String stateName();

    protected abstract File file();

    protected long storedVersion()
    {
        return version();
    }

    // default implementations

    @Override
    EntryUpdater<Key> resetter( Lock lock, Runnable closeAction )
    {
        throw new IllegalStateException( "Cannot reset in state: " + stateName() );
    }

    @Override
    void close() throws IOException
    {
        throw new IllegalStateException( "Cannot close() in state: " + stateName() );
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName();
    }
}
