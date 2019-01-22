/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.kernel.impl.store.kvstore;

import java.io.File;
import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Function;

import org.neo4j.helpers.collection.Pair;
import org.neo4j.io.pagecache.tracing.cursor.context.VersionContextSupplier;

abstract class DeadState<Key> extends ProgressiveState<Key>
{
    @Override
    protected Headers headers()
    {
        throw new IllegalStateException( "Cannot read in state: " + stateName() );
    }

    @Override
    protected boolean lookup( Key key, ValueSink sink )
    {
        throw new IllegalStateException( "Cannot read in state: " + stateName() );
    }

    @Override
    protected DataProvider dataProvider()
    {
        throw new IllegalStateException( "Cannot read in state: " + stateName() );
    }

    @Override
    protected int storedEntryCount()
    {
        throw new IllegalStateException( "Cannot read in state: " + stateName() );
    }

    @Override
    protected Optional<EntryUpdater<Key>> optionalUpdater( long version, Lock lock )
    {
        throw new IllegalStateException( "Cannot write in state: " + stateName() );
    }

    @Override
    protected EntryUpdater<Key> unsafeUpdater( Lock lock )
    {
        throw new IllegalStateException( "Cannot write in state: " + stateName() );
    }

    @Override
    protected boolean hasChanges()
    {
        return false;
    }

    @Override
    public void close()
    {
        throw new IllegalStateException( "Cannot close() in state: " + stateName() );
    }

    @Override
    protected File file()
    {
        throw new IllegalStateException( "No file assigned in state: " + stateName() );
    }

    @Override
    protected long version()
    {
        return keys.version( null );
    }

    @Override
    protected KeyFormat<Key> keyFormat()
    {
        return keys;
    }

    private final KeyFormat<Key> keys;
    final ActiveState.Factory stateFactory;
    final VersionContextSupplier versionContextSupplier;

    private DeadState( KeyFormat<Key> keys, ActiveState.Factory stateFactory, VersionContextSupplier versionContextSupplier )
    {
        this.keys = keys;
        this.stateFactory = stateFactory;
        this.versionContextSupplier = versionContextSupplier;
    }

    static class Stopped<Key> extends DeadState<Key>
    {
        Stopped( KeyFormat<Key> keys, ActiveState.Factory stateFactory, VersionContextSupplier versionContextSupplier )
        {
            super( keys, stateFactory, versionContextSupplier );
        }

        @Override
        String stateName()
        {
            return "stopped";
        }

        @Override
        ProgressiveState<Key> initialize( RotationStrategy rotation ) throws IOException
        {
            Pair<File, KeyValueStoreFile> opened = rotation.open();
            if ( opened == null )
            {
                return new NeedsCreation<>( keyFormat(), stateFactory, rotation, versionContextSupplier );
            }
            return new Prepared<>( stateFactory.open( ReadableState.store( keyFormat(), opened.other() ),
                                                      opened.first(), versionContextSupplier ) );
        }

        @Override
        ProgressiveState<Key> stop()
        {
            return this;
        }
    }

    private static class NeedsCreation<Key> extends DeadState<Key>
            implements Function<ActiveState<Key>, NeedsCreation<Key>>
    {
        private final RotationStrategy rotation;

        private NeedsCreation( KeyFormat<Key> keys, ActiveState.Factory stateFactory, RotationStrategy rotation,
                VersionContextSupplier versionContextSupplier )
        {
            super( keys, stateFactory, versionContextSupplier );
            this.rotation = rotation;
        }

        @Override
        ProgressiveState<Key> stop()
        {
            return new Stopped<>( keyFormat(), stateFactory, versionContextSupplier );
        }

        @Override
        String stateName()
        {
            return "needs creation";
        }

        @Override
        ActiveState<Key> start( DataInitializer<EntryUpdater<Key>> initializer ) throws IOException
        {
            if ( initializer == null )
            {
                throw new IllegalStateException( "Store needs to be created, and no initializer is given." );
            }
            Pair<File, KeyValueStoreFile> created = initialState( initializer );
            return stateFactory.open( ReadableState.store( keyFormat(), created.other() ), created.first(),
                    versionContextSupplier );
        }

        private Pair<File, KeyValueStoreFile> initialState( DataInitializer<EntryUpdater<Key>> initializer )
                throws IOException
        {
            long version = initializer.initialVersion();
            try ( ActiveState<Key> creation = stateFactory.open( ReadableState.empty( keyFormat(), version ), null,
                    versionContextSupplier ) )
            {
                try ( EntryUpdater<Key> updater = creation.resetter( new ReentrantLock(), () -> {} ) )
                {
                    initializer.initialize( updater );
                }
                return rotation.create( keyFormat().filter( creation.dataProvider() ), initializer.initialVersion() );
            }
        }

        /** called during recovery */
        @Override
        protected Optional<EntryUpdater<Key>> optionalUpdater( long version, Lock lock )
        {
            return Optional.empty();
        }

        /** for rotating recovered state (none) */
        @Override
        RotationState<Key> prepareRotation( long version )
        {
            return new Rotation<Key, NeedsCreation<Key>>( this )
            {
                @Override
                ProgressiveState<Key> rotate( boolean force, RotationStrategy strategy, RotationTimerFactory timerFactory,
                                              Consumer<Headers.Builder> headers )
                {
                    return state;
                }

                @Override
                public void close()
                {
                }

                @Override
                long rotationVersion()
                {
                    return state.version();
                }

                @Override
                ProgressiveState<Key> markAsFailed()
                {
                    return this;
                }
            };
        }

        @Override
        public NeedsCreation<Key> apply( ActiveState<Key> keyActiveState ) throws RuntimeException
        {
            return this;
        }
    }

    private static class Prepared<Key> extends DeadState<Key>
    {
        private final ActiveState<Key> state;

        private Prepared( ActiveState<Key> state )
        {
            super( state.keyFormat(), state.factory(), state.versionContextSupplier );
            this.state = state;
        }

        @Override
        protected Headers headers()
        {
            return state.headers();
        }

        /** for applying recovered transactions */
        @Override
        protected Optional<EntryUpdater<Key>> optionalUpdater( long version, Lock lock )
        {
            if ( version <= state.version() )
            {
                return Optional.empty();
            }
            else
            {
                return Optional.of( state.updater( version, lock ) );
            }
        }

        /** for rotating recovered state */
        @Override
        RotationState<Key> prepareRotation( long version )
        {
            return new Rotation<Key, RotationState.Rotation<Key>>( state.prepareRotation( version ) )
            {
                @Override
                ProgressiveState<Key> rotate( boolean force, RotationStrategy strategy, RotationTimerFactory timerFactory,
                                              Consumer<Headers.Builder> headers ) throws IOException
                {
                    return new Prepared<>( state.rotate( force, strategy, timerFactory, headers ) );
                }

                @Override
                public void close() throws IOException
                {
                    state.close();
                }

                @Override
                long rotationVersion()
                {
                    return state.rotationVersion();
                }

                @Override
                ProgressiveState<Key> markAsFailed()
                {
                    return state;
                }
            };
        }

        @Override
        ProgressiveState<Key> stop() throws IOException
        {
            return state.stop();
        }

        @Override
        String stateName()
        {
            return "prepared";
        }

        @Override
        ActiveState<Key> start( DataInitializer<EntryUpdater<Key>> stateInitializer )
        {
            return state;
        }

        @Override
        protected File file()
        {
            return state.file();
        }
    }

    private abstract static class Rotation<Key, State extends ProgressiveState<Key>> extends RotationState<Key>
    {
        final State state;

        Rotation( State state )
        {
            this.state = state;
        }

        @Override
        protected File file()
        {
            return state.file();
        }

        @Override
        Optional<EntryUpdater<Key>> optionalUpdater( long version, Lock lock )
        {
            throw new IllegalStateException( "Cannot write in state: " + stateName() );
        }

        @Override
        protected EntryUpdater<Key> unsafeUpdater( Lock lock )
        {
            throw new IllegalStateException( "Cannot write in state: " + stateName() );
        }

        @Override
        protected boolean hasChanges()
        {
            return state.hasChanges();
        }

        @Override
        protected KeyFormat<Key> keyFormat()
        {
            return state.keyFormat();
        }

        @Override
        protected Headers headers()
        {
            return state.headers();
        }

        @Override
        protected long version()
        {
            return state.version();
        }

        @Override
        protected boolean lookup( Key key, ValueSink sink )
        {
            throw new IllegalStateException( "Cannot read in state: " + stateName() );
        }

        @Override
        protected DataProvider dataProvider()
        {
            throw new IllegalStateException( "Cannot read in state: " + stateName() );
        }

        @Override
        protected int storedEntryCount()
        {
            throw new IllegalStateException( "Cannot read in state: " + stateName() );
        }
    }
}
