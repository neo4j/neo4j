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
import java.io.InterruptedIOException;
import java.util.concurrent.locks.Lock;

import org.neo4j.function.Consumer;
import org.neo4j.helpers.Exceptions;
import org.neo4j.helpers.Pair;
import org.neo4j.kernel.impl.util.function.Optional;
import org.neo4j.kernel.impl.util.function.Optionals;

abstract class RotationState<Key> extends ProgressiveState<Key>
{
    abstract ProgressiveState<Key> rotate( boolean force, RotationStrategy strategy, RotationTimerFactory timerFactory,
                                           Consumer<Headers.Builder> headersUpdater )
            throws IOException;

    @Override
    String stateName()
    {
        return "rotating";
    }

    @Override
    abstract void close() throws IOException;

    abstract long rotationVersion();

    static final class Rotation<Key> extends RotationState<Key>
    {
        private final ActiveState<Key> preState;
        private final PrototypeState<Key> postState;
        private final long threshold;

        Rotation( ActiveState<Key> preState, PrototypeState<Key> postState, long version )
        {
            this.preState = preState;
            this.postState = postState;
            this.threshold = version;
        }

        ActiveState<Key> rotate( boolean force, RotationStrategy strategy, RotationTimerFactory timerFactory,
                                Consumer<Headers.Builder> headersUpdater ) throws IOException
        {
            if ( !force )
            {
                RotationTimerFactory.RotationTimer rotationTimer = timerFactory.createTimer();
                for ( long expected = threshold - preState.store.version(), sleep = 10;
                      preState.applied() < expected; sleep = Math.min( sleep * 2, 100 ) )
                {
                    if ( rotationTimer.isTimedOut() )
                    {
                        throw new RotationTimeoutException( threshold, preState.store.version(),
                                rotationTimer.getElapsedTimeMillis());
                    }
                    try
                    {
                        Thread.sleep( sleep );
                    }
                    catch ( InterruptedException e )
                    {
                        throw Exceptions.withCause( new InterruptedIOException( "Rotation was interrupted." ), e );
                    }
                }
            }
            Pair<File, KeyValueStoreFile> next = strategy
                    .next( file(), updateHeaders( headersUpdater ), keyFormat().filter( preState.dataProvider() ) );
            return postState.create( ReadableState.store( preState.keyFormat(), next.other() ), next.first() );
        }

        @Override
        void close() throws IOException
        {
            preState.close();
        }

        @Override
        long rotationVersion()
        {
            return threshold;
        }

        private Headers updateHeaders( Consumer<Headers.Builder> headersUpdater )
        {
            Headers.Builder builder = new Headers.Builder( Headers.copy( preState.headers() ) );
            headersUpdater.accept( builder );
            return builder.headers();
        }

        @Override
        protected Optional<EntryUpdater<Key>> optionalUpdater( long version, Lock lock )
        {
            final EntryUpdater<Key> post = postState.updater( version, lock );
            if ( version <= threshold )
            {
                final EntryUpdater<Key> pre = preState.updater( version, lock );
                return Optionals.<EntryUpdater<Key>>some( new EntryUpdater<Key>( lock )
                {
                    @Override
                    public void apply( Key key, ValueUpdate update ) throws IOException
                    {
                        // Apply to the postState first, so that if the postState needs to read the state from the preState
                        // it will read the value prior to this update, then subsequent updates to the postState will not
                        // have to read from preState, ensuring that each update is applied exactly once to both preState
                        // and postState, which together with the commutativity of updates ensures consistent outcomes.
                        post.apply( key, update );
                        pre.apply( key, update );
                    }

                    @Override
                    public void close()
                    {
                        post.close();
                        pre.close();
                        super.close();
                    }
                } );
            }
            else
            {
                return Optionals.some( post );
            }
        }

        @Override
        protected File file()
        {
            return preState.file();
        }

        @Override
        protected long storedVersion()
        {
            return preState.storedVersion();
        }

        @Override
        protected KeyFormat<Key> keyFormat()
        {
            return preState.keyFormat();
        }

        @Override
        protected Headers headers()
        {
            return preState.headers();
        }

        @Override
        protected DataProvider dataProvider() throws IOException
        {
            return postState.dataProvider();
        }

        @Override
        protected int storedEntryCount()
        {
            return postState.storedEntryCount();
        }

        @Override
        protected EntryUpdater<Key> unsafeUpdater( Lock lock )
        {
            return postState.unsafeUpdater( lock );
        }

        @Override
        protected boolean hasChanges()
        {
            return preState.hasChanges() || postState.hasChanges();
        }

        @Override
        protected long version()
        {
            return postState.version();
        }

        @Override
        protected boolean lookup( Key key, ValueSink sink ) throws IOException
        {
            return postState.lookup( key, sink );
        }
    }
}
