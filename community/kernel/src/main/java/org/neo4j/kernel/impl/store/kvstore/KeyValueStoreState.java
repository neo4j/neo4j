/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import org.neo4j.helpers.Pair;

abstract class KeyValueStoreState<Key>
{
    public abstract Headers headers();

    public abstract boolean hasChanges();

    public abstract int totalEntriesStored();

    // Data access

    public abstract <Value> Value lookup( Key key, AbstractKeyValueStore.Reader<Value> valueReader ) throws IOException;

    public abstract DataProvider dataProvider() throws IOException;

    public abstract void apply( Key key, ValueUpdate update, boolean reset ) throws IOException;

    // State transitions

    public KeyValueStoreState<Key> init() throws IOException
    {
        throw new IllegalStateException( "The store has already been initialised" );
    }

    public KeyValueStoreState<Key> start() throws IOException
    {
        return this;
    }

    public abstract KeyValueStoreState<Key> rotate( Headers headers ) throws IOException;

    public final KeyValueStoreState<Key> shutdown() throws IOException
    {
        if ( hasChanges() )
        {
            throw new IllegalStateException( "Cannot shutdown when there are changes" );
        }
        return close();
    }

    abstract KeyValueStoreState<Key> close() throws IOException;

    @Override
    public String toString()
    {
        return getClass().getSimpleName();
    }

    public abstract File file();

    abstract KeyValueStoreFile openStoreFile( File path ) throws IOException;

    // Initial state

    static abstract class Stopped<Key> extends PreState<Key>
    {
        final RotationStrategy rotation;

        Stopped( RotationStrategy rotation )
        {
            this.rotation = rotation;
        }

        @Override
        public final KeyValueStoreState<Key> init() throws IOException
        {
            Pair<File, KeyValueStoreFile> opened = rotation.open();
            if ( opened == null )
            {
                return new Initialized();
            }
            return create( opened.first(), opened.other() );
        }

        abstract KeyValueStoreState<Key> create( File path, KeyValueStoreFile file );

        @Override
        final IllegalStateException invalidState()
        {
            return new IllegalStateException( "The store has not been initialized." );
        }

        @Override
        final KeyValueStoreState<Key> close() throws IOException
        {
            return this;
        }

        @Override
        KeyValueStoreFile openStoreFile( File path ) throws IOException
        {
            return rotation.openStoreFile( path );
        }

        private class Initialized extends PreState<Key>
        {
            @Override
            public KeyValueStoreState<Key> start() throws IOException
            {
                Pair<File, KeyValueStoreFile> opened = rotation.create();
                return create( opened.first(), opened.other() );
            }

            @Override
            public Headers headers()
            {
                return null;
            }

            @Override
            KeyValueStoreState<Key> close() throws IOException
            {
                return Stopped.this;
            }

            @Override
            IllegalStateException invalidState()
            {
                return new IllegalStateException( "The store has not been started." );
            }

            @Override
            KeyValueStoreFile openStoreFile( File path ) throws IOException
            {
                return Stopped.this.openStoreFile( path );
            }
        }
    }

    private static abstract class PreState<Key> extends KeyValueStoreState<Key>
    {
        @Override
        public File file()
        {
            return null;
        }

        @Override
        public Headers headers()
        {
            throw invalidState();
        }

        @Override
        public final boolean hasChanges()
        {
            return false;
        }

        @Override
        public final int totalEntriesStored()
        {
            throw invalidState();
        }

        @Override
        public final <Value> Value lookup( Key key, AbstractKeyValueStore.Reader<Value> request ) throws IOException
        {
            throw invalidState();
        }

        @Override
        public final DataProvider dataProvider() throws IOException
        {
            throw invalidState();
        }

        @Override
        public void apply( Key key, ValueUpdate update, boolean reset )
        {
            throw invalidState();
        }

        @Override
        public KeyValueStoreState<Key> init() throws IOException
        {
            throw invalidState();
        }

        @Override
        public KeyValueStoreState<Key> start() throws IOException
        {
            throw invalidState();
        }

        @Override
        public final KeyValueStoreState<Key> rotate( Headers headers ) throws IOException
        {
            throw invalidState();
        }

        abstract IllegalStateException invalidState();
    }
}
