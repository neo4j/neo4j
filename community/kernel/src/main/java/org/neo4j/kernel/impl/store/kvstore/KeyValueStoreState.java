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

abstract class KeyValueStoreState<Key, Meta>
{
    public abstract Meta metadata();

    public abstract boolean hasChanges();

    public abstract int totalEntriesStored();

    // Data access

    public abstract <Value> Value lookup( Key key, AbstractKeyValueStore.Reader<Value> valueReader ) throws IOException;

    public abstract DataProvider dataProvider() throws IOException;

    public abstract void apply( AbstractKeyValueStore.Update<Key> update ) throws IOException;

    // State transitions

    public KeyValueStoreState<Key, Meta> init() throws IOException
    {
        throw new IllegalStateException( "The store has already been initialised" );
    }

    public KeyValueStoreState<Key, Meta> start() throws IOException
    {
        return this;
    }

    public abstract KeyValueStoreState<Key, Meta> rotate( Meta metadata ) throws IOException;

    public final KeyValueStoreState<Key, Meta> shutdown() throws IOException
    {
        if ( hasChanges() )
        {
            throw new IllegalStateException( "Cannot shutdown when there are changes" );
        }
        return close();
    }

    abstract KeyValueStoreState<Key, Meta> close() throws IOException;

    @Override
    public String toString()
    {
        return getClass().getSimpleName();
    }

    public abstract File file();

    abstract KeyValueStoreFile<Meta> openStoreFile( File path ) throws IOException;

    // Initial state

    static abstract class Stopped<Key, Meta> extends PreState<Key, Meta>
    {
        final RotationStrategy<Meta> rotation;

        Stopped( RotationStrategy<Meta> rotation )
        {
            this.rotation = rotation;
        }

        @Override
        public final KeyValueStoreState<Key, Meta> init() throws IOException
        {
            Pair<File, KeyValueStoreFile<Meta>> opened = rotation.open();
            if ( opened == null )
            {
                return new Initialized();
            }
            return create( opened.first(), opened.other() );
        }

        abstract KeyValueStoreState<Key, Meta> create( File path, KeyValueStoreFile<Meta> file );

        @Override
        final IllegalStateException invalidState()
        {
            return new IllegalStateException( "The store has not been initialized." );
        }

        @Override
        final KeyValueStoreState<Key, Meta> close() throws IOException
        {
            return this;
        }

        @Override
        KeyValueStoreFile<Meta> openStoreFile( File path ) throws IOException
        {
            return rotation.openStoreFile( path );
        }

        private class Initialized extends PreState<Key, Meta>
        {
            @Override
            public KeyValueStoreState<Key, Meta> start() throws IOException
            {
                Pair<File, KeyValueStoreFile<Meta>> opened = rotation.create();
                return create( opened.first(), opened.other() );
            }

            @Override
            public Meta metadata()
            {
                return null;
            }

            @Override
            KeyValueStoreState<Key, Meta> close() throws IOException
            {
                return Stopped.this;
            }

            @Override
            IllegalStateException invalidState()
            {
                return new IllegalStateException( "The store has not been started." );
            }

            @Override
            KeyValueStoreFile<Meta> openStoreFile( File path ) throws IOException
            {
                return Stopped.this.openStoreFile( path );
            }
        }
    }

    private static abstract class PreState<Key, Meta> extends KeyValueStoreState<Key, Meta>
    {
        @Override
        public File file()
        {
            return null;
        }

        @Override
        public Meta metadata()
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
        public final void apply( AbstractKeyValueStore.Update<Key> update ) throws IOException
        {
            throw invalidState();
        }

        @Override
        public KeyValueStoreState<Key, Meta> init() throws IOException
        {
            throw invalidState();
        }

        @Override
        public KeyValueStoreState<Key, Meta> start() throws IOException
        {
            throw invalidState();
        }

        @Override
        public final KeyValueStoreState<Key, Meta> rotate( Meta metadata ) throws IOException
        {
            throw invalidState();
        }

        abstract IllegalStateException invalidState();
    }
}
