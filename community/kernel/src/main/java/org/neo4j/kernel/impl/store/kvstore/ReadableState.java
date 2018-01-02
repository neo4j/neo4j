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

import java.io.IOException;

import static org.neo4j.kernel.impl.store.kvstore.DataProvider.EMPTY_DATA_PROVIDER;

abstract class ReadableState<Key>
{
    protected abstract KeyFormat<Key> keyFormat();

    protected abstract Headers headers();

    protected abstract long version();

    protected abstract boolean lookup( Key key, ValueSink sink ) throws IOException;

    protected abstract DataProvider dataProvider() throws IOException;

    protected abstract int storedEntryCount();

    abstract void close() throws IOException;

    static <Key> ReadableState<Key> store( final KeyFormat<Key> keys, final KeyValueStoreFile store )
    {
        return new ReadableState<Key>()
        {
            @Override
            protected KeyFormat<Key> keyFormat()
            {
                return keys;
            }

            @Override
            protected Headers headers()
            {
                return store.headers();
            }

            @Override
            protected long version()
            {
                return keys.version( headers() ); // TODO: 'keys' is not the right guy to have this responsibility
            }

            @Override
            protected boolean lookup( Key key, ValueSink sink ) throws IOException
            {
                return store.scan( new KeyFormat.Searcher<>( keys, key ), sink );
            }

            @Override
            protected DataProvider dataProvider() throws IOException
            {
                return store.dataProvider();
            }

            @Override
            protected int storedEntryCount()
            {
                return store.entryCount();
            }

            @Override
            void close() throws IOException
            {
                store.close();
            }
        };
    }

    static <Key> ReadableState<Key> empty( final KeyFormat<Key> keys, final long version )
    {
        return new ReadableState<Key>()
        {
            @Override
            protected KeyFormat<Key> keyFormat()
            {
                return keys;
            }

            @Override
            protected Headers headers()
            {
                return null;
            }

            @Override
            protected long version()
            {
                return version;
            }

            @Override
            protected boolean lookup( Key key, ValueSink sink ) throws IOException
            {
                return false;
            }

            @Override
            protected DataProvider dataProvider() throws IOException
            {
                return EMPTY_DATA_PROVIDER;
            }

            @Override
            protected int storedEntryCount()
            {
                return 0;
            }

            @Override
            void close() throws IOException
            {
            }
        };
    }
}
