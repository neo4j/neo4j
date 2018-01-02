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

interface KeyFormat<Key>
{
    class Searcher<Key> implements SearchKey
    {
        private final KeyFormat<Key> keys;
        private final Key key;

        Searcher( KeyFormat<Key> keys, Key key )
        {
            this.keys = keys;
            this.key = key;
        }

        @Override
        public void searchKey( WritableBuffer key )
        {
            keys.writeKey( this.key, key );
        }
    }

    void writeKey( Key key, WritableBuffer buffer );

    int keySize();

    int valueSize();

    long version( Headers headers );

    DataProvider filter( DataProvider provider );
}
