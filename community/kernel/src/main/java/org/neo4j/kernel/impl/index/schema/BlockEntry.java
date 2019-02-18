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
package org.neo4j.kernel.impl.index.schema;

import org.neo4j.index.internal.gbptree.Layout;
import org.neo4j.io.pagecache.PageCursor;

import static java.lang.String.format;
import static org.neo4j.index.internal.gbptree.DynamicSizeUtil.extractKeySize;
import static org.neo4j.index.internal.gbptree.DynamicSizeUtil.extractValueSize;
import static org.neo4j.index.internal.gbptree.DynamicSizeUtil.getOverhead;
import static org.neo4j.index.internal.gbptree.DynamicSizeUtil.putKeyValueSize;
import static org.neo4j.index.internal.gbptree.DynamicSizeUtil.readKeyValueSize;

/**
 * A {@link BlockEntry} is a key-value mapping and the smallest unit in the {@link BlockStorage} and {@link IndexUpdateStorage} hierarchy. Except for being a
 * container class for key-value pairs, it also provide static methods for serializing and deserializing {@link BlockEntry} instances and calculating total
 * store size of them.
 */
class BlockEntry<KEY,VALUE>
{
    private final KEY key;
    private final VALUE value;

    BlockEntry( KEY key, VALUE value )
    {
        this.key = key;
        this.value = value;
    }

    KEY key()
    {
        return key;
    }

    VALUE value()
    {
        return value;
    }

    @Override
    public String toString()
    {
        return format( "[%s=%s]", key, value );
    }

    static <VALUE, KEY> int entrySize( Layout<KEY,VALUE> layout, KEY key, VALUE value )
    {
        int keySize = layout.keySize( key );
        int valueSize = layout.valueSize( value );
        return keySize + valueSize + getOverhead( keySize, valueSize );
    }

    static <VALUE, KEY> int keySize( Layout<KEY,VALUE> layout, KEY key )
    {
        int keySize = layout.keySize( key );
        return keySize + getOverhead( keySize, 0 );
    }

    static <KEY, VALUE> BlockEntry<KEY,VALUE> read( PageCursor pageCursor, Layout<KEY,VALUE> layout )
    {
        KEY key = layout.newKey();
        VALUE value = layout.newValue();
        read( pageCursor, layout, key, value );
        return new BlockEntry<>( key, value );
    }

    static <KEY, VALUE> void read( PageCursor pageCursor, Layout<KEY,VALUE> layout, KEY key, VALUE value )
    {
        long entrySize = readKeyValueSize( pageCursor );
        layout.readKey( pageCursor, key, extractKeySize( entrySize ) );
        layout.readValue( pageCursor, value, extractValueSize( entrySize ) );
    }

    static <KEY, VALUE> void read( PageCursor pageCursor, Layout<KEY,VALUE> layout, KEY key )
    {
        long entrySize = readKeyValueSize( pageCursor );
        layout.readKey( pageCursor, key, extractKeySize( entrySize ) );
    }

    static <KEY, VALUE> void write( PageCursor pageCursor, Layout<KEY,VALUE> layout, BlockEntry<KEY,VALUE> entry )
    {
        write( pageCursor, layout, entry.key(), entry.value() );
    }

    static <KEY, VALUE> void write( PageCursor pageCursor, Layout<KEY,VALUE> layout, KEY key, VALUE value )
    {
        int keySize = layout.keySize( key );
        int valueSize = layout.valueSize( value );
        putKeyValueSize( pageCursor, keySize, valueSize );
        layout.writeKey( pageCursor, key );
        layout.writeValue( pageCursor, value );
    }

    static <KEY, VALUE> void write( PageCursor pageCursor, Layout<KEY,VALUE> layout, KEY key )
    {
        int keySize = layout.keySize( key );
        putKeyValueSize( pageCursor, keySize, 0 );
        layout.writeKey( pageCursor, key );
    }
}
