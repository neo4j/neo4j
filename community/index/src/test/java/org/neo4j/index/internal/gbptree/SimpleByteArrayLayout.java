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
package org.neo4j.index.internal.gbptree;

import java.nio.ByteBuffer;
import java.util.Arrays;

import org.neo4j.io.memory.ByteBuffers;
import org.neo4j.io.pagecache.PageCursor;

/**
 * Layout that can create keys and values with varying size.
 * Even if the sizes are varying they are still deterministic
 * based on seed given to {@link #key(long)} or {@link #value(long)}.
 * In this way we can create a randomized workload of keys and values
 * that is still deterministic. This is very useful when creating tests
 * that test both dynamic and fixed layout.
 * <p>
 * Keys and values can be compared in two different ways:
 * <ul>
 *     <li>useFirstLongAsSeed - The first 8 bytes in key or value are read as a long value and comparison is based on this.
 *                              This is the default and most likely what you want.</li>
 *     <li>!useFirstLongAsSeed - Keys and values are compared byte by byte in lexicographic order.
 *                               Only useful if you need to control entry size exactly from outside.</li>
 * </ul>
 */
public class SimpleByteArrayLayout extends TestLayout<RawBytes,RawBytes>
{
    private static final int DEFAULT_LARGE_ENTRY_SIZE = Long.BYTES;
    private static final long NO_LARGE_ENTRIES_MODULO = 0;
    private final boolean useFirstLongAsSeed;
    private final int largeEntriesSize;
    private final long largeEntryModulo;

    /**
     * This should be default constructor unless you want to exactly control entry size from outside
     * or you want entries to vary vastly in size.
     */
    SimpleByteArrayLayout()
    {
        this( true, DEFAULT_LARGE_ENTRY_SIZE, NO_LARGE_ENTRIES_MODULO );
    }

    /**
     * Use this constructor if you want to control entry sizes exactly from outside.
     * Using useFirstLongAsSeed=false will let this keys and value be compared byte
     * by byte. There is no point calling this constructor with useFirstLongAsSeed=true.
     *
     * @param useFirstLongAsSeed False if keys and values should be compared byte by byte.
     */
    SimpleByteArrayLayout( boolean useFirstLongAsSeed )
    {
        this( useFirstLongAsSeed, DEFAULT_LARGE_ENTRY_SIZE, NO_LARGE_ENTRIES_MODULO );
    }

    /**
     * Use this constructor if you want to use entries that vary vastly in size.
     * <p>
     * When generating {@link #key(long) keys} or {@link #value(long) values} from seed
     * largeEntriesSize and largeEntryModulo can be used to force a certain size for those
     * keys or values. If (seed % largeEntryModulo == 0) then the key or value will have
     * size equal to largeEntriesSize instead of the dynamically changing size it would
     * otherwise have. This is mostly used to create a set of keys and values with large
     * difference in size.
     *
     * @param largeEntriesSize Total size of large keys or values.
     * @param largeEntryModulo Control to what degree large values should be used,
     * (seed % largeEntryModulo == 0) will generate a large value. 0=never use large values.
     */
    SimpleByteArrayLayout( int largeEntriesSize, long largeEntryModulo )
    {
        this( true, largeEntriesSize, largeEntryModulo );
    }

    private SimpleByteArrayLayout( boolean useFirstLongAsSeed, int largeEntriesSize, long largeEntryModulo )
    {
        super( false, 666, 0, 0 );
        this.useFirstLongAsSeed = useFirstLongAsSeed;
        this.largeEntriesSize = largeEntriesSize;
        this.largeEntryModulo = largeEntryModulo;
    }

    @Override
    public RawBytes newKey()
    {
        return new RawBytes();
    }

    @Override
    public RawBytes copyKey( RawBytes rawBytes, RawBytes into )
    {
        return copyKey( rawBytes, into, rawBytes.bytes.length );
    }

    private RawBytes copyKey( RawBytes rawBytes, RawBytes into, int length )
    {
        into.bytes = Arrays.copyOf( rawBytes.bytes, length );
        return into;
    }

    @Override
    public RawBytes newValue()
    {
        return new RawBytes();
    }

    @Override
    public int keySize( RawBytes rawBytes )
    {
        if ( rawBytes == null )
        {
            return -1;
        }
        return rawBytes.bytes.length;
    }

    @Override
    public int valueSize( RawBytes rawBytes )
    {
        if ( rawBytes == null )
        {
            return -1;
        }
        return rawBytes.bytes.length;
    }

    @Override
    public void writeKey( PageCursor cursor, RawBytes rawBytes )
    {
        cursor.putBytes( rawBytes.bytes );
    }

    @Override
    public void writeValue( PageCursor cursor, RawBytes rawBytes )
    {
        cursor.putBytes( rawBytes.bytes );
    }

    @Override
    public void readKey( PageCursor cursor, RawBytes into, int keySize )
    {
        into.bytes = new byte[keySize];
        cursor.getBytes( into.bytes );
    }

    @Override
    public void readValue( PageCursor cursor, RawBytes into, int valueSize )
    {
        into.bytes = new byte[valueSize];
        cursor.getBytes( into.bytes );
    }

    @Override
    public void minimalSplitter( RawBytes left, RawBytes right, RawBytes into )
    {
        long leftSeed = keySeed( left );
        long rightSeed = keySeed( right );
        if ( useFirstLongAsSeed && leftSeed != rightSeed )
        {
            // Minimal splitter is first 8B (seed)
            copyKey( right, into, Long.BYTES );
        }
        else
        {
            // They had the same seed. Need to look at entire array
            int maxLength = Math.min( left.bytes.length, right.bytes.length );
            int firstIndexToDiffer = 0;
            for ( ; firstIndexToDiffer < maxLength; firstIndexToDiffer++ )
            {
                if ( left.bytes[firstIndexToDiffer] != right.bytes[firstIndexToDiffer] )
                {
                    break;
                }
            }
            // Convert from index to length
            int targetLength = firstIndexToDiffer + 1;
            copyKey( right, into, targetLength );
        }
    }

    @Override
    public int compare( RawBytes o1, RawBytes o2 )
    {
        if ( o1.bytes == null )
        {
            return -1;
        }
        if ( o2.bytes == null )
        {
            return 1;
        }
        if ( useFirstLongAsSeed )
        {
            int compare = Long.compare( keySeed( o1 ), keySeed( o2 ) );
            return compare != 0 ? compare : byteArrayCompare( o1.bytes, o2.bytes, Long.BYTES );
        }
        else
        {
            return byteArrayCompare( o1.bytes, o2.bytes, 0 );
        }
    }

    @Override
    int compareValue( RawBytes v1, RawBytes v2 )
    {
        return compare( v1, v2 );
    }

    private int byteArrayCompare( byte[] a, byte[] b, int fromPos )
    {
        assert a != null && b != null : "Null arrays not supported.";

        if ( a == b )
        {
            return 0;
        }

        int length = Math.min( a.length, b.length );
        for ( int i = fromPos; i < length; i++ )
        {
            int compare = Byte.compare( a[i], b[i] );
            if ( compare != 0 )
            {
                return compare;
            }
        }

        return Integer.compare( a.length, b.length );
    }

    @Override
    public RawBytes key( long seed )
    {
        RawBytes key = newKey();
        key.bytes = fromSeed( seed );
        return key;
    }

    @Override
    public RawBytes value( long seed )
    {
        RawBytes value = newValue();
        value.bytes = fromSeed( seed );
        return value;
    }

    @Override
    public long keySeed( RawBytes rawBytes )
    {
        return toSeed( rawBytes );
    }

    @Override
    public long valueSeed( RawBytes rawBytes )
    {
        return toSeed( rawBytes );
    }

    @Override
    public void initializeAsLowest( RawBytes rawBytes )
    {
        rawBytes.bytes = new byte[8];
        Arrays.fill( rawBytes.bytes, Byte.MIN_VALUE );
    }

    @Override
    public void initializeAsHighest( RawBytes rawBytes )
    {
        rawBytes.bytes = new byte[8];
        Arrays.fill( rawBytes.bytes, Byte.MAX_VALUE );
    }

    private long toSeed( RawBytes rawBytes )
    {
        ByteBuffer buffer = ByteBuffers.allocate( Long.BYTES );
        // Because keySearch is done inside the same shouldRetry block as keyCount()
        // We risk reading crap data. This is not a problem because we will retry
        // but buffer will throw here if we don't take that into consideration.
        byte[] bytes = rawBytes.bytes;
        if ( bytes.length >= Long.BYTES )
        {
            buffer.put( bytes, 0, Long.BYTES );
            buffer.flip();
            return buffer.getLong();
        }
        return 0;
    }

    private byte[] fromSeed( long seed )
    {
        int tail = (int) Math.abs( seed % Long.BYTES );
        if ( largeEntryModulo != NO_LARGE_ENTRIES_MODULO && (seed % largeEntryModulo) == 0 )
        {
            tail = largeEntriesSize - Long.BYTES;
        }
        ByteBuffer buffer = ByteBuffers.allocate( Long.BYTES + tail );
        buffer.putLong( seed );
        buffer.put( new byte[tail] );
        return buffer.array();
    }
}
