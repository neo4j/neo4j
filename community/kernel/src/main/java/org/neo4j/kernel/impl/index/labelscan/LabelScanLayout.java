/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.impl.index.labelscan;

import org.neo4j.index.bptree.Layout;
import org.neo4j.io.pagecache.PageCursor;

import static java.lang.Long.bitCount;

/**
 * {@link LabelScanKey} is used, but {@link LabelScanKey#idRange} is instead a range, 64 nodes in each
 */
public class LabelScanLayout implements Layout<LabelScanKey,LabelScanValue>
{
    private static final int KEY_SIZE = Integer.BYTES/*labelId*/ + 6/*idRange*/;

    private final int rangeSize;
    private final int rangeSizeBytes;

    public LabelScanLayout( int rangeSize )
    {
        // asserts values are 8, 16, 32 or 64
        assert bitCount( rangeSize ) == 1 && (rangeSize & ~0b1111000) == 0;

        this.rangeSize = rangeSize;
        this.rangeSizeBytes = rangeSize >>> 3;
    }

    @Override
    public int compare( LabelScanKey o1, LabelScanKey o2 )
    {
        int labelComparison = Integer.compare( o1.labelId, o2.labelId );
        return labelComparison != 0 ? labelComparison : Long.compare( o1.idRange, o2.idRange );
    }

    @Override
    public LabelScanKey newKey()
    {
        return new LabelScanKey();
    }

    @Override
    public LabelScanValue newValue()
    {
        return new LabelScanValue();
    }

    @Override
    public int keySize()
    {
        return KEY_SIZE;
    }

    @Override
    public int valueSize()
    {
        return rangeSizeBytes;
    }

    @Override
    public void writeKey( PageCursor cursor, LabelScanKey key )
    {
        cursor.putInt( key.labelId );
        put6ByteLong( cursor, key.idRange );
    }

    private void put6ByteLong( PageCursor cursor, long value )
    {
        cursor.putInt( (int) value );
        cursor.putShort( (short) (value >>> 32) );
    }

    @Override
    public void writeValue( PageCursor cursor, LabelScanValue value )
    {
        switch ( rangeSize )
        {
        case 8:
            cursor.putByte( (byte) value.bits );
            break;
        case 16:
            cursor.putShort( (short) value.bits );
            break;
        case 32:
            cursor.putInt( (int) value.bits );
            break;
        case 64:
            cursor.putLong( value.bits );
            break;
        default:
            throw new IllegalArgumentException( String.valueOf( rangeSize ) );
        }
    }

    @Override
    public void readKey( PageCursor cursor, LabelScanKey into )
    {
        into.labelId = cursor.getInt();
        into.idRange = get6ByteLong( cursor );
    }

    private long get6ByteLong( PageCursor cursor )
    {
        long low4b = cursor.getInt() & 0xFFFFFFFFL;
        long high2b = cursor.getShort();
        return low4b | (high2b << 32);
    }

    @Override
    public void readValue( PageCursor cursor, LabelScanValue into )
    {
        switch ( rangeSize )
        {
        case 8:
            into.bits = cursor.getByte() & 0xFF;
            break;
        case 16:
            into.bits = cursor.getShort() & 0xFFFF;
            break;
        case 32:
            into.bits = cursor.getInt() & 0xFFFFFFFFL;
            break;
        case 64:
            into.bits = cursor.getLong();
            break;
        default:
            throw new IllegalArgumentException( String.valueOf( rangeSize ) );
        }
    }

    @Override
    public long identifier()
    {
        return Layout.namedIdentifier( "LSL", rangeSize );
    }

    @Override
    public int majorVersion()
    {
        return 0;
    }

    @Override
    public int minorVersion()
    {
        return 1;
    }

    @Override
    public void writeMetaData( PageCursor cursor )
    {
        cursor.putInt( rangeSize );
    }

    @Override
    public void readMetaData( PageCursor cursor )
    {
        int rangeSize = cursor.getInt();
        if ( this.rangeSize != rangeSize )
        {
            throw new IllegalArgumentException( "A different range size " + this.rangeSize +
                    " was specified when loading an index with actual range size " + rangeSize );
        }
    }
}
