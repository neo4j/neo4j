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
package org.neo4j.kernel.impl.index.labelscan;

import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.Layout;
import org.neo4j.io.pagecache.PageCursor;

/**
 * {@link Layout} for {@link GBPTree} used by {@link NativeLabelScanStore}.
 *
 * <ul>
 * <li>
 * Each keys is a combination of {@code labelId} and {@code nodeIdRange} ({@code nodeId/64}).
 * </li>
 * <li>
 * Each value is a 64-bit bit set (a primitive {@code long}) where each set bit in it represents
 * a node with that label, such that {@code nodeId = nodeIdRange+bitOffset}. Range size is 64 bits.
 * </li>
 * </ul>
 */
class LabelScanLayout extends Layout.Adapter<LabelScanKey,LabelScanValue>
{
    /**
     * Name part of the {@link #identifier()} value.
     */
    private static final String IDENTIFIER_NAME = "LSL";

    /**
     * Size of each {@link LabelScanKey}.
     */
    private static final int KEY_SIZE = Integer.BYTES/*labelId*/ + 6/*idRange*/;

    /**
     * Compares {@link LabelScanKey}, giving ascending order of {@code labelId} then {@code nodeIdRange}.
     */
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
    public LabelScanKey copyKey( LabelScanKey key, LabelScanKey into )
    {
        into.labelId = key.labelId;
        into.idRange = key.idRange;
        return into;
    }

    @Override
    public LabelScanValue newValue()
    {
        return new LabelScanValue();
    }

    @Override
    public int keySize( LabelScanKey key )
    {
        return KEY_SIZE;
    }

    @Override
    public int valueSize( LabelScanValue value )
    {
        return LabelScanValue.RANGE_SIZE_BYTES;
    }

    @Override
    public void writeKey( PageCursor cursor, LabelScanKey key )
    {
        cursor.putInt( key.labelId );
        put6ByteLong( cursor, key.idRange );
    }

    private static void put6ByteLong( PageCursor cursor, long value )
    {
        cursor.putInt( (int) value );
        cursor.putShort( (short) (value >>> Integer.SIZE) );
    }

    @Override
    public void writeValue( PageCursor cursor, LabelScanValue value )
    {
        cursor.putLong( value.bits );
    }

    @Override
    public void readKey( PageCursor cursor, LabelScanKey into, int keySize )
    {
        into.labelId = cursor.getInt();
        into.idRange = get6ByteLong( cursor );
    }

    private static long get6ByteLong( PageCursor cursor )
    {
        long low4b = cursor.getInt() & 0xFFFFFFFFL;
        long high2b = cursor.getShort();
        return low4b | (high2b << Integer.SIZE);
    }

    @Override
    public void readValue( PageCursor cursor, LabelScanValue into, int valueSize )
    {
        into.bits = cursor.getLong();
    }

    @Override
    public boolean fixedSize()
    {
        return true;
    }

    @Override
    public long identifier()
    {
        return Layout.namedIdentifier( IDENTIFIER_NAME, LabelScanValue.RANGE_SIZE );
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
}
