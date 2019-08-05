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
package org.neo4j.internal.id.indexed;

import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.Layout;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.util.Preconditions;

/**
 * {@link Layout} for a {@link GBPTree} writing and reading the ID ranges that make up the contents of an {@link IndexedIdGenerator}.
 */
class IdRangeLayout extends Layout.Adapter<IdRangeKey, IdRange>
{
    private final int longsPerEntry;
    private final int idsPerEntry;

    IdRangeLayout( int idsPerEntry )
    {
        Preconditions.checkArgument( Integer.bitCount( idsPerEntry ) == 1, "idsPerEntry must be power of 2, was %d", idsPerEntry );
        this.longsPerEntry = ((idsPerEntry - 1) / (IdRange.BITSET_SIZE)) + 1;
        this.idsPerEntry = idsPerEntry;
    }

    @Override
    public IdRangeKey newKey()
    {
        return new IdRangeKey( 0 );
    }

    @Override
    public IdRangeKey copyKey( IdRangeKey key, IdRangeKey into )
    {
        into.setIdRangeIdx( key.getIdRangeIdx() );
        return into;
    }

    @Override
    public IdRange newValue()
    {
        return new IdRange( longsPerEntry );
    }

    @Override
    public int keySize( IdRangeKey key )
    {
        // idRangeIdx
        return Long.BYTES;
    }

    @Override
    public int valueSize( IdRange ignore )
    {
        // generation + state bit-sets
        return Long.BYTES + longsPerEntry * Long.BYTES;
    }

    @Override
    public void writeKey( PageCursor cursor, IdRangeKey key )
    {
        cursor.putLong( key.getIdRangeIdx() );
    }

    @Override
    public void writeValue( PageCursor cursor, IdRange value )
    {
        cursor.putLong( value.getGeneration() );
        writeLongs( cursor, value.getLongs() );
    }

    @Override
    public void readKey( PageCursor cursor, IdRangeKey into, int keySize )
    {
        into.setIdRangeIdx( cursor.getLong() );
    }

    @Override
    public void readValue( PageCursor cursor, IdRange into, int ignore )
    {
        into.setGeneration( cursor.getLong() );
        readLongs( cursor, into.getLongs() );
    }

    private static void writeLongs( PageCursor cursor, long[] octlets )
    {
        for ( long octlet : octlets )
        {
            cursor.putLong( octlet );
        }
    }

    private static void readLongs( PageCursor cursor, long[] octlets )
    {
        for ( int i = 0; i < octlets.length; i++ )
        {
            octlets[i] = cursor.getLong();
        }
    }

    @Override
    public boolean fixedSize()
    {
        return true;
    }

    @Override
    public long identifier()
    {
        return 3735929054L + idsPerEntry;
    }

    @Override
    public int majorVersion()
    {
        return 1;
    }

    @Override
    public int minorVersion()
    {
        return 1;
    }

    @Override
    public int compare( IdRangeKey o1, IdRangeKey o2 )
    {
        return Long.compare( o1.getIdRangeIdx(), o2.getIdRangeIdx() );
    }
}
