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
package org.neo4j.kernel.api.impl.index.bitmaps;

public enum BitmapFormat
{
    _32( 5, 0xFFFF_FFFFL ),
    _64( 6, -1 );
    public final int shift;
    private final long mask;

    private BitmapFormat( int shift, long mask )
    {
        this.shift = shift;
        this.mask = mask;
    }

    public int rangeSize() {
        return 1 << shift;
    }

    public long rangeOf( long id )
    {
        return id >> shift;
    }

    public long[] convertRangeAndBitmapToArray( long range, long bitmap )
    {
        bitmap &= mask;
        int bitCount = Long.bitCount( bitmap );
        if ( bitCount == 0 )
        {
            return null;
        }
        long[] result = new long[bitCount];
        for ( int i = 0, offset = -1; i < result.length; i++ )
        {
            //noinspection StatementWithEmptyBody
            while ( (bitmap & (1L << ++offset)) == 0 )
            {
                ;
            }
            result[i] = (range << shift) | offset;
        }
        return result;
    }

    // Returns true if the label exists on the given node for the given bitmap
    public boolean hasLabel( long bitmap, long nodeId )
    {
        long normalizedNodeId = nodeId % (1L << shift);

        long bitRepresentingNodeIdInBitmap = 1L << normalizedNodeId;

        return ((bitmap & bitRepresentingNodeIdInBitmap) != 0);
    }

    public void set( Bitmap bitmap, long id, boolean set )
    {
        if ( bitmap == null )
        {
            return;
        }
        long low = (1L << shift) - 1;
        if ( set )
        {
            bitmap.bitmap |= 1L << (id & low);
        }
        else
        {
            bitmap.bitmap &= mask ^ (1L << (id & low));
        }
    }
}
