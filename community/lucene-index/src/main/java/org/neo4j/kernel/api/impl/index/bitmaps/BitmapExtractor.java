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

import org.neo4j.helpers.collection.PrefetchingIterator;

public class BitmapExtractor extends PrefetchingIterator<long[]>
{
    private final BitmapFormat format;
    private final long[] rangeBitmap;
    private int offset;

    public BitmapExtractor( BitmapFormat format, long... rangeBitmap )
    {
        this.format = format;
        this.rangeBitmap = rangeBitmap;
    }

    @Override
    protected long[] fetchNextOrNull()
    {
        while ( offset < rangeBitmap.length )
        {
            long[] result = format.convertRangeAndBitmapToArray( rangeBitmap[offset], rangeBitmap[offset + 1] );
            offset += 2;
            if ( result != null )
            {
                return result;
            }
        }
        return null;
    }
}
