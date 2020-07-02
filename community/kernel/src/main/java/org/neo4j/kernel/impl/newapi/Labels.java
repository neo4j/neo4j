/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.kernel.impl.newapi;

import org.eclipse.collections.api.set.primitive.LongSet;

import java.util.Arrays;

import org.neo4j.internal.kernel.api.TokenSet;

public class Labels implements TokenSet
{
    /**
     * This really only needs to be {@code int[]}, but the underlying implementation uses {@code long[]} for some
     * reason.
     */
    private final long[] labelIds;

    private Labels( long[] labelIds )
    {
        this.labelIds = labelIds;
    }

    public static Labels from( long... labels )
    {
        return new Labels( labels );
    }

    static Labels from( LongSet set )
    {
        return new Labels( set.toArray() );
    }

    @Override
    public int numberOfTokens()
    {
        return labelIds.length;
    }

    @Override
    public int token( int offset )
    {
        return (int) labelIds[offset];
    }

    @Override
    public boolean contains( int token )
    {
        //It may look tempting to use binary search
        //however doing a linear search is actually faster for reasonable
        //label sizes (≤100 labels)
        for ( long label : labelIds )
        {
            assert (int) label == label : "value too big to be represented as and int";
            if ( label == token )
            {
                return true;
            }
        }
        return false;
    }

    @Override
    public String toString()
    {
        return "Labels" + Arrays.toString( labelIds );
    }

    @Override
    public long[] all()
    {
        return labelIds;
    }
}
