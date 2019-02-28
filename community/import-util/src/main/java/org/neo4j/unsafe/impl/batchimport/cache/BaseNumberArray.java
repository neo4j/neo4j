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
package org.neo4j.unsafe.impl.batchimport.cache;

/**
 * Contains basic functionality of fixed size number arrays.
 */
abstract class BaseNumberArray<N extends NumberArray<N>> implements NumberArray<N>
{
    protected final int itemSize;
    protected final long base;

    /**
     * @param itemSize byte size of each item in this array.
     * @param base base index to rebase all indexes in accessor methods off of. See {@link #at(long)}.
     */
    protected BaseNumberArray( int itemSize, long base )
    {
        this.itemSize = itemSize;
        this.base = base;
    }

    @SuppressWarnings( "unchecked" )
    @Override
    public N at( long index )
    {
        return (N)this;
    }

    /**
     * Utility for rebasing an external index to internal index.
     * @param index external index.
     * @return index into internal data structure.
     */
    protected long rebase( long index )
    {
        return index - base;
    }
}
