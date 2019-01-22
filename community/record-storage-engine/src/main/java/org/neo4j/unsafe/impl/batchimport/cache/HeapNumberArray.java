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

import static org.neo4j.helpers.Numbers.safeCastLongToInt;

/**
 * Base class for common functionality for any {@link NumberArray} where the data lives inside heap.
 */
abstract class HeapNumberArray<N extends NumberArray<N>> extends BaseNumberArray<N>
{
    protected HeapNumberArray( int itemSize, long base )
    {
        super( itemSize, base );
    }

    @Override
    public void acceptMemoryStatsVisitor( MemoryStatsVisitor visitor )
    {
        visitor.heapUsage( length() * itemSize ); // roughly
    }

    @Override
    public void close()
    {   // Nothing to close
    }

    protected int index( long index )
    {
        return safeCastLongToInt( rebase( index ) );
    }
}
