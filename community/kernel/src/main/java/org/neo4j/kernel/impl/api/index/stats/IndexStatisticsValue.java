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
package org.neo4j.kernel.impl.api.index.stats;

import java.util.Objects;

/**
 * Values in {@link IndexStatisticsStore}, having 16B of data.
 */
class IndexStatisticsValue
{
    static final int SIZE = Long.SIZE * 2;

    // Two longs, used for storing arbitrary counts, depending on what type of key this value is paired with.
    long first;
    long second;

    IndexStatisticsValue()
    {
    }

    IndexStatisticsValue( long first, long second )
    {
        this.first = first;
        this.second = second;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( first, second );
    }

    @Override
    public boolean equals( Object obj )
    {
        if ( obj instanceof IndexStatisticsValue )
        {
            IndexStatisticsValue other = (IndexStatisticsValue) obj;
            return first == other.first && second == other.second;
        }
        return false;
    }

    @Override
    public String toString()
    {
        return "[first:" + first + ",second:" + second + "]";
    }
}
