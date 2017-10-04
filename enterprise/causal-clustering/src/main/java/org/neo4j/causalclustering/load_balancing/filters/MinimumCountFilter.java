/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.causalclustering.load_balancing.filters;

import java.util.Objects;
import java.util.Set;

import static java.util.Collections.emptySet;

/**
 * Only returns a valid (non-empty) result if the minimum count is met.
 */
public class MinimumCountFilter<T> implements Filter<T>
{
    private final int minCount;

    public MinimumCountFilter( int minCount )
    {
        this.minCount = minCount;
    }

    @Override
    public Set<T> apply( Set<T> data )
    {
        return data.size() >= minCount ? data : emptySet();
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        MinimumCountFilter<?> that = (MinimumCountFilter<?>) o;
        return minCount == that.minCount;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( minCount );
    }

    @Override
    public String toString()
    {
        return "MinimumCountFilter{" +
               "minCount=" + minCount +
               '}';
    }
}
