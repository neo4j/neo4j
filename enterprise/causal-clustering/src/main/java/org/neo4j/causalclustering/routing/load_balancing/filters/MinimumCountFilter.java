/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.causalclustering.routing.load_balancing.filters;

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
