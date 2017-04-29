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

import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Filters the set through each filter of the chain in order.
 */
public class FilterChain<T> implements Filter<T>
{
    private List<Filter<T>> chain;

    public FilterChain( List<Filter<T>> chain )
    {
        this.chain = chain;
    }

    @Override
    public Set<T> apply( Set<T> data )
    {
        for ( Filter<T> filter : chain )
        {
            data = filter.apply( data );
        }
        return data;
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
        FilterChain<?> that = (FilterChain<?>) o;
        return Objects.equals( chain, that.chain );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( chain );
    }

    @Override
    public String toString()
    {
        return "FilterChain{" +
               "chain=" + chain +
               '}';
    }
}
