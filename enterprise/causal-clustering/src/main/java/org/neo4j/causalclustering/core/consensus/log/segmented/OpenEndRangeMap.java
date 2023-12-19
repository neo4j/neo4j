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
package org.neo4j.causalclustering.core.consensus.log.segmented;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;

/**
 * Keeps a map of sequential ranges to values, where the highest range is open (right-side unbounded).
 *
 * Typical example:
 * [ 0,  10] -> 1     (closed range)
 * [11, 300] -> 2     (closed range)
 * [301,   [ -> 3     (open range)
 *
 * An added range always replaces a part of existing ranges, which could either be only a part of
 * the open range or even the entire open range and parts of the closed ranges.
 *
 * @param <K> Type of keys which must be comparable.
 * @param <V> Type of values stored.
 */
class OpenEndRangeMap<K extends Comparable<K>, V>
{
    static class ValueRange<K,V>
    {
        private final K limit;
        private final V value;

        ValueRange( K limit, V value )
        {
            this.limit = limit;
            this.value = value;
        }

        Optional<K> limit()
        {
            return Optional.ofNullable( limit );
        }

        Optional<V> value()
        {
            return Optional.ofNullable( value );
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
            ValueRange<?,?> that = (ValueRange<?,?>) o;
            return Objects.equals( limit, that.limit ) &&
                   Objects.equals( value, that.value );
        }

        @Override
        public int hashCode()
        {
            return Objects.hash( limit, value );
        }
    }

    private final TreeMap<K,V> tree = new TreeMap<>();

    /* We optimize by keeping the open end range directly accessible. */
    private K endKey;
    private V endValue;

    Collection<V> replaceFrom( K from, V value )
    {
        Collection<V> removed = new ArrayList<>();

        Iterator<V> itr = tree.tailMap( from ).values().iterator();
        while ( itr.hasNext() )
        {
            removed.add( itr.next() );
            itr.remove();
        }

        tree.put( from, value );

        endKey = from;
        endValue = value;

        return removed;
    }

    ValueRange<K,V> lookup( K at )
    {
        if ( endKey != null && endKey.compareTo( at ) <= 0 )
        {
            return new ValueRange<>( null, endValue );
        }

        Map.Entry<K,V> entry = tree.floorEntry( at );
        return new ValueRange<>( tree.higherKey( at ), entry != null ? entry.getValue() : null );
    }

    public V last()
    {
        return endValue;
    }

    public Set<Map.Entry<K,V>> entrySet()
    {
        return tree.entrySet();
    }

    public Collection<V> remove( K lessThan )
    {
        Collection<V> removed = new ArrayList<>();
        K floor = tree.floorKey( lessThan );

        Iterator<V> itr = tree.headMap( floor, false ).values().iterator();
        while ( itr.hasNext() )
        {
            removed.add( itr.next() );
            itr.remove();
        }

        if ( tree.isEmpty() )
        {
            endKey = null;
            endValue = null;
        }

        return removed;
    }
}
