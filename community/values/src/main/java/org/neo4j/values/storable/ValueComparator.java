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
package org.neo4j.values.storable;

import java.util.Comparator;

import org.neo4j.values.Comparison;
import org.neo4j.values.TernaryComparator;

/**
 * Comparator for values. Usable for sorting values, for example during index range scans.
 */
public final class ValueComparator implements Comparator<Value>, TernaryComparator<Value>
{
    private final Comparator<ValueGroup> valueGroupComparator;

    ValueComparator(
            Comparator<ValueGroup> valueGroupComparator )
    {
        this.valueGroupComparator = valueGroupComparator;
    }

    @Override
    public int compare( Value v1, Value v2 )
    {
        assert v1 != null && v2 != null : "null values are not supported, use NoValue.NO_VALUE instead";

        ValueGroup id1 = v1.valueGroup();
        ValueGroup id2 = v2.valueGroup();

        int x = valueGroupComparator.compare( id1, id2 );

        if ( x == 0 )
        {
            return v1.unsafeCompareTo( v2 );
        }
        return x;
    }

    @Override
    public Comparison ternaryCompare( Value v1, Value v2 )
    {
        assert v1 != null && v2 != null : "null values are not supported, use NoValue.NO_VALUE instead";

        ValueGroup id1 = v1.valueGroup();
        ValueGroup id2 = v2.valueGroup();

        int x = valueGroupComparator.compare( id1, id2 );

        if ( x == 0 )
        {
            return v1.unsafeTernaryCompareTo( v2 );
        }
        return Comparison.from( x );
    }

    @Override
    public boolean equals( Object obj )
    {
        return obj instanceof ValueComparator;
    }

    @Override
    public int hashCode()
    {
        return 1;
    }
}
