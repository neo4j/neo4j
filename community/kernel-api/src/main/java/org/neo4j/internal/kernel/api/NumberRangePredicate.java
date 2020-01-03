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
package org.neo4j.internal.kernel.api;

import org.neo4j.values.storable.NumberValue;
import org.neo4j.values.storable.ValueGroup;
import org.neo4j.values.storable.Values;

public final class NumberRangePredicate extends IndexQuery.RangePredicate<NumberValue>
{
    private NumberRangePredicate( int propertyKeyId, NumberValue from, boolean fromInclusive, NumberValue to,
            boolean toInclusive )
    {
        super( propertyKeyId, ValueGroup.NUMBER, from, fromInclusive, to, toInclusive );
    }

    static org.neo4j.internal.kernel.api.NumberRangePredicate create( int propertyKeyId, NumberValue from, boolean fromInclusive, NumberValue to,
                                        boolean toInclusive )
    {
        // For range queries with numbers we need to redefine the upper bound from NaN to positive infinity.
        // The reason is that we do not want to find NaNs for seeks, but for full scans we do.
        if ( to == null )
        {
            to = Values.doubleValue( Double.POSITIVE_INFINITY );
            toInclusive = true;
        }
        return new org.neo4j.internal.kernel.api.NumberRangePredicate( propertyKeyId, from, fromInclusive, to, toInclusive );
    }

    public Number from()
    {
        return from == null ? null : from.asObject();
    }

    public Number to()
    {
        return to == null ? null : to.asObject();
    }
}
