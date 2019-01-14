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
package org.neo4j.kernel.impl.api;

import org.neo4j.values.storable.Values;

/**
 * This class is only needed because of a missed dependency in Cypher 2.3 and 3.1.
 * It can be removed in 4.0.
 */
@SuppressWarnings( "unused" )
@Deprecated
public class PropertyValueComparison
{
    private PropertyValueComparison()
    {
        throw new AssertionError( "no instance" );
    }

    public static final PropertyValueComparator<Object> COMPARE_VALUES = new PropertyValueComparator<Object>()
    {
        @Override
        public int compare( Object o1, Object o2 )
        {
            return Values.COMPARATOR.compare( Values.of(o1), Values.of(o2) );
        }
    };

    public static final PropertyValueComparator<Number> COMPARE_NUMBERS = new PropertyValueComparator<Number>()
    {
        @Override
        public int compare( Number n1, Number n2 )
        {
            return Values.COMPARATOR.compare( Values.numberValue(n1), Values.numberValue(n2) );
        }
    };

    public static final PropertyValueComparator<Object> COMPARE_STRINGS = new PropertyValueComparator<Object>()
    {
        @Override
        public int compare( Object o1, Object o2 )
        {
            return Values.COMPARATOR.compare( Values.of(o1), Values.of(o2) );
        }
    };
}
