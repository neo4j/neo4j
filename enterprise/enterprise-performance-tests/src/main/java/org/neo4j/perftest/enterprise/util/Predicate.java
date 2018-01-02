/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.perftest.enterprise.util;

public abstract class Predicate<T>
{
    public static Predicate<Long> integerRange( final Long minValue, final Long maxValue )
    {
        return new Predicate<Long>()
        {
            @Override
            boolean matches( Long value )
            {
                return value <= maxValue && value >= minValue;
            }

            @Override
            public String toString()
            {
                return String.format( "%s, %s", minValue == null ? "]-inf" : ("[" + minValue), maxValue == null ? "inf]" : (maxValue + "]") );
            }
        };
    }

    public static Predicate<Long> integerRange( Integer minValue, Integer maxValue )
    {
        return integerRange( minValue == null ? null : minValue.longValue(),
                             maxValue == null ? null : maxValue.longValue() );
    }

    abstract boolean matches( T value );
}
