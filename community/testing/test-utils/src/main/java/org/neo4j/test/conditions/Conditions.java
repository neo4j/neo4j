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
package org.neo4j.test.conditions;

import org.assertj.core.api.Condition;

import java.util.Collection;
import java.util.function.Predicate;

public final class Conditions
{
    public static final Condition<Boolean> TRUE = new Condition<>( value -> value, "Should be true." );
    public static final Condition<Boolean> FALSE = new Condition<>( value -> !value, "Should be false." );

    private Conditions()
    {
    }

    public static <T> Condition<T> condition( Predicate<T> predicate )
    {
        return new Condition<>( predicate, "Generic condition. See predicate for condition details." );
    }

    public static <T> Condition<T> equalityCondition( T value )
    {
        return new Condition<>( v -> v.equals( value ), "Should be equal to " + value );
    }

    public static <T extends Collection<?>> Condition<T> sizeCondition( int expectedSize )
    {
        return new Condition<>( v -> v.size() == expectedSize, "Size should be equal to " + expectedSize );
    }
}
