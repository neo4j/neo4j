/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.internal.values;

class LazyValues
{
    /**
     * Get or loads the value.
     */
    static <T> T getOrLoad( LazyValue<T> value )
    {
        Object maybeValue = value.getMaybeValue();
        if ( maybeValue instanceof Values.ValueLoader<?> )
        {
            synchronized ( value )
            {
                maybeValue = value.getMaybeValue();
                if ( maybeValue instanceof Values.ValueLoader<?> )
                {
                    maybeValue = ((Values.ValueLoader<?>) maybeValue).load();
                    value.registerValue( (T) maybeValue );
                }
            }
        }
        //noinspection unchecked
        return (T) maybeValue;
    }

    static boolean valueIsLoaded( Object maybeValue )
    {
        return !(maybeValue instanceof Values.ValueLoader<?>);
    }
}
