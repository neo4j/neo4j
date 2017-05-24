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
package org.neo4j.values;

import java.util.concurrent.Callable;

import org.neo4j.values.Values.ValueLoader;

abstract class LazyValue<T> extends Value
{
    private volatile Object value;

    LazyValue( ValueLoader<? extends T> producer )
    {
        this.value = producer;
    }

    /**
     * Get or loads the value.
     */
    final T getOrLoad()
    {
        Object maybeValue = this.value;
        if ( maybeValue instanceof ValueLoader<?> )
        {
            synchronized ( this )
            {
                maybeValue = this.value;
                if ( maybeValue instanceof ValueLoader<?> )
                {
                    this.value = produceValue();
                }
            }
        }
        //noinspection unchecked
        return (T) this.value;
    }

    private Object produceValue()
    {
        return ((ValueLoader<?>) value).load();
    }

    boolean valueIsLoaded()
    {
        return !(value instanceof Callable<?>);
    }
}
