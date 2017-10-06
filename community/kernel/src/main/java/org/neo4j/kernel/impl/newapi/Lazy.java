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
package org.neo4j.kernel.impl.newapi;

import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

/**
 * Encapsulates a double checked locking pattern for lazy initialization.
 * TODO: move this to some util place or something.
 */
public final class Lazy<T>
{
    private T cache;
    private volatile Object state;

    public Lazy( Supplier<T> supplier )
    {
        this.state = requireNonNull( supplier, "supplier" );
    }

    public T get()
    {
        T value = cache;
        if ( value == null )
        {
            Object s = state;
            if ( s instanceof Supplier<?> )
            {
                synchronized ( this )
                {
                    s = state;
                    if ( s instanceof Supplier<?> )
                    {
                        @SuppressWarnings( "unchecked" )
                        Supplier<T> supplier = (Supplier<T>) s;
                        s = supplier.get();
                    }
                }
            }
            @SuppressWarnings( "unchecked" )
            T val = (T) s;
            cache = value = val;
        }
        return value;
    }
}
