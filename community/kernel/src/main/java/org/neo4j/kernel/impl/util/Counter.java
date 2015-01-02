/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.impl.util;

import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.function.Factory;

/**
 * Subset of methods on {@link AtomicLong} to be able to inject more testable behavior into tests.
 */
public interface Counter
{
    public static final Factory<Counter> ATOMIC_LONG = new Factory<Counter>()
    {
        @Override
        public Counter newInstance()
        {
            return new Counter()
            {
                private final AtomicLong actual = new AtomicLong();

                @Override
                public void set( long value )
                {
                    actual.set( value );
                }

                @Override
                public long incrementAndGet()
                {
                    return actual.incrementAndGet();
                }

                @Override
                public long get()
                {
                    return actual.get();
                }

                @Override
                public String toString()
                {
                    return actual.toString();
                }
            };
        }
    };

    long incrementAndGet();

    long get();

    void set( long value );
}
