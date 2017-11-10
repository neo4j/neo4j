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
package org.neo4j.kernel.impl.util;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Counter that produce sequence of incremental numbers
 */
public interface MonotonicCounter
{
    long increment();

    /**
     * Create new counter with specified initial value
     * @param initialValue initial value
     * @return counter newly created counter
     */
    static MonotonicCounter newCounter( int initialValue )
    {
        assert initialValue >= 0;
        return new NaturalCounter( initialValue );
    }

    /**
     * Create new counter with default 0 as its initial value
     * @return counter newly created counter
     */
    static MonotonicCounter newCounter()
    {
        return new NaturalCounter( 0 );
    }

    class NaturalCounter implements MonotonicCounter
    {
        private final AtomicLong value;

        NaturalCounter( int initialValue )
        {
            value = new AtomicLong( initialValue );
        }

        @Override
        public long increment()
        {
            int initialValue;
            int incrementedValue;
            do
            {
                initialValue = value.intValue();
                incrementedValue = initialValue == Integer.MAX_VALUE ? 0 : initialValue + 1;
            }
            while ( !value.compareAndSet( initialValue, incrementedValue ) );
            return incrementedValue;
        }
    }
}
