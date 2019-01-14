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

import java.util.Iterator;
import java.util.NoSuchElementException;

import org.neo4j.values.AnyValue;
import org.neo4j.values.SequenceValue;

/**
 * Array of one of the storable primitives
 */
public abstract class ArrayValue extends Value implements SequenceValue
{
    @Override
    public abstract int length();

    @Override
    public IterationPreference iterationPreference()
    {
        return IterationPreference.RANDOM_ACCESS;
    }

    @Override
    public Iterator<AnyValue> iterator()
    {
        return new Iterator<AnyValue>()
        {
            private int offset;

            @Override
            public boolean hasNext()
            {
                return offset < length();
            }

            @Override
            public AnyValue next()
            {
                if ( !hasNext() )
                {
                    throw new NoSuchElementException();
                }
                return value( offset++ );
            }
        };
    }

    @Override
    public final boolean eq( Object other )
    {
        if ( other == null )
        {
            return false;
        }

        return other instanceof SequenceValue && this.equals( (SequenceValue) other );
    }

    @Override
    public final boolean equals( boolean x )
    {
        return false;
    }

    @Override
    public final boolean equals( long x )
    {
        return false;
    }

    @Override
    public final boolean equals( double x )
    {
        return false;
    }

    @Override
    public final boolean equals( char x )
    {
        return false;
    }

    @Override
    public final boolean equals( String x )
    {
        return false;
    }

    @Override
    public boolean isSequenceValue()
    {
        return true;
    }
}
