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
package org.neo4j.values.storable;

import org.neo4j.hashing.HashFunction;
import org.neo4j.values.ValueMapper;

import static java.lang.String.format;

public abstract class BooleanValue extends ScalarValue
{

    private BooleanValue()
    {
    }

    @Override
    public boolean equalTo( Object other )
    {
        return other instanceof Value && equals( (Value) other );
    }

    @Override
    public <T> T map( ValueMapper<T> mapper )
    {
        return mapper.mapBoolean( this );
    }

    @Override
    public ValueGroup valueGroup()
    {
        return ValueGroup.BOOLEAN;
    }

    public abstract boolean booleanValue();

    @Override
    public NumberType numberType()
    {
        return NumberType.NO_NUMBER;
    }

    @Override
    public long updateHash( HashFunction hashFunction, long hash )
    {
        return hashFunction.update( hash, hashCode() );
    }

    @Override
    public String getTypeName()
    {
        return "Boolean";
    }

    @Override
    public long estimatedHeapUsage()
    {
        return 0L;
    }

    public static final BooleanValue TRUE = new BooleanValue()
    {
        @Override
        public boolean equals( Value other )
        {
            return this == other;
        }

        @Override
        public boolean equals( boolean x )
        {
            return x;
        }

        @Override
        public int computeHash()
        {
            return Boolean.hashCode( true );
        }

        @Override
        public boolean booleanValue()
        {
            return true;
        }

        @Override
        int unsafeCompareTo( Value otherValue )
        {
            BooleanValue other = (BooleanValue) otherValue;
            return other.booleanValue() ? 0 : 1;
        }

        @Override
        public <E extends Exception> void writeTo( ValueWriter<E> writer ) throws E
        {
            writer.writeBoolean( true );
        }

        @Override
        public Object asObjectCopy()
        {
            return Boolean.TRUE;
        }

        @Override
        public String prettyPrint()
        {
            return Boolean.toString( true );
        }

        @Override
        public String toString()
        {
            return format( "%s('%s')", getTypeName(), true );
        }
    };

    public static final BooleanValue FALSE = new BooleanValue()
    {
        @Override
        public boolean equals( Value other )
        {
            return this == other;
        }

        @Override
        public boolean equals( boolean x )
        {
            return !x;
        }

        @Override
        public int computeHash()
        {
            return Boolean.hashCode( false );
        }

        @Override
        public boolean booleanValue()
        {
            return false;
        }

        @Override
        int unsafeCompareTo( Value otherValue )
        {
            BooleanValue other = (BooleanValue) otherValue;
            return !other.booleanValue() ? 0 : -1;
        }

        @Override
        public <E extends Exception> void writeTo( ValueWriter<E> writer ) throws E
        {
            writer.writeBoolean( false );
        }

        @Override
        public Object asObjectCopy()
        {
            return Boolean.FALSE;
        }

        @Override
        public String prettyPrint()
        {
            return Boolean.toString( false );
        }

        @Override
        public String toString()
        {
            return format( "%s('%s')", getTypeName(), false );
        }
    };
}
