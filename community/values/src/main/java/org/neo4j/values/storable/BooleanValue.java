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
package org.neo4j.values.storable;

import static java.lang.String.format;

/**
 * This does not extend AbstractProperty since the JVM can take advantage of the 4 byte initial field alignment if
 * we don't extend a class that has fields.
 */
public abstract class BooleanValue extends ScalarValue
{

    private BooleanValue( )
    {
    }

    @Override
    public boolean eq( Object other )
    {
        return other != null && other instanceof Value && equals( (Value) other );
    }

    public ValueGroup valueGroup()
    {
        return ValueGroup.BOOLEAN;
    }

    @Override
    public boolean equals( long x )
    {
        return false;
    }

    @Override
    public boolean equals( double x )
    {
        return false;
    }

    @Override
    public boolean equals( char x )
    {
        return false;
    }

    @Override
    public boolean equals( String x )
    {
        return false;
    }

    public abstract boolean booleanValue();

    public abstract int compareTo( BooleanValue other );

    @Override
    public NumberType numberType()
    {
        return NumberType.NO_NUMBER;
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
            //Use same as Boolean.TRUE.hashCode
            return 1231;
        }

        public boolean booleanValue()
        {
            return true;
        }

        public int compareTo( BooleanValue other )
        {
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
            return format( "Boolean('%s')", Boolean.toString( true ) );
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
            //Use same as Boolean.FALSE.hashCode
            return 1237;
        }

        public boolean booleanValue()
        {
            return false;
        }

        public int compareTo( BooleanValue other )
        {
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
            return format( "Boolean('%s')", Boolean.toString( false ) );
        }

    };
}
