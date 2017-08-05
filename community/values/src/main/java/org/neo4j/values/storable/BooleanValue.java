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
public final class BooleanValue extends ScalarValue
{
    private final boolean value;

    BooleanValue( boolean value )
    {
        this.value = value;
    }

    @Override
    public boolean eq( Object other )
    {
        return other != null && other instanceof Value && equals( (Value) other );
    }

    @Override
    public boolean equals( Value other )
    {
        return other.equals( value );
    }

    @Override
    public boolean equals( boolean x )
    {
        return value == x;
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

    @Override
    public int computeHash()
    {
        return value ? -1 : 0;
    }

    public boolean booleanValue()
    {
        return value;
    }

    public int compareTo( BooleanValue other )
    {
        return Boolean.compare( value, other.booleanValue() );
    }

    @Override
    public <E extends Exception> void writeTo( ValueWriter<E> writer ) throws E
    {
        writer.writeBoolean( value );
    }

    @Override
    public Object asObjectCopy()
    {
        return value;
    }

    @Override
    public String prettyPrint()
    {
        return Boolean.toString( value );
    }

    @Override
    public String toString()
    {
        return format( "Boolean('%s')", Boolean.toString( value ) );
    }

    public ValueGroup valueGroup()
    {
        return ValueGroup.BOOLEAN;
    }

    @Override
    public NumberType numberType()
    {
        return NumberType.NO_NUMBER;
    }
}
