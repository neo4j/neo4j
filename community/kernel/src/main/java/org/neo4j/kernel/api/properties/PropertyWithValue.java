/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.api.properties;

/** Base class for properties that have a value. */
abstract class PropertyWithValue extends Property
{
    @Override
    public abstract Object value();

    @Override
    public Object value( Object defaultValue )
    {
        return value();
    }

    @Override
    public boolean valueEquals( Object value )
    {
        return value().equals( value ); // TODO: specialize sub-classes to avoid the boxing performed by value()
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + "[propertyKeyId=" + propertyKeyId() + ", value=" + valueToString() + "]";
    }

    String valueToString()
    {
        return value().toString();
    }

    @Override
    public String stringValue()
    {
        Object value = value();
        throw new ClassCastException(
                String.format( "[%s:%s] is not a String", value, value.getClass().getSimpleName() ) );
    }

    @Override
    public String stringValue( String defaultValue )
    {
        return stringValue();
    }

    @Override
    public Number numberValue()
    {
        Object value = value();
        throw new ClassCastException(
                String.format( "[%s:%s] is not a Number", value, value.getClass().getSimpleName() ) );
    }

    @Override
    public Number numberValue( Number defaultValue )
    {
        return numberValue();
    }

    @Override
    public int intValue()
    {
        Object value = value();
        throw new ClassCastException(
                String.format( "[%s:%s] is not an int", value, value.getClass().getSimpleName() ) );
    }

    @Override
    public int intValue( int defaultValue )
    {
        return intValue();
    }

    @Override
    public long longValue()
    {
        Object value = value();
        throw new ClassCastException(
                String.format( "[%s:%s] is not a long", value, value.getClass().getSimpleName() ) );
    }

    @Override
    public long longValue( long defaultValue )
    {
        return longValue();
    }

    @Override
    public boolean booleanValue()
    {
        Object value = value();
        throw new ClassCastException(
                String.format( "[%s:%s] is not a boolean", value, value.getClass().getSimpleName() ) );
    }

    @Override
    public boolean booleanValue( boolean defaultValue )
    {
        return booleanValue();
    }
}
