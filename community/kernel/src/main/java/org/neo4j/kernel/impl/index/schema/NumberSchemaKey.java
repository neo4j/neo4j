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
package org.neo4j.kernel.impl.index.schema;

import org.neo4j.values.storable.NumberValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static java.lang.String.format;

/**
 * Includes value and entity id (to be able to handle non-unique values).
 * A value can be any {@link Number} and is represented as a {@code long} to store the raw bits and a type
 * to say if it's a long, double or float.
 *
 * Distinction between double and float exists because coersions between each other and long may differ.
 * TODO this should be figured out and potentially reduced to long, double types only.
 */
class NumberSchemaKey extends NativeSchemaKey<NumberSchemaKey>
{
    static final int SIZE =
            Byte.BYTES + /* type of value */
            Long.BYTES + /* raw value bits */

            // TODO this could use 6 bytes instead and have the highest 2 bits stored in the type byte
            Long.BYTES;  /* entityId */

    byte type;
    long rawValueBits;

    @Override
    protected Value assertCorrectType( Value value )
    {
        if ( !Values.isNumberValue( value ) )
        {
            throw new IllegalArgumentException(
                    "Key layout does only support numbers, tried to create key from " + value );
        }
        return value;
    }

    @Override
    NumberValue asValue()
    {
        return RawBits.asNumberValue( rawValueBits, type );
    }

    @Override
    void initValueAsLowest()
    {
        writeFloatingPoint( Double.NEGATIVE_INFINITY );
    }

    @Override
    void initValueAsHighest()
    {
        writeFloatingPoint( Double.POSITIVE_INFINITY );
    }

    /**
     * Compares the value of this key to that of another key.
     * This method is expected to be called in scenarios where inconsistent reads may happen (and later retried).
     *
     * @param other the {@link NumberSchemaKey} to compare to.
     * @return comparison against the {@code other} {@link NumberSchemaKey}.
     */
    int compareValueTo( NumberSchemaKey other )
    {
        return RawBits.compare( rawValueBits, type, other.rawValueBits, other.type );
    }

    @Override
    public String toString()
    {
        return format( "type=%d,rawValue=%d,value=%s,entityId=%d", type, rawValueBits, asValue(), getEntityId() );
    }

    @Override
    public void writeInteger( byte value )
    {
        type = RawBits.BYTE;
        rawValueBits = value;
    }

    @Override
    public void writeInteger( short value )
    {
        type = RawBits.SHORT;
        rawValueBits = value;
    }

    @Override
    public void writeInteger( int value )
    {
        type = RawBits.INT;
        rawValueBits = value;
    }

    @Override
    public void writeInteger( long value )
    {
        type = RawBits.LONG;
        rawValueBits = value;
    }

    @Override
    public void writeFloatingPoint( float value )
    {
        type = RawBits.FLOAT;
        rawValueBits = Float.floatToIntBits( value );
    }

    @Override
    public void writeFloatingPoint( double value )
    {
        type = RawBits.DOUBLE;
        rawValueBits = Double.doubleToLongBits( value );
    }
}
