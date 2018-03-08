/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.impl.index.schema;

import org.neo4j.values.storable.NumberValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueWriter;
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
class NumberSchemaKey extends ValueWriter.Adapter<RuntimeException> implements NativeSchemaKey
{
    static final int SIZE =
            Byte.BYTES + /* type of value */
            Long.BYTES + /* raw value bits */

            // TODO this could use 6 bytes instead and have the highest 2 bits stored in the type byte
            Long.BYTES;  /* entityId */

    private long entityId;
    private boolean compareId = true;

    byte type;
    long rawValueBits;

    public void setCompareId( boolean compareId )
    {
        this.compareId = compareId;
    }

    public boolean getCompareId()
    {
        return compareId;
    }

    @Override
    public long getEntityId()
    {
        return entityId;
    }

    @Override
    public void setEntityId( long entityId )
    {
        this.entityId = entityId;
        compareId = true;
    }

    @Override
    public void from( long entityId, Value... values )
    {
        extractRawBitsAndType( assertValidValue( values ) );
        this.entityId = entityId;
        compareId = true;
    }

    private NumberValue assertValidValue( Value... values )
    {
        // TODO: support multiple values, right?
        if ( values.length > 1 )
        {
            throw new IllegalArgumentException( "Tried to create composite key with non-composite schema key layout" );
        }
        if ( values.length < 1 )
        {
            throw new IllegalArgumentException( "Tried to create key without value" );
        }
        if ( !Values.isNumberValue( values[0] ) )
        {
            throw new IllegalArgumentException(
                    "Key layout does only support numbers, tried to create key from " + values[0] );
        }
        return (NumberValue) values[0];
    }

    @Override
    public String propertiesAsString()
    {
        return asValue().toString();
    }

    @Override
    public NumberValue asValue()
    {
        return RawBits.asNumberValue( rawValueBits, type );
    }

    @Override
    public void initAsLowest()
    {
        writeFloatingPoint( Double.NEGATIVE_INFINITY );
        entityId = Long.MIN_VALUE;
        compareId = true;
    }

    @Override
    public void initAsHighest()
    {
        writeFloatingPoint( Double.POSITIVE_INFINITY );
        entityId = Long.MAX_VALUE;
        compareId = true;
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

    /**
     * Extracts raw bits and type from a {@link NumberValue} and store as state of this {@link NumberSchemaKey} instance.
     *
     * @param value actual {@link NumberValue} value.
     */
    private void extractRawBitsAndType( NumberValue value )
    {
        value.writeTo( this );
    }

    @Override
    public String toString()
    {
        return format( "type=%d,rawValue=%d,value=%s,entityId=%d", type, rawValueBits, asValue(), entityId );
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
