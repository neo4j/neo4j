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
package org.neo4j.kernel.impl.index.schema;

import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.values.NumberValue;
import org.neo4j.values.NumberValues;
import org.neo4j.values.PrimitiveNumberType;
import org.neo4j.values.Value;
import org.neo4j.values.ValueWriter;
import org.neo4j.values.Values;

import static java.lang.String.format;

/**
 * Includes value and entity id (to be able to handle non-unique values).
 * A value can be any {@link Number} and is represented as a {@code long} to store the raw bits and a type
 * to say if it's a long, double or float.
 *
 * Distinction between double and float exists because coersions between each other and long may differ.
 * TODO this should be figured out and potentially reduced to long, double types only.
 */
class SchemaNumberKey extends ValueWriter.Adapter
{
    static final int SIZE =
            Byte.BYTES + /* type of value */
            Long.BYTES + /* raw value bits */

            // TODO this could use 6 bytes instead and have the highest 2 bits stored in the type byte
            Long.BYTES;  /* entityId */

    byte type;
    long rawValueBits;
    long entityId;

    /**
     * Marks that comparisons with this key requires also comparing entityId, this allows functionality
     * of inclusive/exclusive bounds of range queries.
     * This is because {@link GBPTree} only support from inclusive and to exclusive.
     * <p>
     * Note that {@code entityIdIsSpecialTieBreaker} is only an in memory state.
     */
    boolean entityIdIsSpecialTieBreaker;

    void from( long entityId, Value[] values )
    {
        extractValue( assertValidSingleNumber( values ) );
        this.entityId = entityId;
        entityIdIsSpecialTieBreaker = false;
    }

    private static NumberValue assertValidSingleNumber( Value[] values )
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

    String propertiesAsString()
    {
        return PrimitiveNumberType.from( type ).valueFromRawBits( rawValueBits ).toString();
    }

    void initAsLowest()
    {
        rawValueBits = Double.doubleToLongBits( Double.NEGATIVE_INFINITY );
        type = PrimitiveNumberType.DOUBLE.byteRepresentation();
        entityId = Long.MIN_VALUE;
        entityIdIsSpecialTieBreaker = true;
    }

    void initAsHighest()
    {
        rawValueBits = Double.doubleToLongBits( Double.POSITIVE_INFINITY );
        type = PrimitiveNumberType.DOUBLE.byteRepresentation();
        entityId = Long.MAX_VALUE;
        entityIdIsSpecialTieBreaker = true;
    }

    /**
     * Compares the value of this key to that of another key.
     * This method is expected to be called in scenarios where inconsistent reads may happen (and later retried).
     *
     * @param other the {@link SchemaNumberKey} to compare to.
     * @return comparison against the {@code other} {@link SchemaNumberKey}.
     */
    int compareValueTo( SchemaNumberKey other )
    {
        return NumberValues.compare( rawValueBits, type, other.rawValueBits, other.type );
    }

    /**
     * Extracts data from a {@link NumberValue} into state of this {@link SchemaNumberKey} instance.
     *
     * @param value actual {@link NumberValue} value.
     */
    private void extractValue( NumberValue value )
    {
        value.writeTo( this );
    }

    @Override
    public String toString()
    {
        return format( "type=%d,rawValue=%d,value=%s,entityId=%d",
                type, rawValueBits, PrimitiveNumberType.from( type ).valueFromRawBits( rawValueBits ).toString(), entityId );
    }

    @Override
    public void writeInteger( byte value )
    {
        type = PrimitiveNumberType.BYTE.byteRepresentation();
        rawValueBits = value;
    }

    @Override
    public void writeInteger( short value )
    {
        type = PrimitiveNumberType.SHORT.byteRepresentation();
        rawValueBits = value;
    }

    @Override
    public void writeInteger( int value )
    {
        type = PrimitiveNumberType.INT.byteRepresentation();
        rawValueBits = value;
    }

    @Override
    public void writeInteger( long value )
    {
        type = PrimitiveNumberType.LONG.byteRepresentation();
        rawValueBits = value;
    }

    @Override
    public void writeFloatingPoint( float value )
    {
        type = PrimitiveNumberType.FLOAT.byteRepresentation();
        rawValueBits = Float.floatToIntBits( value );
    }

    @Override
    public void writeFloatingPoint( double value )
    {
        type = PrimitiveNumberType.DOUBLE.byteRepresentation();
        rawValueBits = Double.doubleToLongBits( value );
    }
}
