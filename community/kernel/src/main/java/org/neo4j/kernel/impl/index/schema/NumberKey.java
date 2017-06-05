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

import org.neo4j.values.Value;
import org.neo4j.index.internal.gbptree.GBPTree;

import static org.neo4j.kernel.impl.index.schema.NumberValueConversion.assertValidSingleNumber;

/**
 * Includes value and entity id (to be able to handle non-unique values).
 * A value can be any {@link Number} and is represented as a {@code long} to store the raw bits and a type
 * to say if it's a long, double or float.
 *
 * Distinction between double and float exists because coersions between each other and long may differ.
 * TODO this should be figured out and potentially reduced to long, double types only.
 */
class NumberKey
{
    static final int SIZE =
            Byte.BYTES + /* type of value */
            Long.BYTES + /* raw value bits */

            // TODO this could use 6 bytes instead and have the highest 2 bits stored in the type byte
            Long.BYTES;  /* entityId */

    static final byte TYPE_LONG = 0;
    static final byte TYPE_FLOAT = 1;
    static final byte TYPE_DOUBLE = 2;

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

    String propertiesAsString()
    {
        return String.valueOf( toNumberValue() );
    }

    void initAsLowest()
    {
        rawValueBits = Double.doubleToLongBits( Double.NEGATIVE_INFINITY );
        type = TYPE_DOUBLE;
        entityId = Long.MIN_VALUE;
        entityIdIsSpecialTieBreaker = true;
    }

    void initAsHighest()
    {
        rawValueBits = Double.doubleToLongBits( Double.POSITIVE_INFINITY );
        type = TYPE_DOUBLE;
        entityId = Long.MAX_VALUE;
        entityIdIsSpecialTieBreaker = true;
    }

    /**
     * Compares the value of this key to that of another key.
     * This method is expected to be called in scenarios where inconsistent reads may happen (and later retried).
     *
     * @param other the {@link NumberKey} to compare to.
     * @return comparison against the {@code other} {@link NumberKey}.
     */
    int compareValueTo( NumberKey other )
    {
        return type == TYPE_LONG && other.type == TYPE_LONG
                // If both are long values then compare them directly, w/o going through double.
                // This is because at high values longs have higher precision, or double lower rather,
                // than double values, so converting them to doubles and comparing would have false positives.
                ? Long.compare( rawValueBits, other.rawValueBits )

                // Otherwise convert both to double and compare, with the reasoning that the long precision
                // cannot be upheld anyway and double precious being higher than float precision.
                : Double.compare( doubleValue(), other.doubleValue() );
    }

    /**
     * @return the value as double, with potential precision loss.
     */
    private double doubleValue()
    {
        switch ( type )
        {
        case TYPE_LONG:
            return rawValueBits;
        case TYPE_FLOAT:
            return Float.intBitsToFloat( (int) rawValueBits );
        case TYPE_DOUBLE:
            return Double.longBitsToDouble( rawValueBits );
        default:
            // This is interesting: because of the nature of the page cache and the point in time this method
            // is called we cannot really throw exception here if type is something unexpected - it may simply
            // have been an inconsistent read, which will be retried.
            // It's not for us to decide here, so let's return NaN here.
            return Double.NaN;
        }
    }

    /**
     * Extracts data from a {@link Number} into state of this {@link NumberKey} instance.
     *
     * @param value actual {@link Number} value.
     */
    private void extractValue( Number value )
    {
        if ( value instanceof Double )
        {
            type = TYPE_DOUBLE;
            rawValueBits = Double.doubleToLongBits( (Double) value );
        }
        else if ( value instanceof Float )
        {
            type = TYPE_FLOAT;
            rawValueBits = Float.floatToIntBits( (Float) value );
        }
        else
        {
            type = TYPE_LONG;
            rawValueBits = value.longValue();
        }
    }

    /**
     * Useful for getting the value as {@link Number} for e.g. printing or converting to {@link String}.
     * This method isn't and should not be called on a hot path.
     *
     * @return a {@link Number} of correct type, i.e. {@link Long}, {@link Float} or {@link Double}.
     */
    private Number toNumberValue()
    {
        switch ( type )
        {
        case TYPE_LONG:
            return rawValueBits;
        case TYPE_FLOAT:
            return Float.intBitsToFloat( (int)rawValueBits );
        case TYPE_DOUBLE:
            return Double.longBitsToDouble( rawValueBits );
        default:
            // Unlike in compareValueTo() it is assumed here that the value have been consistently read
            // and that the value is put to some actual use.
            throw new IllegalArgumentException( "Unexpected type " + type );
        }
    }

    @Override
    public String toString()
    {
        return "type=" + type + ",rawValue=" + rawValueBits + ",value=" + toNumberValue() + ",entityId=" + entityId;
    }
}
