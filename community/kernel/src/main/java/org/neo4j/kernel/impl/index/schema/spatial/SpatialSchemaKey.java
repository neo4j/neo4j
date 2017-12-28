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
package org.neo4j.kernel.impl.index.schema.spatial;

import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.NumberValue;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static java.lang.String.format;

/**
 * Includes value and entity id (to be able to handle non-unique values).
 * A value can be any {@link PointValue} and is represented as a {@code long} to store the 1D mapped version
 * and a type describing the geomtry type and number of dimensions.
 */
class SpatialSchemaKey
{
    static final int SIZE =
            Byte.BYTES + /* type of value */
            Long.BYTES + /* raw value bits */

            // TODO this could use 6 bytes instead and have the highest 2 bits stored in the type byte
            Long.BYTES;  /* entityId */

    byte type;
    long rawValueBits;
    public long entityId;

    /**
     * Marks that comparisons with this key requires also comparing entityId, this allows functionality
     * of inclusive/exclusive bounds of range queries.
     * This is because {@link GBPTree} only support from inclusive and to exclusive.
     * <p>
     * Note that {@code entityIdIsSpecialTieBreaker} is only an in memory state.
     */
    boolean entityIdIsSpecialTieBreaker;

    void from( long entityId, Value... values )
    {
        writePoint( assertValidSingleGeometry( values ) );
        this.entityId = entityId;
        entityIdIsSpecialTieBreaker = false;
    }

    private static PointValue assertValidSingleGeometry( Value... values )
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
        if ( !Values.isGeometryValue( values[0] ) )
        {
            throw new IllegalArgumentException(
                    "Key layout does only support geometries, tried to create key from " + values[0] );
        }
        return (PointValue) values[0];
    }

    String propertiesAsString()
    {
        return asValue().toString();
    }

    NumberValue asValue()
    {
        // throw new UnsupportedOperationException( "Cannot extract value from spatial index" );
        return (NumberValue) Values.of( Double.longBitsToDouble( rawValueBits ) );
    }

    void initAsLowest()
    {
        writePoint( Values.pointValue( CoordinateReferenceSystem.WGS84, Double.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY ) );
        entityId = Long.MIN_VALUE;
        entityIdIsSpecialTieBreaker = true;
    }

    void initAsHighest()
    {
        writePoint( Values.pointValue( CoordinateReferenceSystem.WGS84, Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY ) );
        entityId = Long.MAX_VALUE;
        entityIdIsSpecialTieBreaker = true;
    }

    /**
     * Compares the value of this key to that of another key.
     * This method is expected to be called in scenarios where inconsistent reads may happen (and later retried).
     *
     * @param other the {@link SpatialSchemaKey} to compare to.
     * @return comparison against the {@code other} {@link SpatialSchemaKey}.
     */
    int compareValueTo( SpatialSchemaKey other )
    {
        return Long.compare( rawValueBits, other.rawValueBits );
    }

    /**
     * Extracts raw bits and type from a {@link PointValue} and store as state of this {@link SpatialSchemaKey} instance.
     *
     * @param value actual {@link PointValue} value.
     */
    private void writePoint( PointValue value )
    {
        //TODO: Support 2D to 1D mapper like space filling curves
        type = (byte)value.coordinate().length;
        rawValueBits = Double.doubleToLongBits( value.coordinate()[0] );
    }

    @Override
    public String toString()
    {
        return format( "type=%d,rawValue=%d,value=%s,entityId=%d", type, rawValueBits, "unknown", entityId );
    }
}
