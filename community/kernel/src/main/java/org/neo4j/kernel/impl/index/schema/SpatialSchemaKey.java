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

import java.util.Arrays;

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
class SpatialSchemaKey implements NativeSchemaKey
{
    static final int SIZE =
            Byte.BYTES + /* type of value */
            Long.BYTES + /* raw value bits */

            // TODO this could use 6 bytes instead and have the highest 2 bits stored in the type byte
            Long.BYTES;  /* entityId */

    private long entityId;
    private boolean entityIdIsSpecialTieBreaker;

    byte type;
    long rawValueBits;

    @Override
    public void setEntityIdIsSpecialTieBreaker( boolean entityIdIsSpecialTieBreaker )
    {
        this.entityIdIsSpecialTieBreaker = entityIdIsSpecialTieBreaker;
    }

    @Override
    public boolean getEntityIdIsSpecialTieBreaker()
    {
        return entityIdIsSpecialTieBreaker;
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
    }

    @Override
    public void from( long entityId, Value... values )
    {
        extractRawBitsAndType( assertValidValue( values ) );
        this.entityId = entityId;
        entityIdIsSpecialTieBreaker = false;
    }

    @Override
    public String propertiesAsString()
    {
        return asValue().toString();
    }

    @Override
    public NumberValue asValue()
    {
        // throw new UnsupportedOperationException( "Cannot extract value from spatial index" );
        return (NumberValue) Values.of( Double.longBitsToDouble( rawValueBits ) );
    }

    @Override
    public void initAsLowest()
    {
        double[] limit = new double[2];
        Arrays.fill(limit, Double.NEGATIVE_INFINITY);
        writePoint( CoordinateReferenceSystem.WGS84, limit );
        entityId = Long.MIN_VALUE;
        entityIdIsSpecialTieBreaker = true;
    }

    @Override
    public void initAsHighest()
    {
        double[] limit = new double[2];
        Arrays.fill(limit, Double.POSITIVE_INFINITY);
        writePoint( CoordinateReferenceSystem.WGS84, limit );
        entityId = Long.MAX_VALUE;
        entityIdIsSpecialTieBreaker = true;
    }

    // TODO this is incorrect! only adding to handle rebase
    int compareValueTo( SpatialSchemaKey other )
    {
        return RawBits.compare( rawValueBits, type, other.rawValueBits, other.type );
    }

    private PointValue assertValidValue( Value... values )
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

    // TODO this should change
    private void extractRawBitsAndType( PointValue value )
    {
        writePoint( value.getCoordinateReferenceSystem(), value.coordinate() );
    }

    /**
     * Extracts raw bits and type from a {@link PointValue} and store as state of this {@link SpatialSchemaKey} instance.
     */
    private void writePoint( CoordinateReferenceSystem crs, double[] coordinate )
    {
        //TODO: Support 2D to 1D mapper like space filling curves
        type = (byte)coordinate.length;
        rawValueBits = Double.doubleToLongBits( coordinate[0] );
    }

    @Override
    public String toString()
    {
        return format( "type=%d,rawValue=%d,value=%s,entityId=%d", type, rawValueBits, "unknown", entityId );
    }
}
