/*
 * Copyright (c) 2002-2018 "Neo4j,"
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

import org.neo4j.gis.spatial.index.curves.SpaceFillingCurve;
import org.neo4j.kernel.impl.index.schema.config.SpaceFillingCurveSettingsFactory;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.NumberValue;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static java.lang.String.format;

/**
 * Includes value and entity id (to be able to handle non-unique values).
 * A value can be any {@link PointValue} and is represented as a {@code long} to store the 1D mapped version
 */
class SpatialSchemaKey extends NativeSchemaKey<SpatialSchemaKey>
{
    static final int SIZE =
            Integer.BYTES + /* table id */
            Integer.BYTES + /* code */
            Long.BYTES + /* raw value bits */
            Long.BYTES;  /* entityId */

    final SpaceFillingCurveSettingsFactory curveFactory;

    transient boolean highest;
    transient boolean lowest;

    CoordinateReferenceSystem crs;
    long rawValueBits;
    SpaceFillingCurve curve;

    SpatialSchemaKey( SpaceFillingCurveSettingsFactory curveFactory )
    {
        this.curveFactory = curveFactory;
    }

    @Override
    public NumberValue asValue()
    {
        // This is used in the index sampler to estimate value diversity. Since the spatial index does not store values
        // the uniqueness of the space filling curve number is the best estimate. This can become a bad estimate for
        // indexes with badly defined Envelopes for the space filling curves, such that many points exist within the
        // same tile.
        return (NumberValue) Values.of( rawValueBits );
    }

    @Override
    void initialize( long entityId )
    {
        super.initialize( entityId );
        highest = false;
        lowest = false;
    }

    @Override
    void initValueAsLowest()
    {
        lowest = true;
    }

    @Override
    void initValueAsHighest()
    {
        highest = true;
    }

    void fromDerivedValue( long entityId, long derivedValue, CoordinateReferenceSystem crs )
    {
        initialize( entityId );
        this.crs = crs;
        rawValueBits = derivedValue;
    }

    /**
     * This method will compare along the curve, which is not a spatial comparison, but is correct
     * for comparison within the space filling index as long as the original spatial range has already
     * been decomposed into a collection of 1D curve ranges before calling down into the GPTree.
     */
    int compareValueTo( SpatialSchemaKey other )
    {
        if ( lowest || other.lowest )
        {
            return Boolean.compare( other.lowest, lowest );
        }
        if ( highest || other.highest )
        {
            return Boolean.compare( highest, other.highest );
        }

        int tableComparison = Integer.compare( crs.getTable().getTableId(), other.crs.getTable().getTableId() );
        if ( tableComparison != 0 )
        {
            return tableComparison;
        }
        int codeComparison = Integer.compare( crs.getCode(), other.crs.getCode() );
        if ( codeComparison != 0 )
        {
            return codeComparison;
        }

        return Long.compare( rawValueBits, other.rawValueBits );
    }

    @Override
    protected Value assertCorrectType( Value value )
    {
        if ( !Values.isGeometryValue( value ) )
        {
            throw new IllegalArgumentException(
                    "Key layout does only support geometries, tried to create key from " + value );
        }
        return value;
    }

    /**
     * Extracts raw bits from a {@link PointValue} and store as state of this {@link SpatialSchemaKey} instance.
     */
    @Override
    public void writePoint( CoordinateReferenceSystem crs, double[] coordinate )
    {
        updateCurve( crs );
        this.crs = crs;
        rawValueBits = curve.derivedValueFor( coordinate );
    }

    private void updateCurve( CoordinateReferenceSystem crs )
    {
        if ( this.crs == null || this.crs != crs )
        {
            this.curve = curveFactory.settingsFor( crs ).curve();
        }
    }

    @Override
    public String toString()
    {
        return format( "rawValue=%d,value=%s,entityId=%d", rawValueBits, "unknown", getEntityId() );
    }
}
