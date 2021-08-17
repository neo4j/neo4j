/*
 * Copyright (c) "Neo4j"
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

import java.util.Arrays;
import java.util.StringJoiner;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;
import org.neo4j.values.storable.Values;

import static java.lang.Math.toIntExact;
import static java.lang.String.format;

class GeometryType extends Type
{
    // Affected key state:
    // long0 (rawValueBits)
    // long1 (coordinate reference system tableId)
    // long2 (coordinate reference system code)
    // long3 (dimensions)
    // long1Array (coordinates), use long1Array so that it doesn't clash mentally with long0Array in GeometryArrayType

    GeometryType( byte typeId )
    {
        super( ValueGroup.GEOMETRY, typeId, PointValue.MIN_VALUE, PointValue.MAX_VALUE );
    }

    @Override
    int valueSize( GenericKey<?> state )
    {
        int coordinatesSize = dimensions( state ) * PointKeyUtil.SIZE_GEOMETRY_COORDINATE;
        return PointKeyUtil.SIZE_GEOMETRY_HEADER + PointKeyUtil.SIZE_GEOMETRY_DERIVED_SPACE_FILLING_CURVE_VALUE + coordinatesSize;
    }

    static int dimensions( GenericKey<?> state )
    {
        return toIntExact( state.long3 );
    }

    @Override
    void copyValue( GenericKey<?> to, GenericKey<?> from )
    {
        to.long0 = from.long0;
        to.long1 = from.long1;
        to.long2 = from.long2;
        to.long3 = from.long3;
        int dimensions = dimensions( from );
        to.long1Array = ensureBigEnough( to.long1Array, dimensions );
        System.arraycopy( from.long1Array, 0, to.long1Array, 0, dimensions );
        to.spaceFillingCurve = from.spaceFillingCurve;
    }

    @Override
    Value asValue( GenericKey<?> state )
    {
        assertHasCoordinates( state );
        CoordinateReferenceSystem crs = CoordinateReferenceSystem.get( (int) state.long1, (int) state.long2 );
        return asValue( state, crs, 0 );
    }

    static PointValue asValue( GenericKey<?> state, CoordinateReferenceSystem crs, int offset )
    {
        double[] coordinates = new double[dimensions( state )];
        for ( int i = 0; i < coordinates.length; i++ )
        {
            coordinates[i] = Double.longBitsToDouble( state.long1Array[offset + i] );
        }
        return Values.pointValue( crs, coordinates );
    }

    @Override
    int compareValue( GenericKey<?> left, GenericKey<?> right )
    {
        return compare(
                left.long0, left.long1, left.long2, left.long3, left.long1Array, 0,
                right.long0, right.long1, right.long2, right.long3, right.long1Array, 0 );
    }

    @Override
    void putValue( PageCursor cursor, GenericKey<?> state )
    {
        putCrs( cursor, state.long1, state.long2, state.long3 );
        putPoint( cursor, state.long0, state.long3, state.long1Array, 0 );
    }

    @Override
    boolean readValue( PageCursor cursor, int size, GenericKey<?> into )
    {
        return readCrs( cursor, into ) && readPoint( cursor, into );
    }

    @Override
    String toString( GenericKey<?> state )
    {
        String asValueString = hasCoordinates( state ) ? asValue( state ).toString() : "NO_COORDINATES";
        return format( "Geometry[tableId:%d, code:%d, rawValue:%d, value:%s", state.long1, state.long2, state.long0, asValueString );
    }

    /**
     * This method will compare along the curve, which is not a spatial comparison, but is correct
     * for comparison within the space filling index as long as the original spatial range has already
     * been decomposed into a collection of 1D curve ranges before calling down into the GPTree.
     * If value on space filling curve is equal then raw comparison of serialized coordinates is done.
     * This way points are only considered equal in index if they have the same coordinates and not if
     * they only happen to occupy same value on space filling curve.
     */
    static int compare(
            long this_long0, long this_long1, long this_long2, long this_long3, long[] this_long1Array, int this_coordinates_offset,
            long that_long0, long that_long1, long that_long2, long that_long3, long[] that_long1Array, int that_coordinates_offset )
    {
        int tableIdComparison = Integer.compare( (int) this_long1, (int) that_long1 );
        if ( tableIdComparison != 0 )
        {
            return tableIdComparison;
        }

        int codeComparison = Integer.compare( (int) this_long2, (int) that_long2 );
        if ( codeComparison != 0 )
        {
            return codeComparison;
        }

        int derivedValueComparison = Long.compare( this_long0, that_long0 );
        if ( derivedValueComparison != 0 )
        {
            return derivedValueComparison;
        }

        long dimensions = Math.min( this_long3, that_long3 );
        for ( int i = 0; i < dimensions; i++ )
        {
            // It's ok to compare the coordinate value here without deserializing them
            // because we are only defining SOME deterministic order so that we can
            // correctly separate unique points from each other, even if they collide
            // on the space filling curve.
            int coordinateComparison = Long.compare( this_long1Array[this_coordinates_offset + i], that_long1Array[that_coordinates_offset + i] );
            if ( coordinateComparison != 0 )
            {
                return coordinateComparison;
            }
        }
        return 0;
    }

    static void putCrs( PageCursor cursor, long long1, long long2, long long3 )
    {
        PointKeyUtil.writeHeader( cursor, long1, long2, long3 );
    }

    static void putPoint( PageCursor cursor, long long0, long long3, long[] long1Array, int long1ArrayOffset )
    {
        cursor.putLong( long0 );
        for ( int i = 0; i < long3; i++ )
        {
            cursor.putLong( long1Array[long1ArrayOffset + i] );
        }
    }

    /**
     * This check exists because of how range queries are performed, where one range gets broken down into multiple
     * sub-ranges following a space filling curve. These sub-ranges doesn't have exact coordinates associated with them,
     * only the derived 1D comparison value. The sub-range querying is only initialized into keys acting as from/to
     * markers for a query and so should never be used for writing into the tree or generating values from,
     * so practically it's not a problem, merely an inconvenience and slight inconsistency for this value type.
     *
     * @param state holds the key state.
     */
    static void assertHasCoordinates( GenericKey<?> state )
    {
        if ( !hasCoordinates( state ) )
        {
            throw new IllegalStateException( "This geometry key doesn't have coordinates and can therefore neither be persisted nor generate point value." );
        }
    }

    static boolean hasCoordinates( GenericKey<?> state )
    {
        return state.long3 != 0 && state.long1Array != null;
    }

    static void setNoCoordinates( GenericKey<?> state )
    {
        state.long3 = 0;
    }

    static boolean readCrs( PageCursor cursor, GenericKey<?> into )
    {
        int header = PointKeyUtil.readHeader( cursor );
        into.long1 = PointKeyUtil.crsTableId(header );
        into.long2 = PointKeyUtil.crsCode( header );
        into.long3 = PointKeyUtil.dimensions(header );
        return true;
    }

    private static boolean readPoint( PageCursor cursor, GenericKey<?> into )
    {
        into.long0 = cursor.getLong();
        // into.long3 have just been read by readCrs, before this method is called
        int dimensions = dimensions( into );
        into.long1Array = ensureBigEnough( into.long1Array, dimensions );
        for ( int i = 0; i < dimensions; i++ )
        {
            into.long1Array[i] = cursor.getLong();
        }
        return true;
    }

    static void write( BtreeKey state, long derivedSpaceFillingCurveValue, double[] coordinate )
    {
        state.long0 = derivedSpaceFillingCurveValue;
        state.long1Array = ensureBigEnough( state.long1Array, coordinate.length );
        for ( int i = 0; i < coordinate.length; i++ )
        {
            state.long1Array[i] = Double.doubleToLongBits( coordinate[i] );
        }
        state.long3 = coordinate.length;
    }

    @Override
    protected void addTypeSpecificDetails( StringJoiner joiner, GenericKey<?> state )
    {
        joiner.add( "long0=" + state.long0 );
        joiner.add( "long1=" + state.long1 );
        joiner.add( "long2=" + state.long2 );
        joiner.add( "long3=" + state.long3 );
        joiner.add( "long1Array=" + Arrays.toString( state.long1Array ) );
    }
}
