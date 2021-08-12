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

import org.junit.jupiter.api.Test;

import org.neo4j.configuration.Config;
import org.neo4j.gis.spatial.index.curves.SpaceFillingCurve;
import org.neo4j.kernel.impl.index.schema.config.IndexSpecificSpaceFillingCurveSettings;
import org.neo4j.values.AnyValues;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.PointArray;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.Values;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.kernel.impl.index.schema.NativeIndexKey.Inclusion.NEUTRAL;

class BtreeKeyStateTest extends IndexKeyStateTest<BtreeKey>
{
    private final IndexSpecificSpaceFillingCurveSettings noSpecificIndexSettings = IndexSpecificSpaceFillingCurveSettings.fromConfig( Config.defaults() );

    @Test
    void comparePointsMustOnlyReturnZeroForEqualPoints()
    {
        PointValue firstPoint = random.randomValues().nextPointValue();
        PointValue equalPoint = Values.point( firstPoint );
        CoordinateReferenceSystem crs = firstPoint.getCoordinateReferenceSystem();
        SpaceFillingCurve curve = noSpecificIndexSettings.forCrs( crs );
        Long spaceFillingCurveValue = curve.derivedValueFor( firstPoint.coordinate() );
        PointValue centerPoint = Values.pointValue( crs, curve.centerPointFor( spaceFillingCurveValue ) );

        BtreeKey firstKey = newKeyState();
        firstKey.writeValue( firstPoint, NEUTRAL );
        BtreeKey equalKey = newKeyState();
        equalKey.writeValue( equalPoint, NEUTRAL );
        BtreeKey centerKey = newKeyState();
        centerKey.writeValue( centerPoint, NEUTRAL );
        BtreeKey noCoordsKey = newKeyState();
        noCoordsKey.writeValue( equalPoint, NEUTRAL );
        GeometryType.setNoCoordinates( noCoordsKey );

        assertEquals( 0, firstKey.compareValueTo( equalKey ), "expected keys to be equal" );
        assertEquals( firstPoint.compareTo( centerPoint ) != 0, firstKey.compareValueTo( centerKey ) != 0,
                "expected keys to be equal if and only if source points are equal" );
        assertEquals( 0, firstKey.compareValueTo( noCoordsKey ), "expected keys to be equal" );
    }

    @Test
    void comparePointArraysMustOnlyReturnZeroForEqualArrays()
    {
        PointArray firstArray = random.randomValues().nextPointArray();
        PointValue[] sourcePointValues = firstArray.asObjectCopy();
        PointArray equalArray = Values.pointArray( sourcePointValues );
        PointValue[] centerPointValues = new PointValue[sourcePointValues.length];
        for ( int i = 0; i < sourcePointValues.length; i++ )
        {
            PointValue sourcePointValue = sourcePointValues[i];
            CoordinateReferenceSystem crs = sourcePointValue.getCoordinateReferenceSystem();
            SpaceFillingCurve curve = noSpecificIndexSettings.forCrs( crs );
            Long spaceFillingCurveValue = curve.derivedValueFor( sourcePointValue.coordinate() );
            centerPointValues[i] = Values.pointValue( crs, curve.centerPointFor( spaceFillingCurveValue ) );
        }
        PointArray centerArray = Values.pointArray( centerPointValues );

        BtreeKey firstKey = newKeyState();
        firstKey.writeValue( firstArray, NEUTRAL );
        BtreeKey equalKey = newKeyState();
        equalKey.writeValue( equalArray, NEUTRAL );
        BtreeKey centerKey = newKeyState();
        centerKey.writeValue( centerArray, NEUTRAL );
        BtreeKey noCoordsKey = newKeyState();
        noCoordsKey.writeValue( equalArray, NEUTRAL );
        GeometryType.setNoCoordinates( noCoordsKey );

        assertEquals( 0, firstKey.compareValueTo( equalKey ), "expected keys to be equal" );
        assertEquals( firstArray.compareToSequence( centerArray, AnyValues.COMPARATOR ) != 0, firstKey.compareValueTo( centerKey ) != 0,
                "expected keys to be equal if and only if source points are equal" );
        assertEquals( 0, firstKey.compareValueTo( noCoordsKey ), "expected keys to be equal" );
    }

    @Override
    boolean includePointTypesForComparisons()
    {
        return false;
    }

    @Override
    int getPointSerialisedSize( int dimensions )
    {
        if ( dimensions == 2 )
        {
            return  28;
        }
        else if ( dimensions == 3 )
        {
            return  36;
        }
        else
        {
            throw new RuntimeException( "Did not expect spatial value with " + dimensions + " dimensions." );
        }
    }

    @Override
    int getArrayPointSerialisedSize( int dimensions )
    {
        if ( dimensions == 2 )
        {
            return  24;
        }
        else if ( dimensions == 3 )
        {
            return  32;
        }
        else
        {
            throw new RuntimeException( "Did not expect spatial value with " + dimensions + " dimensions." );
        }
    }

    @Override
    Layout<BtreeKey> newLayout( int numberOfSlots )
    {
        GenericLayout btreeLayout = new GenericLayout( numberOfSlots, noSpecificIndexSettings );
        return new Layout<>()
        {

            @Override
            public BtreeKey newKey()
            {
                return btreeLayout.newKey();
            }

            @Override
            public void minimalSplitter( BtreeKey left, BtreeKey right, BtreeKey into )
            {
                btreeLayout.minimalSplitter( left, right, into );
            }

            @Override
            public int compare( BtreeKey k1, BtreeKey k2 )
            {
                return btreeLayout.compare( k1, k2 );
            }
        };
    }
}
