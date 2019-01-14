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
package org.neo4j.gis.spatial.index.curves;

import java.util.EnumMap;

import org.neo4j.gis.spatial.index.Envelope;

import static org.neo4j.gis.spatial.index.curves.HilbertSpaceFillingCurve2D.Direction2D.DOWN;
import static org.neo4j.gis.spatial.index.curves.HilbertSpaceFillingCurve2D.Direction2D.LEFT;
import static org.neo4j.gis.spatial.index.curves.HilbertSpaceFillingCurve2D.Direction2D.RIGHT;
import static org.neo4j.gis.spatial.index.curves.HilbertSpaceFillingCurve2D.Direction2D.UP;

public class HilbertSpaceFillingCurve2D extends SpaceFillingCurve
{

    /**
     * Description of the space filling curve structure
     */
    static class HilbertCurve2D extends CurveRule
    {
        private CurveRule[] children;

        private HilbertCurve2D( int... npointValues )
        {
            super( 2, npointValues );
            assert npointValues[0] == 0 || npointValues[0] == 3;
        }

        private Direction2D direction( int end )
        {
            int start = npointValues[0];
            end -= start;
            switch ( end )
            {
            case 1:
                return UP; // move up      00->01
            case 2:
                return RIGHT; // move right   00->10
            case -2:
                return LEFT; // move left    11->01
            case -1:
                return DOWN; // move down    11->10
            default:
                throw new IllegalArgumentException( "Illegal direction: " + end );
            }
        }

        private Direction2D name()
        {
            return direction( npointValues[1] );
        }

        private void setChildren( CurveRule... children )
        {
            this.children = children;
        }

        @Override
        public CurveRule childAt( int npoint )
        {
            return children[npoint];
        }

        @Override
        public String toString()
        {
            return String.valueOf( name() );
        }
    }

    enum Direction2D
    {
        UP, RIGHT, LEFT, DOWN
    }

    private static EnumMap<Direction2D,HilbertCurve2D> curves = new EnumMap<>( Direction2D.class );

    private static void addCurveRule( int... npointValues )
    {
        HilbertCurve2D curve = new HilbertCurve2D( npointValues );
        Direction2D name = curve.name();
        if ( !curves.containsKey( name ) )
        {
            curves.put( name, curve );
        }
    }

    private static void setChildren( Direction2D parent, Direction2D... children )
    {
        HilbertCurve2D curve = curves.get( parent );
        HilbertCurve2D[] childCurves = new HilbertCurve2D[children.length];
        for ( int i = 0; i < children.length; i++ )
        {
            childCurves[i] = curves.get( children[i] );
        }
        curve.setChildren( childCurves );
    }

    private static final HilbertCurve2D curveUp;

    static
    {
        addCurveRule( 0, 1, 3, 2 );
        addCurveRule( 0, 2, 3, 1 );
        addCurveRule( 3, 1, 0, 2 );
        addCurveRule( 3, 2, 0, 1 );
        setChildren( UP, RIGHT, UP, UP, LEFT );
        setChildren( RIGHT, UP, RIGHT, RIGHT, DOWN );
        setChildren( DOWN, LEFT, DOWN, DOWN, RIGHT );
        setChildren( LEFT, DOWN, LEFT, LEFT, UP );
        curveUp = curves.get( UP );
    }

    public static final int MAX_LEVEL = 63 / 2 - 1;

    public HilbertSpaceFillingCurve2D( Envelope range )
    {
        this( range, MAX_LEVEL );
    }

    public HilbertSpaceFillingCurve2D( Envelope range, int maxLevel )
    {
        super( range, maxLevel );
        assert maxLevel <= MAX_LEVEL;
        assert range.getDimension() == 2;
    }

    @Override
    protected CurveRule rootCurve()
    {
        return curveUp;
    }

}
