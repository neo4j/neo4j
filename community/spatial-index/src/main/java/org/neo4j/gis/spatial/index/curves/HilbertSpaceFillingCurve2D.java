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
package org.neo4j.gis.spatial.index.curves;

import java.util.HashMap;
import java.util.LinkedHashMap;

import org.neo4j.gis.spatial.index.Envelope;

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

        public char direction( int end )
        {
            int start = npointValues[0];
            end -= start;
            switch ( end )
            {
            case 1:
                return 'U'; // move up      00->01
            case 2:
                return 'R'; // move right   00->10
            case -2:
                return 'L'; // move left    11->01
            case -1:
                return 'D'; // move down    11->10
            default:
                return '-';
            }
        }

        public String name()
        {
            return String.valueOf( direction( npointValues[1] ) );
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
    }

    private static HashMap<String,HilbertCurve2D> curves = new LinkedHashMap<>();

    private static void addCurveRule( int... npointValues )
    {
        HilbertCurve2D curve = new HilbertCurve2D( npointValues );
        String name = curve.name();
        if ( !curves.containsKey( name ) )
        {
            curves.put( name, curve );
        }
    }

    private static void setChildren( String parent, String... children )
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
        setChildren( "U", "R", "U", "U", "L" );
        setChildren( "R", "U", "R", "R", "D" );
        setChildren( "D", "L", "D", "D", "R" );
        setChildren( "L", "D", "L", "L", "U" );
        curveUp = curves.get( "U" );
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
