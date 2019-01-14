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

import java.util.LinkedHashMap;
import java.util.Map;

import org.neo4j.gis.spatial.index.Envelope;

import static org.neo4j.gis.spatial.index.curves.HilbertSpaceFillingCurve3D.Direction3D.BACK;
import static org.neo4j.gis.spatial.index.curves.HilbertSpaceFillingCurve3D.Direction3D.DOWN;
import static org.neo4j.gis.spatial.index.curves.HilbertSpaceFillingCurve3D.Direction3D.FRONT;
import static org.neo4j.gis.spatial.index.curves.HilbertSpaceFillingCurve3D.Direction3D.LEFT;
import static org.neo4j.gis.spatial.index.curves.HilbertSpaceFillingCurve3D.Direction3D.RIGHT;
import static org.neo4j.gis.spatial.index.curves.HilbertSpaceFillingCurve3D.Direction3D.UP;

public class HilbertSpaceFillingCurve3D extends SpaceFillingCurve
{

    /**
     * Utilities for rotating point values in binary about various axes
     */
    static class BinaryCoordinateRotationUtils3D
    {
        static int rotateNPointLeft( int value )
        {
            return (value << 1) & 0b111 | ((value & 0b100) >> 2);
        }

        static int rotateNPointRight( int value )
        {
            return (value >> 1) | ((value & 0b001) << 2);
        }

        static int xXOR( int value )
        {
            return value ^ 0b100;
        }

        static int rotateYZ( int value )
        {
            return value ^ 0b011;
        }
    }

    /**
     * Description of the space filling curve structure
     */
    static class HilbertCurve3D extends CurveRule
    {
        HilbertCurve3D[] children;

        private HilbertCurve3D( int... npointValues )
        {
            super( 3, npointValues );
            assert npointValues[0] == 0 || npointValues[0] == 3 || npointValues[0] == 5 || npointValues[0] == 6;
        }

        @Override
        public CurveRule childAt( int npoint )
        {
            return children[npoint];
        }

        @Override
        public String toString()
        {
            return name().toString();
        }

        static String binaryString( int value )
        {
            String binary = "00" + Integer.toBinaryString( value );
            return binary.substring( binary.length() - 3, binary.length() );
        }

        private Direction3D direction( int start, int end )
        {
            end -= start;
            switch ( end )
            {
            case 1:
                return FRONT; // move forward 000->001
            case 2:
                return UP;    // move up      000->010
            case 4:
                return RIGHT; // move right   000->100
            case -4:
                return LEFT;  // move left    111->011
            case -2:
                return DOWN;  // move down    111->101
            case -1:
                return BACK;  // move back    111->110
            default:
                throw new IllegalArgumentException( "Illegal direction: " + end );
            }
        }

        SubCurve3D name()
        {
            return new SubCurve3D(
                    direction( npointValues[0], npointValues[1] ),
                    direction( npointValues[1], npointValues[2] ),
                    direction( npointValues[0], npointValues[length() - 1] ) );
        }

        /**
         * Rotate about the normal diagonal (the 000->111 diagonal). This simply involves
         * rotating the bits of all npoint values either left or right depending on the
         * direction of rotation, normal or reversed (positive or negative).
         */
        private HilbertCurve3D rotateOneThirdDiagonalPos( boolean direction )
        {
            int[] newNpoints = new int[length()];
            for ( int i = 0; i < length(); i++ )
            {
                if ( direction )
                {
                    newNpoints[i] = BinaryCoordinateRotationUtils3D.rotateNPointRight( npointValues[i] );
                }
                else
                {
                    newNpoints[i] = BinaryCoordinateRotationUtils3D.rotateNPointLeft( npointValues[i] );
                }
            }
            return new HilbertCurve3D( newNpoints );
        }

        /**
         * Rotate about the neg-x diagonal (the 100->011 diagonal). This is similar to the
         * normal diagonal rotation, but with x-switched, so we XOR the x value before and after
         * the rotation, and rotate in the opposite direction to specified.
         */
        private HilbertCurve3D rotateOneThirdDiagonalNeg( boolean direction )
        {
            int[] newNpoints = new int[length()];
            for ( int i = 0; i < length(); i++ )
            {
                if ( direction )
                {
                    newNpoints[i] = BinaryCoordinateRotationUtils3D.xXOR(
                            BinaryCoordinateRotationUtils3D.rotateNPointLeft( BinaryCoordinateRotationUtils3D.xXOR( npointValues[i] ) ) );
                }
                else
                {
                    newNpoints[i] = BinaryCoordinateRotationUtils3D.xXOR(
                            BinaryCoordinateRotationUtils3D.rotateNPointRight( BinaryCoordinateRotationUtils3D.xXOR( npointValues[i] ) ) );
                }
            }
            return new HilbertCurve3D( newNpoints );
        }

        /**
         * Rotate about the x-axis. This involves leaving x values the same, but xOR'ing the rest.
         */
        private HilbertCurve3D rotateAboutX()
        {
            int[] newNpoints = new int[length()];
            for ( int i = 0; i < length(); i++ )
            {
                newNpoints[i] = BinaryCoordinateRotationUtils3D.rotateYZ( npointValues[i] );
            }
            return new HilbertCurve3D( newNpoints );
        }

        private void buildCurveTree( Map<SubCurve3D,HilbertCurve3D> curves )
        {
            if ( children == null )
            {
                makeChildren( curves );
                curves.put( name(), this );

                for ( HilbertCurve3D child : children )
                {
                    child.buildCurveTree( curves );
                }
            }
        }

        private void makeChildren( Map<SubCurve3D,HilbertCurve3D> curves )
        {
            children = new HilbertCurve3D[length()];
            children[0] = singleton( curves, rotateOneThirdDiagonalPos( true ) );
            children[1] = singleton( curves, rotateOneThirdDiagonalPos( false ) );
            children[2] = singleton( curves, rotateOneThirdDiagonalPos( false ) );
            children[3] = singleton( curves, rotateAboutX() );
            children[4] = singleton( curves, rotateAboutX() );
            children[5] = singleton( curves, rotateOneThirdDiagonalNeg( true ) );
            children[6] = singleton( curves, rotateOneThirdDiagonalNeg( true ) );
            children[7] = singleton( curves, rotateOneThirdDiagonalNeg( false ) );
        }

        private HilbertCurve3D singleton( Map<SubCurve3D,HilbertCurve3D> curves, HilbertCurve3D newCurve )
        {
            return curves.computeIfAbsent( newCurve.name(), key -> newCurve );
        }
    }

    enum Direction3D
    {
        UP, RIGHT, LEFT, DOWN, FRONT, BACK
    }

    static class SubCurve3D
    {
        private final Direction3D firstMove;
        private final Direction3D secondMove;
        private final Direction3D overallDirection;

        SubCurve3D( Direction3D firstMove, Direction3D secondMove, Direction3D overallDirection )
        {
            this.firstMove = firstMove;
            this.secondMove = secondMove;
            this.overallDirection = overallDirection;
        }

        @Override
        public int hashCode()
        {
            return java.util.Objects.hash( firstMove, secondMove, overallDirection );
        }

        @Override
        public boolean equals( Object obj )
        {
            if ( obj == null || this.getClass() != obj.getClass() )
            {
                return false;
            }
            SubCurve3D other = (SubCurve3D) obj;
            return this.firstMove == other.firstMove && this.secondMove == other.secondMove && this.overallDirection == other.overallDirection;
        }

        @Override
        public String toString()
        {
            return firstMove.toString() + secondMove.toString() + overallDirection.toString();
        }
    }

    // this is left accessible to make debugging easier
    static Map<SubCurve3D,HilbertCurve3D> curves = new LinkedHashMap<>();

    private static HilbertCurve3D buildTheCurve()
    {
        // We start with a UFR curve
        int[] npointValues = {0b000, 0b010, 0b011, 0b001, 0b101, 0b111, 0b110, 0b100};
        HilbertCurve3D theCurve = new HilbertCurve3D( npointValues );

        theCurve.buildCurveTree( curves );
        return theCurve;
    }

    private static final HilbertCurve3D THE_CURVE = buildTheCurve();

    public static final int MAX_LEVEL = 63 / 3 - 1;

    public HilbertSpaceFillingCurve3D( Envelope range )
    {
        this( range, MAX_LEVEL );
    }

    public HilbertSpaceFillingCurve3D( Envelope range, int maxLevel )
    {
        super( range, maxLevel );
        assert maxLevel <= MAX_LEVEL;
        assert range.getDimension() == 3;
    }

    @Override
    protected CurveRule rootCurve()
    {
        return THE_CURVE;
    }
}
