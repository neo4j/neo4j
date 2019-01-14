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

import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.neo4j.gis.spatial.index.Envelope;
import org.neo4j.gis.spatial.index.curves.HilbertSpaceFillingCurve3D.HilbertCurve3D;
import org.neo4j.gis.spatial.index.curves.HilbertSpaceFillingCurve3D.SubCurve3D;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertArrayEquals;
import static org.neo4j.gis.spatial.index.curves.HilbertSpaceFillingCurve3D.BinaryCoordinateRotationUtils3D.rotateNPointLeft;
import static org.neo4j.gis.spatial.index.curves.HilbertSpaceFillingCurve3D.BinaryCoordinateRotationUtils3D.rotateNPointRight;

public class SpaceFillingCurveTest
{
    private static final boolean verbose = false;
    private static final boolean debug = false;

    private static void log( String line )
    {
        if ( verbose )
        {
            //TODO: Consider adding timestamp to all lines
            System.out.println( line );
        }
    }

    private static void debug( String line )
    {
        if ( debug )
        {
            //TODO: Consider adding timestamp to all lines
            System.out.println( line );
        }
    }

    //
    // Set of tests for 2D ZOrderCurve at various levels
    //
    @Test
    public void shouldCreateSimple2DZOrderCurveAtLevel1()
    {
        Envelope envelope = new Envelope( -8, 8, -8, 8 );
        ZOrderSpaceFillingCurve2D curve = new ZOrderSpaceFillingCurve2D( envelope, 1 );
        assertAtLevel( curve, envelope );
        assertRange( "Bottom-left should evaluate to zero", curve, getTileEnvelope( envelope, 2, 0, 1 ), 0L );
        assertRange( "Top-left should evaluate to one", curve, getTileEnvelope( envelope, 2, 1, 1 ), 1L );
        assertRange( "Top-right should evaluate to two", curve, getTileEnvelope( envelope, 2, 0, 0 ), 2L );
        assertRange( "Bottom-right should evaluate to three", curve, getTileEnvelope( envelope, 2, 1, 0 ), 3L );
    }

    @Test
    public void shouldCreateSimple2DZOrderCurveAtLevel2()
    {
        Envelope envelope = new Envelope( -8, 8, -8, 8 );
        ZOrderSpaceFillingCurve2D curve = new ZOrderSpaceFillingCurve2D( envelope, 2 );
        assertAtLevel( curve, envelope );
        assertRange( "'00' should evaluate to 0", curve, getTileEnvelope( envelope, 4, 0, 3 ), 0L );
        assertRange( "'10' should evaluate to 1", curve, getTileEnvelope( envelope, 4, 1, 3 ), 1L );
        assertRange( "'11' should evaluate to 2", curve, getTileEnvelope( envelope, 4, 0, 2 ), 2L );
        assertRange( "'01' should evaluate to 3", curve, getTileEnvelope( envelope, 4, 1, 2 ), 3L );
        assertRange( "'02' should evaluate to 4", curve, getTileEnvelope( envelope, 4, 2, 3 ), 4L );
        assertRange( "'03' should evaluate to 5", curve, getTileEnvelope( envelope, 4, 3, 3 ), 5L );
        assertRange( "'13' should evaluate to 6", curve, getTileEnvelope( envelope, 4, 2, 2 ), 6L );
        assertRange( "'12' should evaluate to 7", curve, getTileEnvelope( envelope, 4, 3, 2 ), 7L );
        assertRange( "'22' should evaluate to 8", curve, getTileEnvelope( envelope, 4, 0, 1 ), 8L );
        assertRange( "'23' should evaluate to 9", curve, getTileEnvelope( envelope, 4, 1, 1 ), 9L );
        assertRange( "'33' should evaluate to 10", curve, getTileEnvelope( envelope, 4, 0, 0 ), 10L );
        assertRange( "'32' should evaluate to 11", curve, getTileEnvelope( envelope, 4, 1, 0 ), 11L );
        assertRange( "'31' should evaluate to 12", curve, getTileEnvelope( envelope, 4, 2, 1 ), 12L );
        assertRange( "'21' should evaluate to 13", curve, getTileEnvelope( envelope, 4, 3, 1 ), 13L );
        assertRange( "'20' should evaluate to 14", curve, getTileEnvelope( envelope, 4, 2, 0 ), 14L );
        assertRange( "'30' should evaluate to 15", curve, getTileEnvelope( envelope, 4, 3, 0 ), 15L );
    }

    @Test
    public void shouldCreateSimple2DZOrderCurveAtLevel3()
    {
        Envelope envelope = new Envelope( -8, 8, -8, 8 );
        assertAtLevel( new ZOrderSpaceFillingCurve2D( envelope, 3 ), envelope );
    }

    @Test
    public void shouldCreateSimple2DZOrderCurveAtLevel4()
    {
        Envelope envelope = new Envelope( -8, 8, -8, 8 );
        assertAtLevel( new ZOrderSpaceFillingCurve2D( envelope, 4 ), envelope );
    }

    @Test
    public void shouldCreateSimple2DZOrderCurveAtLevel5()
    {
        Envelope envelope = new Envelope( -8, 8, -8, 8 );
        assertAtLevel( new ZOrderSpaceFillingCurve2D( envelope, 5 ), envelope );
    }

    @Test
    public void shouldCreateSimple2DZOrderCurveAtLevel24()
    {
        Envelope envelope = new Envelope( -8, 8, -8, 8 );
        assertAtLevel( new ZOrderSpaceFillingCurve2D( envelope, 24 ), envelope );
    }

    @Test
    public void shouldCreateSimple2DZOrderCurveAtManyLevels()
    {
        Envelope envelope = new Envelope( new double[]{-8, -8}, new double[]{8, 8} );
        for ( int level = 1; level <= ZOrderSpaceFillingCurve2D.MAX_LEVEL; level++ )
        {
            ZOrderSpaceFillingCurve2D curve = new ZOrderSpaceFillingCurve2D( envelope, level );
            log( "Max value at level " + level + ": " + curve.getValueWidth() );
            assertAtLevel( curve, envelope );
            assertRange( (int) curve.getWidth(), 0, curve, 0, (int) curve.getWidth() - 1 );
            assertRange( (int) curve.getWidth(), curve.getValueWidth() - 1, curve, (int) curve.getWidth() - 1, 0 );
        }
    }

    @Test
    public void shouldCreateSimple2DZOrderCurveAtLevelDefault()
    {
        Envelope envelope = new Envelope( -8, 8, -8, 8 );
        assertAtLevel( new ZOrderSpaceFillingCurve2D( envelope ), envelope );
    }

    @Test
    public void shouldCreate2DZOrderCurveWithRectangularEnvelope()
    {
        assert2DAtLevel( new Envelope( -8, 8, -20, 20 ), 3 );
    }

    @Test
    public void shouldCreate2DZOrderCurveWithNonCenteredEnvelope()
    {
        assert2DAtLevel( new Envelope( 2, 7, 2, 7 ), 3 );
    }

    @Test
    public void shouldWorkWithNormalGPSCoordinatesZOrder()
    {
        Envelope envelope = new Envelope( -180, 180, -90, 90 );
        ZOrderSpaceFillingCurve2D curve = new ZOrderSpaceFillingCurve2D( envelope );
        assertAtLevel( curve, envelope );
    }

    @Test
    public void shouldGet2DZOrderSearchTilesForManyLevels()
    {
        Envelope envelope = new Envelope( -8, 8, -8, 8 );
        for ( int level = 1; level <= ZOrderSpaceFillingCurve2D.MAX_LEVEL; level++ )
        {
            ZOrderSpaceFillingCurve2D curve = new ZOrderSpaceFillingCurve2D( envelope, level );
            double halfTile = curve.getTileWidth( 0, level ) / 2.0;
            long start = System.currentTimeMillis();
            assertTiles( curve.getTilesIntersectingEnvelope( new Envelope( -8, -8 + halfTile, 8 - halfTile, 8 ) ),
                    new SpaceFillingCurve.LongRange( 0, 0 ) );
            assertTiles( curve.getTilesIntersectingEnvelope( new Envelope( 8 - halfTile, 8, -8, -8 + halfTile ) ),
                    new SpaceFillingCurve.LongRange( curve.getValueWidth() - 1, curve.getValueWidth() - 1 ) );
            assertTiles( curve.getTilesIntersectingEnvelope( new Envelope( 8 - halfTile, 8, 0, 0 + halfTile ) ),
                    new SpaceFillingCurve.LongRange( curve.getValueWidth() / 2 - 1, curve.getValueWidth() / 2 - 1 ) );
            log( "Hilbert query at level " + level + " took " + (System.currentTimeMillis() - start) + "ms" );
        }
    }

    //
    // Set of tests for 2D HilbertCurve at various levels
    //

    @Test
    public void shouldCreateSimple2DHilbertCurveAtLevel1()
    {
        Envelope envelope = new Envelope( -8, 8, -8, 8 );
        HilbertSpaceFillingCurve2D curve = new HilbertSpaceFillingCurve2D( envelope, 1 );
        assertAtLevel( curve, envelope );
        assertRange( "Bottom-left should evaluate to zero", curve, getTileEnvelope( envelope, 2, 0, 0 ), 0L );
        assertRange( "Top-left should evaluate to one", curve, getTileEnvelope( envelope, 2, 0, 1 ), 1L );
        assertRange( "Top-right should evaluate to two", curve, getTileEnvelope( envelope, 2, 1, 1 ), 2L );
        assertRange( "Bottom-right should evaluate to three", curve, getTileEnvelope( envelope, 2, 1, 0 ), 3L );
    }

    @Test
    public void shouldCreateSimple2DHilbertCurveAtLevel2()
    {
        Envelope envelope = new Envelope( -8, 8, -8, 8 );
        HilbertSpaceFillingCurve2D curve = new HilbertSpaceFillingCurve2D( envelope, 2 );
        assertAtLevel( curve, envelope );
        assertRange( "'00' should evaluate to 0", curve, getTileEnvelope( envelope, 4, 0, 0 ), 0L );
        assertRange( "'10' should evaluate to 1", curve, getTileEnvelope( envelope, 4, 1, 0 ), 1L );
        assertRange( "'11' should evaluate to 2", curve, getTileEnvelope( envelope, 4, 1, 1 ), 2L );
        assertRange( "'01' should evaluate to 3", curve, getTileEnvelope( envelope, 4, 0, 1 ), 3L );
        assertRange( "'02' should evaluate to 4", curve, getTileEnvelope( envelope, 4, 0, 2 ), 4L );
        assertRange( "'03' should evaluate to 5", curve, getTileEnvelope( envelope, 4, 0, 3 ), 5L );
        assertRange( "'13' should evaluate to 6", curve, getTileEnvelope( envelope, 4, 1, 3 ), 6L );
        assertRange( "'12' should evaluate to 7", curve, getTileEnvelope( envelope, 4, 1, 2 ), 7L );
        assertRange( "'22' should evaluate to 8", curve, getTileEnvelope( envelope, 4, 2, 2 ), 8L );
        assertRange( "'23' should evaluate to 9", curve, getTileEnvelope( envelope, 4, 2, 3 ), 9L );
        assertRange( "'33' should evaluate to 10", curve, getTileEnvelope( envelope, 4, 3, 3 ), 10L );
        assertRange( "'32' should evaluate to 11", curve, getTileEnvelope( envelope, 4, 3, 2 ), 11L );
        assertRange( "'31' should evaluate to 12", curve, getTileEnvelope( envelope, 4, 3, 1 ), 12L );
        assertRange( "'21' should evaluate to 13", curve, getTileEnvelope( envelope, 4, 2, 1 ), 13L );
        assertRange( "'20' should evaluate to 14", curve, getTileEnvelope( envelope, 4, 2, 0 ), 14L );
        assertRange( "'30' should evaluate to 15", curve, getTileEnvelope( envelope, 4, 3, 0 ), 15L );
    }

    @Test
    public void shouldCreateSimple2DHilbertCurveAtLevel3()
    {
        assert2DAtLevel( new Envelope( -8, 8, -8, 8 ), 3 );
    }

    @Test
    public void shouldCreateSimple2DHilbertCurveAtLevel4()
    {
        assert2DAtLevel( new Envelope( -8, 8, -8, 8 ), 4 );
    }

    @Test
    public void shouldCreateSimple2DHilbertCurveAtLevel5()
    {
        assert2DAtLevel( new Envelope( -8, 8, -8, 8 ), 5 );
    }

    @Test
    public void shouldCreateSimple2DHilbertCurveAtLevel24()
    {
        assert2DAtLevel( new Envelope( -8, 8, -8, 8 ), 24 );
    }

    @Test
    public void shouldCreateSimple2DHilbertCurveAtManyLevels()
    {
        Envelope envelope = new Envelope( new double[]{-8, -8}, new double[]{8, 8} );
        for ( int level = 1; level <= HilbertSpaceFillingCurve2D.MAX_LEVEL; level++ )
        {
            HilbertSpaceFillingCurve2D curve = new HilbertSpaceFillingCurve2D( envelope, level );
            log( "Max value at level " + level + ": " + curve.getValueWidth() );
            assertAtLevel( curve, envelope );
            assertRange( (int) curve.getWidth(), 0, curve, 0, 0 );
            assertRange( (int) curve.getWidth(), curve.getValueWidth() - 1, curve, (int) curve.getWidth() - 1, 0 );
        }
    }

    @Test
    public void shouldCreateSimple2DHilbertCurveAtLevelDefault()
    {
        Envelope envelope = new Envelope( -8, 8, -8, 8 );
        assertAtLevel( new HilbertSpaceFillingCurve2D( envelope ), envelope );
    }

    @Test
    public void shouldCreate2DHilbertCurveWithRectangularEnvelope()
    {
        assert2DAtLevel( new Envelope( -8, 8, -20, 20 ), 3 );
    }

    @Test
    public void shouldCreate2DHilbertCurveWithNonCenteredEnvelope()
    {
        assert2DAtLevel( new Envelope( 2, 7, 2, 7 ), 3 );
    }

    @Test
    public void shouldCreate2DHilbertCurveOfThreeLevelsFromExampleInThePaper()
    {
        HilbertSpaceFillingCurve2D curve = new HilbertSpaceFillingCurve2D( new Envelope( 0, 8, 0, 8 ), 3 );
        assertThat( "Example should evaluate to 101110", curve.derivedValueFor( new double[]{6, 4} ), equalTo( 46L ) );
    }

    @Test
    public void shouldWorkWithNormalGPSCoordinatesHilbert()
    {
        Envelope envelope = new Envelope( -180, 180, -90, 90 );
        HilbertSpaceFillingCurve2D curve = new HilbertSpaceFillingCurve2D( envelope );
        assertAtLevel( curve, envelope );
    }

    @Test
    public void shouldGet2DHilbertSearchTilesForLevel1()
    {
        Envelope envelope = new Envelope( -8, 8, -8, 8 );
        HilbertSpaceFillingCurve2D curve = new HilbertSpaceFillingCurve2D( envelope, 1 );
        assertTiles( curve.getTilesIntersectingEnvelope( new Envelope( -6, -5, -6, -5 ) ), new SpaceFillingCurve.LongRange( 0, 0 ) );
        assertTiles( curve.getTilesIntersectingEnvelope( new Envelope( 0, 6, -6, -5 ) ), new SpaceFillingCurve.LongRange( 3, 3 ) );
        assertTiles( curve.getTilesIntersectingEnvelope( new Envelope( -6, 4, -5, -2 ) ), new SpaceFillingCurve.LongRange( 0, 0 ),
                new SpaceFillingCurve.LongRange( 3, 3 ) );
        assertTiles( curve.getTilesIntersectingEnvelope( new Envelope( -2, -1, -6, 5 ) ), new SpaceFillingCurve.LongRange( 0, 1 ) );
        assertTiles( curve.getTilesIntersectingEnvelope( new Envelope( -2, 1, -6, 5 ) ), new SpaceFillingCurve.LongRange( 0, 3 ) );
    }

    @Test
    public void shouldGet2DHilbertSearchTilesForLevel2()
    {
        Envelope envelope = new Envelope( -8, 8, -8, 8 );
        HilbertSpaceFillingCurve2D curve = new HilbertSpaceFillingCurve2D( envelope, 2 );
        assertTiles( curve.getTilesIntersectingEnvelope( new Envelope( -6, -5, -6, -5 ) ), new SpaceFillingCurve.LongRange( 0, 0 ) );
        assertTiles( curve.getTilesIntersectingEnvelope( new Envelope( 0, 6, -6, -5 ) ), new SpaceFillingCurve.LongRange( 14, 15 ) );
        assertTiles( curve.getTilesIntersectingEnvelope( new Envelope( -6, 4, -5, -2 ) ), new SpaceFillingCurve.LongRange( 0, 3 ),
                new SpaceFillingCurve.LongRange( 12, 15 ) );
        assertTiles( curve.getTilesIntersectingEnvelope( new Envelope( -2, -1, -6, 5 ) ), new SpaceFillingCurve.LongRange( 1, 2 ),
                new SpaceFillingCurve.LongRange( 6, 7 ) );
        assertTiles( curve.getTilesIntersectingEnvelope( new Envelope( -2, 1, -6, 5 ) ), new SpaceFillingCurve.LongRange( 1, 2 ),
                new SpaceFillingCurve.LongRange( 6, 9 ), new SpaceFillingCurve.LongRange( 13, 14 ) );
    }

    @Test
    public void shouldGet2DHilbertSearchTilesForLevel3()
    {
        Envelope envelope = new Envelope( -8, 8, -8, 8 );
        HilbertSpaceFillingCurve2D curve = new HilbertSpaceFillingCurve2D( envelope, 3 );
        assertTiles( curve.getTilesIntersectingEnvelope( new Envelope( -8, -7, -8, -7 ) ), new SpaceFillingCurve.LongRange( 0, 0 ) );
        assertTiles( curve.getTilesIntersectingEnvelope( new Envelope( 0, 1, 0, 1 ) ), new SpaceFillingCurve.LongRange( 32, 32 ) );
        assertTiles( curve.getTilesIntersectingEnvelope( new Envelope( 7, 8, -8, -1 ) ), new SpaceFillingCurve.LongRange( 48, 49 ),
                new SpaceFillingCurve.LongRange( 62, 63 ) );
    }

    @Test
    public void shouldGet2DHilbertSearchTilesForManyLevels()
    {
        Envelope envelope = new Envelope( -8, 8, -8, 8 );
        for ( int level = 1; level <= HilbertSpaceFillingCurve2D.MAX_LEVEL; level++ )
        {
            HilbertSpaceFillingCurve2D curve = new HilbertSpaceFillingCurve2D( envelope, level );
            double halfTile = curve.getTileWidth( 0, level ) / 2.0;
            long start = System.currentTimeMillis();
            assertTiles( curve.getTilesIntersectingEnvelope( new Envelope( -8, -8 + halfTile, -8, -8 + halfTile ) ),
                    new SpaceFillingCurve.LongRange( 0, 0 ) );
            assertTiles( curve.getTilesIntersectingEnvelope( new Envelope( 8 - halfTile, 8, -8, -8 + halfTile ) ),
                    new SpaceFillingCurve.LongRange( curve.getValueWidth() - 1, curve.getValueWidth() - 1 ) );
            assertTiles( curve.getTilesIntersectingEnvelope( new Envelope( 0, halfTile, 0, halfTile ) ),
                    new SpaceFillingCurve.LongRange( curve.getValueWidth() / 2, curve.getValueWidth() / 2 ) );
            log( "Hilbert query at level " + level + " took " + (System.currentTimeMillis() - start) + "ms" );
        }
    }

    @Test
    public void shouldGet2DHilbertSearchTilesForWideRangeAtManyLevels()
    {
        final int xmin = -100;
        final int xmax = 100;
        final int ymin = -100;
        final int ymax = 100;
        Envelope envelope = new Envelope( xmin, xmax, ymin, ymax );
        for ( int level = 1; level <= HilbertSpaceFillingCurve2D.MAX_LEVEL; level++ )
        {
            HilbertSpaceFillingCurve2D curve = new HilbertSpaceFillingCurve2D( envelope, level );
            final int delta = 1;
            final int xhalf = xmin + (xmax - xmin) / 2;
            final int yhalf = ymin + (ymax - ymin) / 2;
            Envelope q1 = new Envelope( xmin, xhalf - delta, ymin, yhalf - delta );
            Envelope q2 = new Envelope( xmin, xhalf - delta, yhalf, ymax );
            Envelope q3 = new Envelope( xhalf, xmax, yhalf, ymax );
            Envelope q4 = new Envelope( xhalf, xmax, ymin, yhalf - delta );

            // Bottom left should give 1/4 of all tiles started at index 0
            assertTiles( curve.getTilesIntersectingEnvelope( q1 ),
                    new SpaceFillingCurve.LongRange( 0, curve.getValueWidth() / 4 - 1 ) );
            // Top left should give 1/4 of all tiles started at index 1/4
            assertTiles( curve.getTilesIntersectingEnvelope( q2 ),
                    new SpaceFillingCurve.LongRange( curve.getValueWidth() / 4, curve.getValueWidth() / 2 - 1 ) );
            // Top right should give 1/4 of all tiles started at index 1/2
            assertTiles( curve.getTilesIntersectingEnvelope( q3 ),
                    new SpaceFillingCurve.LongRange( curve.getValueWidth() / 2, 3 * curve.getValueWidth() / 4 - 1 ) );
            // Bottom right should give 1/4 of all tiles started at index 3/4
            assertTiles( curve.getTilesIntersectingEnvelope( q4 ),
                    new SpaceFillingCurve.LongRange( 3 * curve.getValueWidth() / 4, curve.getValueWidth() - 1 ) );
        }
    }

    private static class MonitorStats
    {
        long total;
        long min = Long.MAX_VALUE;
        long max;
        int count;

        void add( long value )
        {
            total += value;
            min = Math.min( min, value );
            max = Math.max( max, value );
            count++;
        }

        double avg()
        {
            return (double) total / (double) count;
        }
    }

    private static class MonitorDoubleStats
    {
        double total;
        double min = Double.MAX_VALUE;
        double max;
        int count;

        void add( double value )
        {
            total += value;
            min = Math.min( min, value );
            max = Math.max( max, value );
            count++;
        }

        double avg()
        {
            return total / count;
        }
    }

    @Test
    public void shouldHaveReasonableCoveredArea()
    {
        final double minExtent = 0.000001;
        final double maxAspect = 100.0;
        final int xmin = -100;
        final int xmax = 100;
        final int ymin = -100;
        final int ymax = 100;
        // Chosen to be smaller than 10, and "random" enough to not intersect with tile borders on higher levels.
        final double rectangleStepsPerDimension = 9.789;
        final double extensionFactor = 5;
        String formatHeader1 = "Level  Depth Limitation Configuration                  Area Ratio              Ranges                  Depth";
        String formatHeader2 = "                                                        avg    min    max       avg    min    max       avg    min    max";
        String formatBody = "%5d  %-42s   %7.2f%7.2f%7.2f   %7.2f%7d%7d   %7.2f%7d%7d";

        Envelope envelope = new Envelope( xmin, xmax, ymin, ymax );
        // For all 2D levels
        for ( int level = 1; level <= HilbertSpaceFillingCurve2D.MAX_LEVEL; level++ )
        {
            log( "" );
            log( formatHeader1 );
            log( formatHeader2 );
            for ( SpaceFillingCurveConfiguration config : new SpaceFillingCurveConfiguration[]{new StandardConfiguration( 1 ),
                    new StandardConfiguration( 2 ), new StandardConfiguration( 3 ), new StandardConfiguration( 4 ),
                    new PartialOverlapConfiguration( 1, 0.99, 0.1 ), new PartialOverlapConfiguration( 1, 0.99, 0.5 ),
                    new PartialOverlapConfiguration( 2, 0.99, 0.1 ), new PartialOverlapConfiguration( 2, 0.99, 0.5 ),
                    new PartialOverlapConfiguration( 3, 0.99, 0.1 ), new PartialOverlapConfiguration( 3, 0.99, 0.5 ),
                    new PartialOverlapConfiguration( 4, 0.99, 0.1 ), new PartialOverlapConfiguration( 4, 0.99, 0.5 )} )
            {
                MonitorDoubleStats areaStats = new MonitorDoubleStats();
                MonitorStats rangeStats = new MonitorStats();
                MonitorStats maxDepthStats = new MonitorStats();
                HilbertSpaceFillingCurve2D curve = new HilbertSpaceFillingCurve2D( envelope, level );

                // For differently shaped rectangles
                for ( double xExtent = minExtent; xExtent <= xmax; xExtent *= extensionFactor )
                {
                    for ( double yExtent = minExtent; yExtent <= ymax; yExtent *= extensionFactor )
                    {
                        // Filter out very thin rectangles
                        double aspect = xExtent > yExtent ? (xExtent / yExtent) : (yExtent / xExtent);
                        if ( aspect < maxAspect )
                        {
                            // For different positions of the rectangle
                            for ( double xOffset = 0; xmin + xOffset + xExtent <= xmax; xOffset += (xmax - xmin - xExtent) / rectangleStepsPerDimension )
                            {
                                for ( double yOffset = 0; xmin + yOffset + yExtent <= ymax; yOffset += (ymax - ymin - yExtent) / rectangleStepsPerDimension )
                                {
                                    HistogramMonitor monitor = new HistogramMonitor( curve.getMaxLevel() );
                                    final double xStart = xmin + xOffset;
                                    final double xEnd = xStart + xExtent;
                                    final double yStart = ymin + yOffset;
                                    final double yEnd = yStart + yExtent;
                                    Envelope searchEnvelope = new Envelope( xStart, xEnd, yStart, yEnd );
                                    final long start = System.currentTimeMillis();
                                    List<SpaceFillingCurve.LongRange> ranges = curve.getTilesIntersectingEnvelope( searchEnvelope, config, monitor );
                                    final long end = System.currentTimeMillis();
                                    debug( String.format(
                                            "Results for level %d, with x=[%f,%f] y=[%f,%f]. " +
                                                    "Search size vs covered size: %d vs %d (%f x). Ranges: %d. Took %d ms\n",
                                            level, xStart, xEnd, yStart, yEnd, monitor.getSearchArea(), monitor.getCoveredArea(),
                                            (double) (monitor.getCoveredArea()) / monitor.getSearchArea(), ranges.size(), end - start ) );
                                    int[] counts = monitor.getCounts();
                                    for ( int i = 0; i <= monitor.getHighestDepth(); i++ )
                                    {
                                        debug( "\t" + i + "\t" + counts[i] );
                                    }

                                    areaStats.add( (double) (monitor.getCoveredArea()) / monitor.getSearchArea() );
                                    rangeStats.add( ranges.size() );
                                    maxDepthStats.add( monitor.getHighestDepth() );

                                    assertThat( String.format( "Search size was bigger than covered size for level %d, with x=[%s,%s] y=[%s,%s]", level,
                                            Double.toHexString( xStart ), Double.toHexString( xEnd ), Double.toHexString( yStart ),
                                            Double.toHexString( yEnd ) ), monitor.getSearchArea(), lessThanOrEqualTo( monitor.getCoveredArea() ) );
                                }
                            }
                        }
                    }
                }

                // Average over all runs on this level
                log( String.format( formatBody, level, config.toString(),
                        areaStats.avg(), areaStats.min, areaStats.max,
                        rangeStats.avg(), rangeStats.min, rangeStats.max,
                        maxDepthStats.avg(), maxDepthStats.min, maxDepthStats.max ) );
            }
        }
    }

    @Test
    public void shouldHaveReasonableCoveredVolume()
    {
        final double minExtent = 0.000001;
        final double maxAspect = 100.0;
        Envelope envelope = new Envelope( new double[]{-100, -100, -100}, new double[]{100, 100, 100} );
        // Chosen to be smaller than 10, and "random" enough to not intersect with tile borders on higher levels.
        final double rectangleStepsPerDimension = 3.789;
        final double extensionFactor = 8;
        String formatHeader1 = "Level  Depth Limitation Configuration                  Area Ratio              Ranges                  Depth";
        String formatHeader2 = "                                                        avg    min    max       avg    min    max       avg    min    max";
        String formatBody = "%5d  %-42s   %7.2f%7.2f%7.2f   %7.2f%7d%7d   %7.2f%7d%7d";

        // For all 3D levels
        for ( int level = 7; level <= HilbertSpaceFillingCurve3D.MAX_LEVEL; level += 3 )
        {
            log( "" );
            log( formatHeader1 );
            log( formatHeader2 );
            for ( SpaceFillingCurveConfiguration config : new SpaceFillingCurveConfiguration[]{new StandardConfiguration( 1 ), new StandardConfiguration( 2 ),
                    new StandardConfiguration( 3 ), new StandardConfiguration( 4 ), new PartialOverlapConfiguration( 1, 0.99, 0.1 ),
                    new PartialOverlapConfiguration( 1, 0.99, 0.5 ), new PartialOverlapConfiguration( 2, 0.99, 0.1 ),
                    new PartialOverlapConfiguration( 2, 0.99, 0.5 ), new PartialOverlapConfiguration( 3, 0.99, 0.1 ),
                    new PartialOverlapConfiguration( 3, 0.99, 0.5 ), new PartialOverlapConfiguration( 4, 0.99, 0.1 ),
                    new PartialOverlapConfiguration( 4, 0.99, 0.5 )} )
            {
                MonitorDoubleStats areaStats = new MonitorDoubleStats();
                MonitorStats rangeStats = new MonitorStats();
                MonitorStats maxDepthStats = new MonitorStats();
                HilbertSpaceFillingCurve3D curve = new HilbertSpaceFillingCurve3D( envelope, level );

                double[] extent = new double[3];
                // For differently shaped rectangles
                for ( extent[0] = minExtent; extent[0] <= envelope.getMax( 0 ); extent[0] *= extensionFactor )
                {
                    for ( extent[1] = minExtent; extent[1] <= envelope.getMax( 1 ); extent[1] *= extensionFactor )
                    {
                        for ( extent[2] = minExtent; extent[2] <= envelope.getMax( 2 ); extent[2] *= extensionFactor )
                        {
                            // Filter out very thin rectangles
                            double aspectXY = extent[0] > extent[1] ? (extent[0] / extent[1]) : (extent[1] / extent[0]);
                            double aspectYZ = extent[1] > extent[2] ? (extent[1] / extent[2]) : (extent[2] / extent[1]);
                            double aspectZX = extent[2] > extent[0] ? (extent[2] / extent[0]) : (extent[0] / extent[2]);
                            if ( aspectXY < maxAspect && aspectYZ < maxAspect && aspectZX < maxAspect )
                            {
                                double[] offset = new double[3];
                                // For different positions of the rectangle
                                for ( offset[0] = 0; envelope.getMin( 0 ) + offset[0] + extent[0] <= envelope.getMax( 0 );
                                        offset[0] += (envelope.getWidth( 0 ) - extent[0]) / rectangleStepsPerDimension )
                                {
                                    for ( offset[1] = 0; envelope.getMin( 1 ) + offset[1] + extent[1] <= envelope.getMax( 1 );
                                            offset[1] += (envelope.getWidth( 1 ) - extent[1]) / rectangleStepsPerDimension )
                                    {
                                        for ( offset[2] = 0; envelope.getMin( 2 ) + offset[2] + extent[2] <= envelope.getMax( 2 );
                                                offset[2] += (envelope.getWidth( 2 ) - extent[2]) / rectangleStepsPerDimension )
                                        {
                                            HistogramMonitor monitor = new HistogramMonitor( curve.getMaxLevel() );
                                            final double[] startPoint = Arrays.copyOf( envelope.getMin(), 3 );
                                            final double[] endPoint = Arrays.copyOf( extent, 3 );
                                            for ( int i = 0; i < 3; i++ )
                                            {
                                                startPoint[i] += offset[i];
                                                endPoint[i] += startPoint[i];
                                            }
                                            Envelope searchEnvelope = new Envelope( startPoint, endPoint );
                                            final long start = System.currentTimeMillis();
                                            List<SpaceFillingCurve.LongRange> ranges = curve.getTilesIntersectingEnvelope( searchEnvelope, config, monitor );
                                            final long end = System.currentTimeMillis();
                                            debug( String.format( "Results for level %d, with search %s.", level, searchEnvelope.toString() ) );
                                            debug( String.format( "Search size vs covered size: %d vs %d (%f x). Ranges: %d. Took %d ms\n",
                                                    monitor.getSearchArea(), monitor.getCoveredArea(),
                                                    (double) (monitor.getCoveredArea()) / monitor.getSearchArea(), ranges.size(), end - start ) );
                                            int[] counts = monitor.getCounts();
                                            for ( int i = 0; i <= monitor.getHighestDepth(); i++ )
                                            {
                                                debug( "\t" + i + "\t" + counts[i] );
                                            }

                                            areaStats.add( (double) (monitor.getCoveredArea()) / monitor.getSearchArea() );
                                            rangeStats.add( ranges.size() );
                                            maxDepthStats.add( monitor.getHighestDepth() );

                                            assertThat( String.format( "Search size was bigger than covered size for level %d, with search %s", level,
                                                    searchEnvelope.toString() ), monitor.getSearchArea(), lessThanOrEqualTo( monitor.getCoveredArea() ) );
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

                // Average over all runs on this level
                log( String.format( formatBody, level, config.toString(),
                        areaStats.avg(), areaStats.min, areaStats.max,
                        rangeStats.avg(), rangeStats.min, rangeStats.max,
                        maxDepthStats.avg(), maxDepthStats.min, maxDepthStats.max ) );
            }
        }
    }

    /**
     * This test can be uses to reproduce a bug with a single search envelope, if {@link #shouldHaveReasonableCoveredArea()}
     * fails an assertion. It should be ignored by default.
     */
    @Ignore
    public void debugSingle()
    {
        final int xmin = -100;
        final int xmax = 100;
        final int ymin = -100;
        final int ymax = 100;

        final int level = 1;
        final double xStart = -0x1.9p6;
        final double xEnd = -0x1.8ffffd60e94eep6;
        final double yStart = 0x1.8ff5c28f5c28ep6;
        final double yEnd = 0x1.8ffffffffffffp6;

        Envelope envelope = new Envelope( xmin, xmax, ymin, ymax );
        HilbertSpaceFillingCurve2D curve = new HilbertSpaceFillingCurve2D( envelope, level );
        Envelope searchEnvelope = new Envelope( xStart, xEnd, yStart, yEnd );
        HistogramMonitor monitor = new HistogramMonitor( curve.getMaxLevel() );
        List<SpaceFillingCurve.LongRange> ranges = curve.getTilesIntersectingEnvelope( searchEnvelope, new StandardConfiguration(), monitor );

        log( String.format( "Results for level %d, with x=[%f,%f] y=[%f,%f]\n", level, xStart, xEnd, yStart, yEnd ) );
        log( String.format( "Search size vs covered size: %d vs %d\n", monitor.getSearchArea(), monitor.getCoveredArea() ) );
        log( "Ranges: " + ranges.size() );
        int[] counts = monitor.getCounts();
        for ( int i = 0; i <= curve.getMaxLevel(); i++ )
        {
            log( "\t" + i + "\t" + counts[i] );
        }
    }

    @Test
    public void shouldGet2DHilbertSearchTilesForCenterRangeAndTraverseToBottom()
    {
        TraverseToBottomConfiguration configuration = new TraverseToBottomConfiguration();
        Envelope envelope = new Envelope( -8, 8, -8, 8 );
        for ( int level = 2; level <= 11; level++ )  // 12 takes 6s, 13 takes 25s, 14 takes 100s, 15 takes over 400s
        {
            HilbertSpaceFillingCurve2D curve = new HilbertSpaceFillingCurve2D( envelope, level );
            double fullTile = curve.getTileWidth( 0, level );
            double halfTile = fullTile / 2.0;
            Envelope centerWithoutOuterRing = new Envelope( envelope.getMin( 0 ) + fullTile + halfTile, envelope.getMax( 0 ) - fullTile - halfTile,
                    envelope.getMin( 1 ) + fullTile + halfTile, envelope.getMax( 1 ) - fullTile - halfTile );
            long start = System.currentTimeMillis();
            List<SpaceFillingCurve.LongRange> result = curve.getTilesIntersectingEnvelope( centerWithoutOuterRing, configuration, null );
            log( "Hilbert query at level " + level + " took " + (System.currentTimeMillis() - start) + "ms to produce " + result.size() + " tiles" );
            assertTiles( result, tilesNotTouchingOuterRing( curve ).toArray( new SpaceFillingCurve.LongRange[0] ) );
        }
    }

    @Test
    public void shouldGiveRangesWithinMaxValuesWhenMatchingWholeEnvelopeAtMaxLevel()
    {
        Envelope envelope = new Envelope( -8, 8, -8, 8 );
        HilbertSpaceFillingCurve2D curve = new HilbertSpaceFillingCurve2D( envelope );
        List<SpaceFillingCurve.LongRange> ranges = curve.getTilesIntersectingEnvelope( envelope );
        assertThat( ranges.size(), equalTo( 1 ) );
        assertThat( ranges.get( 0 ).max, lessThan( Long.MAX_VALUE ) );
        assertThat( ranges.get( 0 ).min, greaterThan( Long.MIN_VALUE ) );
    }

    private List<SpaceFillingCurve.LongRange> tilesNotTouchingOuterRing( SpaceFillingCurve curve )
    {
        ArrayList<SpaceFillingCurve.LongRange> expected = new ArrayList<>();
        HashSet<Long> outerRing = new HashSet<>();
        for ( int x = 0; x < curve.getWidth(); x++ )
        {
            // Adding top and bottom rows
            outerRing.add( curve.derivedValueFor( new long[]{x, 0} ) );
            outerRing.add( curve.derivedValueFor( new long[]{x, curve.getWidth() - 1} ) );
        }
        for ( int y = 0; y < curve.getWidth(); y++ )
        {
            // adding left and right rows
            outerRing.add( curve.derivedValueFor( new long[]{0, y} ) );
            outerRing.add( curve.derivedValueFor( new long[]{curve.getWidth() - 1, y} ) );
        }
        for ( long derivedValue = 0; derivedValue < curve.getValueWidth(); derivedValue++ )
        {
            if ( !outerRing.contains( derivedValue ) )
            {
                SpaceFillingCurve.LongRange current = (expected.size() > 0) ? expected.get( expected.size() - 1 ) : null;
                if ( current != null && current.max == derivedValue - 1 )
                {
                    current.expandToMax( derivedValue );
                }
                else
                {
                    current = new SpaceFillingCurve.LongRange( derivedValue );
                    expected.add( current );
                }
            }
        }
        return expected;
    }

    //
    // Set of tests for 3D HilbertCurve at various levels
    //

    @Test
    public void shouldRotate3DNPointsLeft()
    {
        assertThat( rotateNPointLeft( 0b000 ), equalTo( 0b000 ) );
        assertThat( rotateNPointLeft( 0b001 ), equalTo( 0b010 ) );
        assertThat( rotateNPointLeft( 0b010 ), equalTo( 0b100 ) );
        assertThat( rotateNPointLeft( 0b100 ), equalTo( 0b001 ) );
        assertThat( rotateNPointLeft( 0b011 ), equalTo( 0b110 ) );
        assertThat( rotateNPointLeft( 0b110 ), equalTo( 0b101 ) );
        assertThat( rotateNPointLeft( 0b101 ), equalTo( 0b011 ) );
        assertThat( rotateNPointLeft( 0b111 ), equalTo( 0b111 ) );
    }

    @Test
    public void shouldRotate3DNPointsRight()
    {
        assertThat( rotateNPointRight( 0b000 ), equalTo( 0b000 ) );
        assertThat( rotateNPointRight( 0b001 ), equalTo( 0b100 ) );
        assertThat( rotateNPointRight( 0b100 ), equalTo( 0b010 ) );
        assertThat( rotateNPointRight( 0b010 ), equalTo( 0b001 ) );
        assertThat( rotateNPointRight( 0b011 ), equalTo( 0b101 ) );
        assertThat( rotateNPointRight( 0b101 ), equalTo( 0b110 ) );
        assertThat( rotateNPointRight( 0b110 ), equalTo( 0b011 ) );
        assertThat( rotateNPointRight( 0b111 ), equalTo( 0b111 ) );
    }

    @Test
    public void shouldCreateSimple3DHilbertCurveAtLevel1()
    {
        Envelope envelope = new Envelope( new double[]{-8, -8, -8}, new double[]{8, 8, 8} );
        HilbertSpaceFillingCurve3D curve = new HilbertSpaceFillingCurve3D( envelope, 1 );
        assertAtLevel( curve, envelope );
        assertRange( 2, 0, curve, 0, 0, 0 );
        assertRange( 2, 1, curve, 0, 1, 0 );
        assertRange( 2, 2, curve, 0, 1, 1 );
        assertRange( 2, 3, curve, 0, 0, 1 );
        assertRange( 2, 4, curve, 1, 0, 1 );
        assertRange( 2, 5, curve, 1, 1, 1 );
        assertRange( 2, 6, curve, 1, 1, 0 );
        assertRange( 2, 7, curve, 1, 0, 0 );
    }

    @Test
    public void shouldCreateSimple3DHilbertCurveAtLevel2()
    {
        Envelope envelope = new Envelope( new double[]{-8, -8, -8}, new double[]{8, 8, 8} );
        HilbertSpaceFillingCurve3D curve = new HilbertSpaceFillingCurve3D( envelope, 2 );
        assertAtLevel( curve, envelope );
        assertRange( 4, 0, curve, 0, 0, 0 );
        assertRange( 4, 1, curve, 0, 0, 1 );
        assertRange( 4, 2, curve, 1, 0, 1 );
        assertRange( 4, 3, curve, 1, 0, 0 );
        assertRange( 4, 4, curve, 1, 1, 0 );
        assertRange( 4, 5, curve, 1, 1, 1 );
        assertRange( 4, 6, curve, 0, 1, 1 );
        assertRange( 4, 7, curve, 0, 1, 0 );
        assertRange( 4, 8, curve, 0, 2, 0 );
        assertRange( 4, 9, curve, 1, 2, 0 );
        assertRange( 4, 10, curve, 1, 3, 0 );
        assertRange( 4, 11, curve, 0, 3, 0 );
        assertRange( 4, 12, curve, 0, 3, 1 );
        assertRange( 4, 13, curve, 1, 3, 1 );
        assertRange( 4, 14, curve, 1, 2, 1 );
        assertRange( 4, 15, curve, 0, 2, 1 );
        assertRange( 4, 63, curve, 3, 0, 0 );
    }

    @Test
    public void shouldCreateSimple3DHilbertCurveAtLevel3()
    {
        Envelope envelope = new Envelope( new double[]{-8, -8, -8}, new double[]{8, 8, 8} );
        HilbertSpaceFillingCurve3D curve = new HilbertSpaceFillingCurve3D( envelope, 3 );
        assertAtLevel( curve, envelope );
        assertRange( 8, 0, curve, 0, 0, 0 );
        assertRange( 8, (long) Math.pow( 8, 3 ) - 1, curve, 7, 0, 0 );
    }

    @Test
    public void shouldCreateSimple3DHilbertCurveAtLevel4()
    {
        Envelope envelope = new Envelope( new double[]{-8, -8, -8}, new double[]{8, 8, 8} );
        HilbertSpaceFillingCurve3D curve = new HilbertSpaceFillingCurve3D( envelope, 4 );
        assertAtLevel( curve, envelope );
        assertRange( 16, 0, curve, 0, 0, 0 );
        assertRange( 16, (long) Math.pow( 8, 4 ) - 1, curve, 15, 0, 0 );
    }

    @Test
    public void shouldCreateSimple3DHilbertCurveAtLevel5()
    {
        Envelope envelope = new Envelope( new double[]{-8, -8, -8}, new double[]{8, 8, 8} );
        HilbertSpaceFillingCurve3D curve = new HilbertSpaceFillingCurve3D( envelope, 5 );
        assertAtLevel( curve, envelope );
        assertRange( 32, 0, curve, 0, 0, 0 );
        assertRange( 32, (long) Math.pow( 8, 5 ) - 1, curve, 31, 0, 0 );
    }

    @Test
    public void shouldCreateSimple3DHilbertCurveAtLevel6()
    {
        Envelope envelope = new Envelope( new double[]{-8, -8, -8}, new double[]{8, 8, 8} );
        HilbertSpaceFillingCurve3D curve = new HilbertSpaceFillingCurve3D( envelope, 6 );
        assertAtLevel( curve, envelope );
        assertRange( 64, 0, curve, 0, 0, 0 );
        assertRange( 64, (long) Math.pow( 8, 6 ) - 1, curve, 63, 0, 0 );
    }

    @Test
    public void shouldCreateSimple3DHilbertCurveAtLevel7()
    {
        Envelope envelope = new Envelope( new double[]{-8, -8, -8}, new double[]{8, 8, 8} );
        HilbertSpaceFillingCurve3D curve = new HilbertSpaceFillingCurve3D( envelope, 7 );
        assertAtLevel( curve, envelope );
        assertRange( 128, 0, curve, 0, 0, 0 );
        assertRange( 128, (long) Math.pow( 8, 7 ) - 1, curve, 127, 0, 0 );
    }

    @Test
    public void shouldCreateSimple3DHilbertCurveAtManyLevels()
    {
        Envelope envelope = new Envelope( new double[]{-8, -8, -8}, new double[]{8, 8, 8} );
        for ( int level = 1; level <= HilbertSpaceFillingCurve3D.MAX_LEVEL; level++ )
        {
            HilbertSpaceFillingCurve3D curve = new HilbertSpaceFillingCurve3D( envelope, level );
            log( "Max value at level " + level + ": " + curve.getValueWidth() );
            assertAtLevel( curve, envelope );
            assertRange( (int) curve.getWidth(), 0, curve, 0, 0, 0 );
            assertRange( (int) curve.getWidth(), curve.getValueWidth() - 1, curve, (int) curve.getWidth() - 1, 0, 0 );
        }
    }

    @Test
    public void shouldCreateSimple3DHilbertCurveAtLevelDefault()
    {
        Envelope envelope = new Envelope( new double[]{-8, -8, -8}, new double[]{8, 8, 8} );
        assertAtLevel( new HilbertSpaceFillingCurve3D( envelope ), envelope );
    }

    @Test
    public void shouldCreate3DHilbertCurveWithRectangularEnvelope()
    {
        Envelope envelope = new Envelope( new double[]{-8, -20, -15}, new double[]{8, 0, 15} );
        assertAtLevel( new HilbertSpaceFillingCurve3D( envelope ), envelope );
    }

    @Test
    public void shouldCreate3DHilbertCurveWithNonCenteredEnvelope()
    {
        Envelope envelope = new Envelope( new double[]{2, 2, 2}, new double[]{7, 7, 7} );
        assertAtLevel( new HilbertSpaceFillingCurve3D( envelope ), envelope );
    }

    @Test
    public void shouldWorkWithNormalGPSCoordinatesAndHeight()
    {
        Envelope envelope = new Envelope( new double[]{-180, -90, 0}, new double[]{180, 90, 10000} );
        HilbertSpaceFillingCurve3D curve = new HilbertSpaceFillingCurve3D( envelope );
        assertAtLevel( curve, envelope );
    }

    @Test
    public void shouldGet3DSearchTilesForLevel1()
    {
        Envelope envelope = new Envelope( new double[]{-8, -8, -8}, new double[]{8, 8, 8} );
        HilbertSpaceFillingCurve3D curve = new HilbertSpaceFillingCurve3D( envelope, 1 );
        assertTiles( curve.getTilesIntersectingEnvelope( new Envelope( new double[]{-6, -6, -6}, new double[]{-5, -5, -5} ) ),
                new SpaceFillingCurve.LongRange( 0, 0 ) );
        assertTiles( curve.getTilesIntersectingEnvelope( new Envelope( new double[]{0, -6, -6}, new double[]{6, -5, -5} ) ),
                new SpaceFillingCurve.LongRange( 7, 7 ) );
        assertTiles( curve.getTilesIntersectingEnvelope( new Envelope( new double[]{-6, -5, -5}, new double[]{4, -2, -2} ) ),
                new SpaceFillingCurve.LongRange( 0, 0 ), new SpaceFillingCurve.LongRange( 7, 7 ) );
        assertTiles( curve.getTilesIntersectingEnvelope( new Envelope( new double[]{-2, -6, -2}, new double[]{-1, 5, -1} ) ),
                new SpaceFillingCurve.LongRange( 0, 1 ) );
        assertTiles( curve.getTilesIntersectingEnvelope( new Envelope( new double[]{-2, -1, -1}, new double[]{-1, 1, 1} ) ),
                new SpaceFillingCurve.LongRange( 0, 3 ) );
        assertTiles( curve.getTilesIntersectingEnvelope( new Envelope( new double[]{-1, -1, -1}, new double[]{1, 1, 1} ) ),
                new SpaceFillingCurve.LongRange( 0, 7 ) );
    }

    @Test
    public void shouldGet3DSearchTilesForLevel2()
    {
        Envelope envelope = new Envelope( new double[]{-8, -8, -8}, new double[]{8, 8, 8} );
        HilbertSpaceFillingCurve3D curve = new HilbertSpaceFillingCurve3D( envelope, 2 );
        int[] mid = new int[]{5, 14, 17, 28, 35, 46, 49, 58};
        SpaceFillingCurve.LongRange[] midRanges = new SpaceFillingCurve.LongRange[mid.length];
        for ( int i = 0; i < mid.length; i++ )
        {
            midRanges[i] = new SpaceFillingCurve.LongRange( mid[i], mid[i] );
        }
        assertTiles( curve.getTilesIntersectingEnvelope( new Envelope( new double[]{-6, -6, -6}, new double[]{-5, -5, -5} ) ),
                new SpaceFillingCurve.LongRange( 0, 0 ) );
        assertTiles( curve.getTilesIntersectingEnvelope( new Envelope( new double[]{4, -6, -6}, new double[]{6, -5, -5} ) ),
                new SpaceFillingCurve.LongRange( 63, 63 ) );
        assertTiles( curve.getTilesIntersectingEnvelope( new Envelope( new double[]{-6, -5, -5}, new double[]{4, -2, -2} ) ),
                new SpaceFillingCurve.LongRange( 0, 7 ), new SpaceFillingCurve.LongRange( 56, 63 ) );
        assertTiles( curve.getTilesIntersectingEnvelope( new Envelope( new double[]{-2, -6, -2}, new double[]{-1, 5, -1} ) ),
                new SpaceFillingCurve.LongRange( 2, 2 ), new SpaceFillingCurve.LongRange( 5, 5 ), new SpaceFillingCurve.LongRange( 13, 14 ) );
        assertTiles( curve.getTilesIntersectingEnvelope( new Envelope( new double[]{-8, -3, -3}, new double[]{-1, 3, 3} ) ),
                new SpaceFillingCurve.LongRange( 5, 6 ), new SpaceFillingCurve.LongRange( 14, 17 ), new SpaceFillingCurve.LongRange( 27, 28 ) );
        assertTiles( curve.getTilesIntersectingEnvelope( new Envelope( new double[]{-1, -1, -1}, new double[]{1, 1, 1} ) ), midRanges );
    }

    @Test
    public void shouldGet3DSearchTilesForLevel3()
    {
        Envelope envelope = new Envelope( new double[]{-8, -8, -8}, new double[]{8, 8, 8} );
        HilbertSpaceFillingCurve3D curve = new HilbertSpaceFillingCurve3D( envelope, 3 );
        assertTiles( curve.getTilesIntersectingEnvelope( new Envelope( new double[]{-8, -8, -8}, new double[]{-7, -7, -7} ) ),
                new SpaceFillingCurve.LongRange( 0, 0 ) );
        assertTiles( curve.getTilesIntersectingEnvelope( new Envelope( new double[]{7, -8, -8}, new double[]{8, -7, -7} ) ),
                new SpaceFillingCurve.LongRange( 511, 511 ) );
        assertTiles( curve.getTilesIntersectingEnvelope( new Envelope( new double[]{-8, -8, -8}, new double[]{7, 7, 7} ) ),
                new SpaceFillingCurve.LongRange( 0, 511 ) );
    }

    @Test
    public void shouldGet3DSearchTilesForManyLevels()
    {
        Envelope envelope = new Envelope( new double[]{-8, -8, -8}, new double[]{8, 8, 8} );
        for ( int level = 1; level <= HilbertSpaceFillingCurve3D.MAX_LEVEL; level++ )
        {
            HilbertSpaceFillingCurve3D curve = new HilbertSpaceFillingCurve3D( envelope, level );
            double halfTile = curve.getTileWidth( 0, level ) / 2.0;
            long start = System.currentTimeMillis();
            assertTiles(
                    curve.getTilesIntersectingEnvelope( new Envelope( new double[]{-8, -8, -8}, new double[]{-8 + halfTile, -8 + halfTile, -8 + halfTile} ) ),
                    new SpaceFillingCurve.LongRange( 0, 0 ) );
            assertTiles(
                    curve.getTilesIntersectingEnvelope( new Envelope( new double[]{8 - halfTile, -8, -8}, new double[]{8, -8 + halfTile, -8 + halfTile} ) ),
                    new SpaceFillingCurve.LongRange( curve.getValueWidth() - 1, curve.getValueWidth() - 1 ) );
            //TODO: There is a performance issue building the full range when the search envelope hits a very wide part of the extent
            // Suggestion to fix this with shallower traversals
            //assertTiles(curve.getTilesIntersectingEnvelope(new Envelope(new double[]{-8, -8, -8}, new double[]{8, 8, 8})),
            // new HilbertSpaceFillingCurve.LongRange(0, curve.getValueWidth() - 1));
            log( "Hilbert query at level " + level + " took " + (System.currentTimeMillis() - start) + "ms" );
        }
    }

    @Test
    public void shouldStepMoreThanDistanceOneForZOrderOnlyHalfTime()
    {
        Envelope envelope = new Envelope( new double[]{-8, -8}, new double[]{8, 8} );
        for ( int level = 1; level < 8; level++ )
        { // more than 8 takes way too long
            ZOrderSpaceFillingCurve2D curve = new ZOrderSpaceFillingCurve2D( envelope, level );
            shouldNeverStepMoreThanDistanceOne( curve, level, 50 );
        }
    }

    @Test
    public void shouldNeverStepMoreThanDistanceOneForHilbert2D()
    {
        Envelope envelope = new Envelope( new double[]{-8, -8}, new double[]{8, 8} );
        for ( int level = 1; level < 8; level++ )
        { // more than 8 takes way too long
            HilbertSpaceFillingCurve2D curve = new HilbertSpaceFillingCurve2D( envelope, level );
            shouldNeverStepMoreThanDistanceOne( curve, level, 0 );
        }
    }

    @Test
    public void shouldNotStepMoreThanDistanceOneMoreThan10Percent()
    {
        Envelope envelope = new Envelope( new double[]{-8, -8, -8}, new double[]{8, 8, 8} );
        for ( int level = 1; level < 8; level++ )
        { // more than 8 takes way too long
            HilbertSpaceFillingCurve3D curve = new HilbertSpaceFillingCurve3D( envelope, level );
            shouldNeverStepMoreThanDistanceOne( curve, level, 10 );
        }
    }

    private void shouldNeverStepMoreThanDistanceOne( SpaceFillingCurve curve, int level, int badnessThresholdPercentage )
    {
        int badCount = 0;
        long[] previous = null;
        for ( long derivedValue = 0; derivedValue < curve.getValueWidth(); derivedValue++ )
        {
            long[] point = curve.normalizedCoordinateFor( derivedValue, level );
            if ( previous != null )
            {
                double distance = 0;
                for ( int i = 0; i < point.length; i++ )
                {
                    distance += Math.pow( point[i] - previous[i], 2 );
                }
                distance = Math.sqrt( distance );
                if ( distance > 1.0 )
                {
                    badCount++;
                }
//                    assertThat("Distance at level:" + level + " between " + strOf(derivedValue, point) + " and " + strOf(derivedValue - 1, previous)
//                              + " should be 1.0", distance, equalTo(1.0D));
            }
            previous = point;
        }
        int badness = (int) (100 * badCount / (curve.getValueWidth() - 1));
        assertThat( "Bad distance percentage should never be greater than " + badnessThresholdPercentage + "%", badness,
                lessThanOrEqualTo( badnessThresholdPercentage ) );
        log( String.format( "Bad distance count for level: %d (%d/%d = %d%%)", level, badCount, curve.getValueWidth() - 1, badness ) );
    }

    //
    // Test utilities and grouped/complex assertions for 2D and 3D Hilbert Curves
    //

    private void populateChildren( SpaceFillingCurve.CurveRule c, int level )
    {
        int sizeBefore = HilbertSpaceFillingCurve3D.curves.size();
        for ( int i = 0; i < c.length(); ++i )
        {
            SpaceFillingCurve.CurveRule curve = c.childAt( i );
            int sizeAfter = HilbertSpaceFillingCurve3D.curves.size();
            if ( sizeAfter - sizeBefore > 0 )
            {
                populateChildren( curve, level + 1 );
            }
        }
    }

    private void printMapping()
    {
        HashMap<Integer,Map<SubCurve3D,HilbertCurve3D>> map = new HashMap<>();
        for ( Map.Entry<SubCurve3D,HilbertCurve3D> entry : HilbertSpaceFillingCurve3D.curves.entrySet() )
        {
            int start = entry.getValue().npointForIndex( 0 );
            Map<SubCurve3D,HilbertCurve3D> mapEntry;
            if ( map.containsKey( start ) )
            {
                mapEntry = map.get( start );
            }
            else
            {
                mapEntry = new HashMap<>();
                map.put( start, mapEntry );
            }
            mapEntry.put( entry.getKey(), entry.getValue() );
        }
        ArrayList<Integer> sortedKeys = new ArrayList<>();
        sortedKeys.addAll( map.keySet() );
        Collections.sort( sortedKeys );
        for ( Integer start : sortedKeys )
        {
            log( HilbertCurve3D.binaryString( start ) + ":\t" + map.get( start ).size() );
            for ( Map.Entry<SubCurve3D,HilbertCurve3D> mapEntry : map.get( start ).entrySet() )
            {
                log( "\t" + mapEntry.getKey() + ":\t" + Arrays.toString( mapEntry.getValue().children ) );
            }
        }
    }

    private void assertTiles( List<SpaceFillingCurve.LongRange> results, List<SpaceFillingCurve.LongRange> expected )
    {
        assertTiles( results, expected.toArray( new SpaceFillingCurve.LongRange[expected.size()] ) );
    }

    private void assertTiles( List<SpaceFillingCurve.LongRange> results, SpaceFillingCurve.LongRange... expected )
    {
        assertThat( "Result differ: " + results + " != " + Arrays.toString( expected ), results.size(), equalTo( expected.length ) );
        for ( int i = 0; i < results.size(); i++ )
        {
            assertThat( "Result at " + i + " should be the same", results.get( i ), equalTo( expected[i] ) );
        }
    }

    private Envelope getTileEnvelope( Envelope envelope, int divisor, int... index )
    {
        double[] widths = envelope.getWidths( divisor );
        double[] min = Arrays.copyOf( envelope.getMin(), envelope.getDimension() );
        double[] max = Arrays.copyOf( envelope.getMin(), envelope.getDimension() );
        for ( int i = 0; i < min.length; i++ )
        {
            min[i] += index[i] * widths[i];
            max[i] += (index[i] + 1) * widths[i];
        }
        return new Envelope( min, max );
    }

    private void assertRange( String message, ZOrderSpaceFillingCurve2D curve, Envelope range, long value )
    {
        for ( double x = range.getMinX(); x < range.getMaxX(); x += 1.0 )
        {
            for ( double y = range.getMinY(); y < range.getMaxY(); y += 1.0 )
            {
                assertCurveAt( message, curve, value, x, y );
            }
        }
    }

    private void assertRange( String message, HilbertSpaceFillingCurve2D curve, Envelope range, long value )
    {
        for ( double x = range.getMinX(); x < range.getMaxX(); x += 1.0 )
        {
            for ( double y = range.getMinY(); y < range.getMaxY(); y += 1.0 )
            {
                assertCurveAt( message, curve, value, x, y );
            }
        }
    }

    private void assertRange( String message, HilbertSpaceFillingCurve3D curve, Envelope range, long value )
    {
        for ( double x = range.getMin( 0 ); x < range.getMax( 0 ); x += 1.0 )
        {
            for ( double y = range.getMin( 1 ); y < range.getMax( 1 ); y += 1.0 )
            {
                for ( double z = range.getMin( 2 ); z < range.getMax( 2 ); z += 1.0 )
                {
                    assertCurveAt( message, curve, value, x, y, z );
                }
            }
        }
    }

    private void assertRange( int divisor, long value, ZOrderSpaceFillingCurve2D curve, int... index )
    {
        Envelope range = getTileEnvelope( curve.getRange(), divisor, index );
        String message = Arrays.toString( index ) + " should evaluate to " + value;
        assertRange( message, curve, range, value );
    }

    private void assertRange( int divisor, long value, HilbertSpaceFillingCurve2D curve, int... index )
    {
        Envelope range = getTileEnvelope( curve.getRange(), divisor, index );
        String message = Arrays.toString( index ) + " should evaluate to " + value;
        assertRange( message, curve, range, value );
    }

    private void assertRange( int divisor, long value, HilbertSpaceFillingCurve3D curve, int... index )
    {
        Envelope range = getTileEnvelope( curve.getRange(), divisor, index );
        String message = Arrays.toString( index ) + " should evaluate to " + value;
        assertRange( message, curve, range, value );
    }

    private void assertCurveAt( String message, SpaceFillingCurve curve, long value, double... coord )
    {
        double[] halfTileWidths = new double[coord.length];
        for ( int i = 0; i < coord.length; i++ )
        {
            halfTileWidths[i] = curve.getTileWidth( i, curve.getMaxLevel() ) / 2.0;
        }
        long result = curve.derivedValueFor( coord );
        double[] coordinate = curve.centerPointFor( result );
        assertThat( message + ": " + Arrays.toString( coord ), result, equalTo( value ) );
        for ( int i = 0; i < coord.length; i++ )
        {
            assertThat( message + ": " + Arrays.toString( coord ), Math.abs( coordinate[i] - coord[i] ), lessThanOrEqualTo( halfTileWidths[i] ) );
        }
    }

    private void assert2DAtLevel( Envelope envelope, int level )
    {
        assertAtLevel( new HilbertSpaceFillingCurve2D( envelope, level ), envelope );
    }

    private void assertAtLevel( ZOrderSpaceFillingCurve2D curve, Envelope envelope )
    {
        int level = curve.getMaxLevel();
        long width = (long) Math.pow( 2, level );
        long valueWidth = width * width;
        double justInsideMaxX = envelope.getMaxX() - curve.getTileWidth( 0, level ) / 2.0;
        double justInsideMaxY = envelope.getMaxY() - curve.getTileWidth( 1, level ) / 2.0;
        double midX = (envelope.getMinX() + envelope.getMaxX()) / 2.0;
        double midY = (envelope.getMinY() + envelope.getMaxY()) / 2.0;

        long topRight = 1L;
        long topRightFactor = 2L;
        long topRightDiff = 1;
        String topRightDescription = "1";
        for ( int l = 0; l < level; l++ )
        {
            topRight = topRightFactor - topRightDiff;
            topRightDescription = String.valueOf( topRightFactor ) + " - " + topRightDescription;
            topRightDiff = topRightFactor + topRightDiff;
            topRightFactor *= 4;
        }

        assertThat( "Level " + level + " should have width of " + width, curve.getWidth(), equalTo( width ) );
        assertThat( "Level " + level + " should have max value of " + valueWidth, curve.getValueWidth(), equalTo( valueWidth ) );

        assertCurveAt( "Top-left should evaluate to zero", curve, 0, envelope.getMinX(), envelope.getMaxY() );
        assertCurveAt( "Just inside right edge on the bottom should evaluate to max-value", curve, curve.getValueWidth() - 1, justInsideMaxX,
                envelope.getMinY() );
        assertCurveAt( "Just inside top-right corner should evaluate to " + topRightDescription, curve, topRight, justInsideMaxX, justInsideMaxY );
        assertCurveAt( "Right on top-right corner should evaluate to " + topRightDescription, curve, topRight, envelope.getMaxX(), envelope.getMaxY() );
        assertCurveAt( "Bottom-right should evaluate to max-value", curve, curve.getValueWidth() - 1, envelope.getMaxX(), envelope.getMinY() );
        assertCurveAt( "Max x-value, middle y-value should evaluate to (maxValue-1) / 2", curve, (curve.getValueWidth() - 1) / 2, envelope.getMaxX(), midY );
    }

    private void assertAtLevel( HilbertSpaceFillingCurve2D curve, Envelope envelope )
    {
        int level = curve.getMaxLevel();
        long width = (long) Math.pow( 2, level );
        long valueWidth = width * width;
        double justInsideMaxX = envelope.getMaxX() - curve.getTileWidth( 0, level ) / 2.0;
        double justInsideMaxY = envelope.getMaxY() - curve.getTileWidth( 1, level ) / 2.0;
        double midX = (envelope.getMinX() + envelope.getMaxX()) / 2.0;
        double midY = (envelope.getMinY() + envelope.getMaxY()) / 2.0;

        long topRight = 0L;
        long topRightFactor = 2L;
        StringBuilder topRightDescription = new StringBuilder();
        for ( int l = 0; l < level; l++ )
        {
            topRight += topRightFactor;
            if ( topRightDescription.length() == 0 )
            {
                topRightDescription.append( topRightFactor );
            }
            else
            {
                topRightDescription.append( " + " ).append( topRightFactor );
            }
            topRightFactor *= 4;
        }

        assertThat( "Level " + level + " should have width of " + width, curve.getWidth(), equalTo( width ) );
        assertThat( "Level " + level + " should have max value of " + valueWidth, curve.getValueWidth(), equalTo( valueWidth ) );

        assertCurveAt( "Bottom-left should evaluate to zero", curve, 0, envelope.getMinX(), envelope.getMinY() );
        assertCurveAt( "Just inside right edge on the bottom should evaluate to max-value", curve, curve.getValueWidth() - 1, justInsideMaxX,
                envelope.getMinY() );
        assertCurveAt( "Just inside top-right corner should evaluate to " + topRightDescription, curve, topRight, justInsideMaxX, justInsideMaxY );
        assertCurveAt( "Right on top-right corner should evaluate to " + topRightDescription, curve, topRight, envelope.getMaxX(), envelope.getMaxY() );
        assertCurveAt( "Bottom-right should evaluate to max-value", curve, curve.getValueWidth() - 1, envelope.getMaxX(), envelope.getMinY() );
        assertCurveAt( "Middle value should evaluate to (max-value+1) / 2", curve, curve.getValueWidth() / 2, midX, midY );
    }

    private void assertAtLevel( HilbertSpaceFillingCurve3D curve, Envelope envelope )
    {
        int level = curve.getMaxLevel();
        int dimension = curve.rootCurve().dimension;
        long width = (long) Math.pow( 2, level );
        long valueWidth = (long) Math.pow( width, dimension );
        double midY = (envelope.getMax( 1 ) + envelope.getMin( 1 )) / 2.0;
        double[] justInsideMax = new double[dimension];
        double[] midValY = new double[]{envelope.getMin( 1 ), envelope.getMax( 1 )};
        for ( int i = 0; i < level; i++ )
        {
            if ( i % 2 == 0 )
            {
                midValY[1] = (midValY[0] + midValY[1]) / 2.0;
            }
            else
            {
                midValY[0] = (midValY[0] + midValY[1]) / 2.0;
            }
        }
        double[] locationOfHalfCurve = new double[]{(envelope.getMin( 0 ) + envelope.getMax( 0 )) / 2.0,            // mid-way on x
                (midValY[0] + midValY[1]) / 2.0,                                                 // lower y - depending on level
                envelope.getMax( 1 ) - curve.getTileWidth( 2, level ) / 2.0     // near front of z
        };
        for ( int i = 0; i < dimension; i++ )
        {
            justInsideMax[i] = envelope.getMax( i ) - curve.getTileWidth( i, level ) / 2.0;
        }

        long frontRightMid = valueWidth / 2 + valueWidth / 8 + valueWidth / 256;
        String fromRightMidDescription = String.valueOf( valueWidth ) + "/2 + " + valueWidth + "/8";

        assertThat( "Level " + level + " should have width of " + width, curve.getWidth(), equalTo( width ) );
        assertThat( "Level " + level + " should have max value of " + valueWidth, curve.getValueWidth(), equalTo( valueWidth ) );

        assertCurveAt( "Bottom-left should evaluate to zero", curve, 0, envelope.getMin() );
        assertCurveAt( "Just inside right edge on the bottom back should evaluate to max-value", curve, curve.getValueWidth() - 1,
                replaceOne( envelope.getMin(), justInsideMax[0], 0 ) );
        if ( curve.getMaxLevel() < 5 )
        {
            assertCurveAt( "Just above front-right-mid edge should evaluate to " + fromRightMidDescription, curve, frontRightMid,
                    replaceOne( justInsideMax, midY, 1 ) );
            assertCurveAt( "Right on top-right-front corner should evaluate to " + fromRightMidDescription, curve, frontRightMid,
                    replaceOne( envelope.getMax(), midY, 1 ) );
        }
        assertCurveAt( "Bottom-right-back should evaluate to max-value", curve, curve.getValueWidth() - 1,
                replaceOne( envelope.getMin(), envelope.getMax( 0 ), 0 ) );
        if ( curve.getMaxLevel() < 3 )
        {
            assertCurveAt( "Middle value should evaluate to (max-value+1) / 2", curve, curve.getValueWidth() / 2, locationOfHalfCurve );
        }
    }

    private double[] replaceOne( double[] values, double value, int index )
    {
        double[] newValues = Arrays.copyOf( values, values.length );
        newValues[index] = value;
        return newValues;
    }
}
