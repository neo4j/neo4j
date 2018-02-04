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
package org.neo4j.gis.spatial.index;

import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class EnvelopeTest
{
    @Test
    public void shouldCreateBasic2DEnvelopes()
    {
        for ( double width = 0.0; width < 10.0; width += 2.5 )
        {
            for ( double minx = -10.0; minx < 10.0; minx += 2.5 )
            {
                for ( double miny = -10.0; miny < 10.0; miny += 2.5 )
                {
                    double maxx = minx + width;
                    double maxy = miny + width;
                    makeAndTestEnvelope( new double[]{minx, miny}, new double[]{maxx, maxy}, new double[]{width, width} );
                }
            }
        }
    }

    @Test
    public void shouldHandleIntersectionsIn1D()
    {
        double width_x = 1.0;
        double width_y = 1.0;
        Envelope left = new Envelope( 0.0, width_x, 0.0, width_y );
        for ( double minx = -10.0; minx < 10.0; minx += 0.2 )
        {
            double maxx = minx + width_x;
            Envelope right = new Envelope( minx, maxx, 0.0, width_y );
            if ( maxx < left.getMinX() || minx > left.getMaxX() )
            {
                testDoesNotOverlap(left, right);
            }
            else
            {
                double overlap_x = (maxx < left.getMaxX()) ? maxx - left.getMinX() : left.getMaxX() - minx;
                double overlap = overlap_x * width_y;
                testOverlaps(left, right, true, overlap);
            }
        }
    }

    private void testDoesNotOverlap( Envelope left, Envelope right )
    {
        Envelope bbox = new Envelope( left );
        bbox.expandToInclude(right);
        testOverlaps(left, right, false, 0.0);
    }

    private void testOverlaps( Envelope left, Envelope right, boolean intersects, double overlap )
    {
        String intersectMessage = intersects ? "Should intersect" : "Should not intersect";
        String overlapMessage = intersects ? "Should overlap" : "Should not have overlap";
        assertThat(intersectMessage, left.intersects(right), equalTo(intersects));
        assertThat(intersectMessage, right.intersects(left), equalTo(intersects));
        assertThat(overlapMessage, left.overlap(right), closeTo(overlap, 0.000001));
        assertThat(overlapMessage, right.overlap(left), closeTo(overlap, 0.000001));
    }

    private void testOverlaps( Envelope left, Envelope right, boolean intersects, double overlap, double overlapArea )
    {
        testOverlaps(left, right, intersects, overlap);
        assertThat("Expected overlap area", left.intersection(right).getArea(), closeTo(overlapArea, 0.000001));
        assertThat("Expected overlap area", right.intersection(left).getArea(), closeTo(overlapArea, 0.000001));
    }

    @Test
    public void shouldHandleIntersectionsIn2D()
    {
        Envelope left = new Envelope( 0.0, 1.0, 0.0, 1.0 );
        testOverlaps(left, new Envelope( 0.0, 1.0, 0.0, 1.0 ), true, 1.0, 1.0);        // copies
        testOverlaps(left, new Envelope( 0.5, 1.0, 0.5, 1.0 ), true, 1.0, 0.25);       // top right quadrant
        testOverlaps(left, new Envelope( 0.25, 0.75, 0.25, 0.75 ), true, 1.0, 0.25);   // centered
        testOverlaps(left, new Envelope( -0.5, 0.5, -0.5, 0.5 ), true, 0.25, 0.25);   // overlaps bottom left quadrant
        testOverlaps(left, new Envelope( -0.5, 1.5, -0.5, 1.5 ), true, 1.0, 1.0);      // encapsulates
        testOverlaps(left, new Envelope( -1.0, 0.0, 0.0, 1.0 ), true, 0.0, 0.0);       // touches left edge
        testOverlaps(left, new Envelope( 0.5, 1.5, 1.0, 2.0 ), true, 0.0, 0.0);        // touches top-right edge
        testOverlaps(left, new Envelope( 0.5, 1.5, 0.0, 1.0 ), true, 0.5, 0.5);        // overlaps right half
    }

    private void makeAndTestEnvelope( double[] min, double[] max, double[] width )
    {
        Envelope env = new Envelope( min, max );
        assertThat("Expected min-x to be correct", env.getMinX(), equalTo(min[0]));
        assertThat("Expected min-y to be correct", env.getMinY(), equalTo(min[1]));
        assertThat("Expected max-x to be correct", env.getMaxX(), equalTo(max[0]));
        assertThat("Expected max-y to be correct", env.getMaxY(), equalTo(max[1]));
        assertThat("Expected dimension to be same as min.length", env.getDimension(), equalTo(min.length));
        assertThat("Expected dimension to be same as max.length", env.getDimension(), equalTo(max.length));
        for ( int i = 0; i < min.length; i++ )
        {
            assertThat("Expected min[" + i + "] to be correct", env.getMin(i), equalTo(min[i]));
            assertThat("Expected max[" + i + "] to be correct", env.getMax(i), equalTo(max[i]));
        }
        double area = 1.0;
        Envelope copy = new Envelope( env );
        Envelope intersection = env.intersection(copy);
        for ( int i = 0; i < min.length; i++ )
        {
            assertThat("Expected width[" + i + "] to be correct", env.getWidth(i), equalTo(width[i]));
            assertThat("Expected copied width[" + i + "] to be correct", copy.getWidth(i), equalTo(width[i]));
            assertThat("Expected intersected width[" + i + "] to be correct", intersection.getWidth(i), equalTo(width[i]));
            area *= width[i];
        }
        assertThat("Expected area to be correct", env.getArea(), equalTo(area));
        assertThat("Expected copied area to be correct", env.getArea(), equalTo(copy.getArea()));
        assertThat("Expected intersected area to be correct", env.getArea(), equalTo(intersection.getArea()));
        assertTrue( env.intersects( copy ), "Expected copied envelope to intersect" );
        assertThat("Expected copied envelope to intersect completely", env.overlap(copy), equalTo(1.0));
    }
}
