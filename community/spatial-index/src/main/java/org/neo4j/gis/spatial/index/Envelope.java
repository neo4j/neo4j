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
package org.neo4j.gis.spatial.index;

import java.util.Arrays;

public class Envelope
{
    static final double MAXIMAL_ENVELOPE_SIDE_RATIO = 100_000;

    protected final double[] min;
    protected final double[] max;

    /**
     * Copy constructor
     */
    public Envelope( Envelope e )
    {
        this( e.min, e.max );
    }

    /**
     * General constructor for the n-dimensional case
     */
    public Envelope( double[] min, double[] max )
    {
        this.min = min.clone();
        this.max = max.clone();
        if ( !isValid() )
        {
            throw new IllegalArgumentException( "Invalid envelope created " + toString() );
        }
    }

    /**
     * Special constructor for the 2D case
     */
    public Envelope( double xmin, double xmax, double ymin, double ymax )
    {
        this( new double[] { xmin, ymin }, new double[] { xmax, ymax } );
    }

    /**
     * @return a copy of the envelope where the ratio of smallest to largest side is not more than 1:100
     */
    public Envelope withSideRatioNotTooSmall( )
    {
        double[] from = Arrays.copyOf( this.min, min.length );
        double[] to = Arrays.copyOf( this.max, max.length );
        double highestDiff = -Double.MAX_VALUE;
        double[] diffs = new double[from.length];
        for ( int i = 0; i < from.length; i++ )
        {
            diffs[i] = to[i] - from[i];
            highestDiff = Math.max( highestDiff, diffs[i] );
        }
        final double mindiff = highestDiff / MAXIMAL_ENVELOPE_SIDE_RATIO;
        for ( int i = 0; i < from.length; i++ )
        {
            if ( diffs[i] < mindiff )
            {
                to[i] = from[i] + mindiff;
            }
        }
        return new Envelope( from, to );
    }

    public double[] getMin()
    {
        return min;
    }

    public double[] getMax()
    {
        return max;
    }

    public double getMin( int dimension )
    {
        return min[dimension];
    }

    public double getMax( int dimension )
    {
        return max[dimension];
    }

    public double getMinX()
    {
        return getMin(0);
    }

    public double getMaxX()
    {
        return getMax(0);
    }

    public double getMinY()
    {
        return getMin(1);
    }

    public double getMaxY()
    {
        return getMax(1);
    }

    public int getDimension()
    {
        return min.length;
    }

    /**
     * Note that this doesn't exclude the envelope boundary.
     * See JTS Envelope.
     */
    public boolean contains( Envelope other )
    {
        return covers(other);
    }

    public boolean covers( Envelope other )
    {
        boolean covers = getDimension() == other.getDimension();
        for ( int i = 0; i < min.length && covers; i++ )
        {
            covers = other.min[i] >= min[i] && other.max[i] <= max[i];
        }
        return covers;
    }

    public boolean intersects( Envelope other )
    {
        boolean intersects = getDimension() == other.getDimension();
        for ( int i = 0; i < min.length && intersects; i++ )
        {
            intersects = other.min[i] <= max[i] && other.max[i] >= min[i];
        }
        return intersects;
    }

    public void expandToInclude( Envelope other )
    {
        if ( getDimension() != other.getDimension() )
        {
            throw new IllegalArgumentException( "Cannot join Envelopes with different dimensions: " + this.getDimension() + " != " + other.getDimension() );
        }
        else
        {
            for ( int i = 0; i < min.length; i++ )
            {
                if ( other.min[i] < min[i] )
                {
                    min[i] = other.min[i];
                }
                if ( other.max[i] > max[i] )
                {
                    max[i] = other.max[i];
                }
            }
        }
    }

    @Override
    public boolean equals( Object obj )
    {
        if ( obj instanceof Envelope )
        {
            Envelope other = (Envelope) obj;
            if ( this.getDimension() != other.getDimension() )
            {
                return false;
            }
            for ( int i = 0; i < getDimension(); i++ )
            {
                if ( this.min[i] != other.getMin( i ) || this.max[i] != other.getMax( i ) )
                {
                    return false;
                }
            }
            return true;
        }
        else
        {
            return false;
        }
    }

    @Override
    public int hashCode()
    {
        int result = 1;
        for ( double element : min )
        {
            long bits = Double.doubleToLongBits( element );
            result = 31 * result + (int) (bits ^ (bits >>> 32));
        }
        for ( double element : max )
        {
            long bits = Double.doubleToLongBits( element );
            result = 31 * result + (int) (bits ^ (bits >>> 32));
        }
        return result;
    }

    /**
     * Return the distance between the two envelopes on one dimension. This can return negative values if the envelopes intersect on this dimension.
     * @return distance between envelopes
     */
    public double distance( Envelope other, int dimension )
    {
        if ( min[dimension] < other.min[dimension] )
        {
            return other.min[dimension] - max[dimension];
        }
        else
        {
            return min[dimension] - other.max[dimension];
        }
    }

    /**
     * Find the pythagorean distance between two envelopes
     */
    public double distance( Envelope other )
    {
        if ( intersects(other) )
        {
            return 0;
        }

        double distance = 0.0;
        for ( int i = 0; i < min.length; i++ )
        {
            double dist = distance(other, i);
            if ( dist > 0 )
            {
                distance += dist * dist;
            }
        }
        return Math.sqrt(distance);
    }

    /**
     * @return getWidth(0) for special 2D case with the first dimension being x (width)
     */
    public double getWidth()
    {
        return getWidth(0);
    }

    /**
     * Return the width of the envelope at the specified dimension
     * @return with of that dimension, ie. max[d] - min[d]
     */
    public double getWidth( int dimension )
    {
        return max[dimension] - min[dimension];
    }

    /**
     * Return the fractional widths of the envelope at all axes
     *
     * @param divisor the number of segments to divide by (a 2D envelope will be divided into quadrants using 2)
     * @return double array of widths, ie. max[d] - min[d]
     */
    public double[] getWidths( int divisor )
    {
        double[] widths = Arrays.copyOf(max, max.length);
        for ( int d = 0; d < max.length; d++ )
        {
            widths[d] -= min[d];
            widths[d] /= divisor;
        }
        return widths;
    }

    public double getArea()
    {
        double area = 1.0;
        for ( int i = 0; i < min.length; i++ )
        {
            area *= max[i] - min[i];
        }
        return area;
    }

    public double overlap( Envelope other )
    {
        Envelope smallest = this.getArea() < other.getArea() ? this : other;
        Envelope intersection = this.intersection(other);
        return intersection == null ? 0.0 : smallest.isPoint() ? 1.0 : intersection.getArea() / smallest.getArea();
    }

    public boolean isPoint()
    {
        boolean ans = true;
        for ( int i = 0; i < min.length && ans; i++ )
        {
            ans = min[i] == max[i];
        }
        return ans;
    }

    private boolean isValid()
    {
        boolean valid = min != null && max != null && min.length == max.length;
        for ( int i = 0; valid && i < min.length; i++ )
        {
            valid = min[i] <= max[i];
        }
        return valid;
    }

    @Override
    public String toString()
    {
        return "Envelope: min=" + makeString(min) + ", max=" + makeString(max);
    }

    private static String makeString( double[] vals )
    {
        StringBuilder sb = new StringBuilder();
        if ( vals == null )
        {
            sb.append("null");
        }
        else
        {
            for ( double val : vals )
            {
                if ( sb.length() > 0 )
                {
                    sb.append( "," );
                }
                else
                {
                    sb.append( "(" );
                }
                sb.append( val );
            }
            if ( sb.length() > 0 )
            {
                sb.append( ")" );
            }
        }
        return sb.toString();
    }

    public Envelope intersection( Envelope other )
    {
        if ( getDimension() == other.getDimension() )
        {
            double[] iMin = new double[this.min.length];
            double[] iMax = new double[this.min.length];
            Arrays.fill(iMin, Double.NaN);
            Arrays.fill(iMax, Double.NaN);
            boolean result = true;
            for ( int i = 0; i < min.length; i++ )
            {
                if ( other.min[i] <= this.max[i] && other.max[i] >= this.min[i] )
                {
                    iMin[i] = Math.max(this.min[i], other.min[i]);
                    iMax[i] = Math.min(this.max[i], other.max[i]);
                }
                else
                {
                    result = false;
                }
            }
            return result ? new Envelope( iMin, iMax ) : null;
        }
        else
        {
            throw new IllegalArgumentException(
                    "Cannot calculate intersection of Envelopes with different dimensions: " + this.getDimension() + " != " + other.getDimension() );
        }
    }
}
