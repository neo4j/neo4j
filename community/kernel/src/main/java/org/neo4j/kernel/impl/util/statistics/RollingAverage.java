/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.impl.util.statistics;

import org.neo4j.helpers.ArrayUtil;

import java.io.Serializable;

/** Takes a stream of inputs, tracks the average over time, giving more weight to more recent data. */
public class RollingAverage implements Serializable
{
    private static final long serialVersionUID = -230424883810571860L;

    private final Parameters parameters;

    /** Tracks averages across 8 windows, index 0 is the current one used. */
    private final double averages[] = new double[8];
    private int populatedWindows = 1;

    private long samplesInCurrentWindow = 0;

    public RollingAverage( Parameters parameters )
    {
        this.parameters = parameters;
    }

    public void record( long value )
    {
        averages[0] = (averages[0] * samplesInCurrentWindow + value) / (samplesInCurrentWindow + 1);

        // Do we need to move the window?
        if(++samplesInCurrentWindow > parameters.windowSize)
        {
            // Shift averages right, dropping the oldest on the floor
            double previous = averages[0], current;
            for ( int i = 1; i < averages.length - 1; i++ )
            {
                current = averages[i];
                averages[i] = previous;
                previous = current;
            }

            // Don't 0 this out entirely, otherwise we'd get very skewed numbers for a while after a window shift
            samplesInCurrentWindow = samplesInCurrentWindow / 4;
            populatedWindows = Math.min( populatedWindows+1, averages.length );
        }
    }

    public double average()
    {
        double average = 0;
        for ( int i = 0; i < populatedWindows; i++ )
        {
            long weightMultiplier = (long) Math.pow( (populatedWindows - i), 2 );
            average += averages[i] * weightMultiplier;
        }
        int totalWeightMultipliers = (populatedWindows * (populatedWindows + 1) * (populatedWindows * 2 + 1)) / 6;
        return average / totalWeightMultipliers;
    }


    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RollingAverage other = (RollingAverage) o;

        return
            populatedWindows == other.populatedWindows &&
            parameters.equals( other.parameters ) &&
            ArrayUtil.approximatelyEqual( averages, other.averages, parameters.equalityTolerance );
    }

    @Override
    public int hashCode()
    {
        int result = parameters.hashCode();
        result = 31 * result + populatedWindows;
        return result;
    }

    public static class Parameters implements Serializable
    {
        private static final long serialVersionUID = 8909006975623003761L;

        public static final int DEFAULT_WINDOW_SIZE = 1024;
        public static final double DEFAULT_EQUALITY_TOLERANCE = 0.0001d;

        public final long windowSize;
        public final double equalityTolerance;

        public Parameters()
        {
            this( DEFAULT_WINDOW_SIZE, DEFAULT_EQUALITY_TOLERANCE );
        }

        public Parameters( long windowSize, double equalityTolerance )
        {
            this.windowSize = windowSize;
            this.equalityTolerance = equalityTolerance;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
            {
                return true;
            }

            if (o == null || getClass() != o.getClass())
            {
                return false;
            }

            Parameters that = (Parameters) o;

            return
              Double.compare( that.equalityTolerance, equalityTolerance ) == 0
              && windowSize == that.windowSize;
        }

        @Override
        public int hashCode()
        {
            int result = (int) ( windowSize ^ ( windowSize >>> 32 ) ) ;
            long temp = Double.doubleToLongBits( equalityTolerance );
            result = 31 * result + (int) ( temp ^ ( temp >>> 32 ) );
            return result;
        }
    }
}
