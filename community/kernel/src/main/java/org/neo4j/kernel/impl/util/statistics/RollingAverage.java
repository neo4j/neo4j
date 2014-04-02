/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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

import java.io.Serializable;

/** Takes a stream of inputs, tracks the average over time, giving more weight to more recent data. */
public class RollingAverage implements Serializable
{
    private final long windowSize;

    /** Tracks averages across 8 windows, index 0 is the current one used. */
    private final double averages[] = new double[8];

    private int populatedWindows = 1;
    private long samplesInCurrentWindow = 0;

    public RollingAverage( long windowSize )
    {
        this.windowSize = windowSize;
    }

    public void record( long value )
    {
        averages[0] = (averages[0] * samplesInCurrentWindow + value) / (samplesInCurrentWindow + 1);

        // Do we need to move the window?
        if(++samplesInCurrentWindow > windowSize)
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
}
