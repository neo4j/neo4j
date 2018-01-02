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
package org.neo4j.unsafe.impl.batchimport.staging;

import static java.lang.Math.round;

/**
 * Takes a value range and projects it to a very discrete number of integer values, quantizing based
 * on float precision.
 */
public class QuantizedProjection
{
    private final long max;
    private final long projectedMax;

    private double absoluteWay;
    private long step;

    public QuantizedProjection( long max, long projectedMax )
    {
        this.max = max;
        this.projectedMax = projectedMax;
    }

    /**
     * @param step a part of the max, not the projection.
     * @return {@code true} if the total so far including {@code step} is equal to or less than the max allowed,
     * otherwise {@code false} -- meaning that we stepped beyond max.
     */
    public boolean next( long step )
    {
        double absoluteStep = (double)step / (double)max;
        if ( absoluteWay + absoluteStep > 1f )
        {
            return false;
        }

        long prevProjection = round( absoluteWay * projectedMax );
        absoluteWay += absoluteStep;
        long projection = round( absoluteWay * projectedMax );
        this.step = projection - prevProjection;

        return true;
    }

    public long step()
    {
        return step;
    }
}
