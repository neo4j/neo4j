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
import java.util.HashMap;
import java.util.Map;

/**
 * A single-writer multiple-reader data structure that tracks the relative distribution of labelled things.
 *
 * Note: This does not pertain to "labels" in the Neo4j data model, it pertains to labels as in a "labeled data set".
 *
 * For each sample recorded, it may fit in zero or more labelled categories, meaning the distributions may overlap.
 * For instance if two samples with labels [8,9] are recorded, both 8 and 9 will have a distribution of 1, since all
 * samples contain both labels.
 */
public class LabelledDistribution<T> implements Serializable
{
    private long total = 0;
    private final Map<T, Long> rawData = new HashMap<>();
    private Map<T, Float> distribution = new HashMap<>();

    public float get( T label )
    {
        if(distribution.containsKey( label ))
        {
            return distribution.get( label );
        }
        else
        {
            return 0.0f;
        }
    }

    public LabelledDistribution<T> record( Iterable<T> labels, int ticks )
    {
        synchronized ( rawData )
        {
            for ( T label : labels )
            {
                Long current = rawData.get( label );
                rawData.put( label, (current == null ? 0 : current) + ticks );
            }
            total += ticks;
            return this;
        }
    }

    public LabelledDistribution<T> record( Iterable<T> labels )
    {
        return record( labels, 1 );
    }

    public LabelledDistribution<T> recalculate()
    {
        synchronized ( rawData )
        {
            Map<T, Float> newDistribution = new HashMap<>();
            for ( Map.Entry<T, Long> entry : rawData.entrySet() )
            {
                newDistribution.put( entry.getKey(), entry.getValue() / (1.0f * total) );
            }
            this.distribution = newDistribution;
            return this;
        }
    }

    @Override
    public String toString()
    {
        return distribution.toString();
    }

    public boolean equals( LabelledDistribution<T> other, float tolerance )
    {
        if(other.distribution.size() != distribution.size())
        {
            return false;
        }

        for ( Map.Entry<T, Float> entry : distribution.entrySet() )
        {
            if(!other.distribution.containsKey( entry.getKey() ))
            {
                return false;
            }

            float otherValue = other.distribution.get( entry.getKey() );
            if(Math.abs(otherValue - entry.getValue()) > tolerance)
            {
                return false;
            }
        }

        return true;
    }
}
