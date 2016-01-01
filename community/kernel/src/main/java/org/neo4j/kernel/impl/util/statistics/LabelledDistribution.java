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

import org.neo4j.helpers.collection.MapUtil;

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
    private static final long serialVersionUID = -6076855164000786095L;

    private final Map<T, Long> rawData = new HashMap<>();
    private final double equalityTolerance;

    private long total = 0;
    private Map<T, Double> distribution = new HashMap<>();

    public LabelledDistribution( double equalityTolerance )
    {
        this.equalityTolerance = equalityTolerance;
    }

    public double get( T label )
    {
        Double value = distribution.get(label);
        return value == null ? 0.0d : value;
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
            Map<T, Double> newDistribution = new HashMap<>();
            for ( Map.Entry<T, Long> entry : rawData.entrySet() )
            {
                newDistribution.put( entry.getKey(), entry.getValue().doubleValue() / total );
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

    @Override
    public boolean equals( Object obj )
    {
        if ( this == obj )
        {
            return true;
        }

        if ( null == obj || getClass() != obj.getClass() )
        {
            return false;
        }

        LabelledDistribution<T> other = (LabelledDistribution<T>) obj;
        return
          equalityTolerance == other.equalityTolerance &&
          MapUtil.approximatelyEqual( this.distribution, other.distribution, equalityTolerance );
    }

    @Override
    public int hashCode() {
        long temp = Double.doubleToLongBits( equalityTolerance );
        return (int) ( temp ^ ( temp >>> 32 ) );
    }
}
