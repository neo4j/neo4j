/**
 * Copyright (c) 2002-2011 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.bench.cases.concurrent;

import org.neo4j.bench.domain.CaseResult;
import org.neo4j.bench.domain.Unit;

/**
 * Result from a single worker. The same thing as normal CaseResult.Metric, except
 * WorkerMetrics can be aggregated together, either by counting a total, or by getting
 * an average.
 */
public class WorkerMetric extends CaseResult.Metric
{

    private final Aggregation aggregation;

    public WorkerMetric( Aggregation aggregation, String name, double value, Unit unit)
    {
        this(aggregation, name, value, unit, false);
    }

    public WorkerMetric( Aggregation aggregation, String name, double value, Unit unit, boolean trackRegression )
    {
        super( name, value, unit, trackRegression );
        this.aggregation = aggregation;
    }

    public Aggregation getAggregation()
    {
        return aggregation;
    }

    public WorkerMetric aggregateWith(WorkerMetric other)
    {
        if(getUnit().equals( other.getUnit())
                && getName().equals( other.getName() ))
        {
            return new WorkerMetric(
                    getAggregation(),
                    getName(),
                    aggregation.aggregate( getValue(), other.getValue() ),
                    getUnit(),
                    shouldTrackRegression() || other.shouldTrackRegression() );
        } else {
            throw new RuntimeException( "Can only aggregate worker metrics with the same name and unit. " +
                    "(" + this + ", " + other + ")." );
        }
    }

    @Override
    public String toString()
    {
        return String.format( "WorkerMetric[name='%s', value=%s, unit=%s aggregation=%s]", getName(), getValue(),
                getUnit(), getAggregation() );
    }

    /**
     * Set how a worker result should be aggregated.
     */
    public static interface Aggregation
    {
        public double aggregate(double one, double other);
    }

    public static Aggregation TOTAL = new Aggregation()
    {
        @Override
        public String toString()
        {
            return String.format( "Aggregation[TOTAL]" );
        }

        @Override
        public double aggregate( double one, double other )
        {
            return one + other;
        }
    };

    public static Aggregation AVERAGE = new Aggregation()
    {
        @Override
        public String toString()
        {
            return String.format( "Aggregation[AVERAGE]" );
        }

        @Override
        public double aggregate( double one, double other )
        {
            return (one + other) / 2;
        }
    };
}
