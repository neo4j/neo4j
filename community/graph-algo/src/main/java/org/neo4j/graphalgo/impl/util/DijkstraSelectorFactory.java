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
package org.neo4j.graphalgo.impl.util;

import org.neo4j.graphalgo.CostEvaluator;
import org.neo4j.graphdb.traversal.TraversalBranch;

import static org.neo4j.graphdb.Direction.OUTGOING;

public class DijkstraSelectorFactory extends BestFirstSelectorFactory<Double, Double>
{
    private final CostEvaluator<Double> evaluator;

    public DijkstraSelectorFactory( PathInterest<Double> interest, CostEvaluator<Double> evaluator )
    {
        super( interest );
        this.evaluator = evaluator;
    }

    @Override
    protected Double calculateValue( TraversalBranch next )
    {
        return next.length() == 0 ? 0d : evaluator.getCost(
                next.lastRelationship(), OUTGOING );
    }

    @Override
    protected Double addPriority( TraversalBranch source,
            Double currentAggregatedValue, Double value )
    {
        return withDefault( currentAggregatedValue, 0d ) + withDefault( value, 0d );
    }

    private <T> T withDefault( T valueOrNull, T valueIfNull )
    {
        return valueOrNull != null ? valueOrNull : valueIfNull;
    }

    @Override
    protected Double getStartData()
    {
        return 0d;
    }
}
