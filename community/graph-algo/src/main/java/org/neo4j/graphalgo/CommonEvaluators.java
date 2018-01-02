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
package org.neo4j.graphalgo;

import org.neo4j.graphalgo.impl.util.DoubleEvaluator;
import org.neo4j.graphalgo.impl.util.DoubleEvaluatorWithDefault;
import org.neo4j.graphalgo.impl.util.GeoEstimateEvaluator;
import org.neo4j.graphalgo.impl.util.IntegerEvaluator;

/**
 * Factory for common evaluators used by some graph algos, f.ex
 * {@link CostEvaluator} and {@link EstimateEvaluator}.
 * 
 * @author Mattias Persson
 */
public abstract class CommonEvaluators
{
    public static CostEvaluator<Double> doubleCostEvaluator( String relationshipCostPropertyKey )
    {
        return new DoubleEvaluator( relationshipCostPropertyKey );
    }

    public static CostEvaluator<Double> doubleCostEvaluator( String relationshipCostPropertyKey, double defaultCost )
    {
        return new DoubleEvaluatorWithDefault( relationshipCostPropertyKey, defaultCost );
    }
    
    public static CostEvaluator<Integer> intCostEvaluator( String relationshipCostPropertyKey )
    {
        return new IntegerEvaluator( relationshipCostPropertyKey );
    }
    
    public static EstimateEvaluator<Double> geoEstimateEvaluator(
            String latitudePropertyKey, String longitudePropertyKey )
    {
        return new GeoEstimateEvaluator( latitudePropertyKey, longitudePropertyKey );
    }
}
