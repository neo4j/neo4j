package org.neo4j.graphalgo;

import org.neo4j.graphalgo.impl.util.DoubleEvaluator;
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
