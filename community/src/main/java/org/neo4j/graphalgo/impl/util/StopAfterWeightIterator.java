package org.neo4j.graphalgo.impl.util;

import java.util.Iterator;

import org.neo4j.commons.iterator.PrefetchingIterator;
import org.neo4j.graphalgo.CostEvaluator;
import org.neo4j.graphalgo.WeightedPath;
import org.neo4j.graphdb.Path;

public class StopAfterWeightIterator extends PrefetchingIterator<WeightedPath>
{
    private final Iterator<Path> paths;
    private final CostEvaluator<Double> costEvaluator;
    private Double foundWeight;
    
    public StopAfterWeightIterator( Iterator<Path> paths,
            CostEvaluator<Double> costEvaluator )
    {
        this.paths = paths;
        this.costEvaluator = costEvaluator;
    }
    
    @Override
    protected WeightedPath fetchNextOrNull()
    {
        if ( !paths.hasNext() )
        {
            return null;
        }
        WeightedPath path = new WeightedPathImpl( costEvaluator, paths.next() );
        System.out.println( "found path " + path );
        if ( foundWeight != null && path.weight() > foundWeight )
        {
            return null;
        }
        foundWeight = path.weight();
        return path;
    }
}
