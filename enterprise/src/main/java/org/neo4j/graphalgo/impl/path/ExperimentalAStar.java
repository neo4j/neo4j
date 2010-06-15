package org.neo4j.graphalgo.impl.path;

import java.util.Iterator;

import org.neo4j.graphalgo.CostEvaluator;
import org.neo4j.graphalgo.EstimateEvaluator;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphalgo.WeightedPath;
import org.neo4j.graphalgo.impl.util.BestFirstSelectorFactory;
import org.neo4j.graphalgo.impl.util.StopAfterWeightIterator;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipExpander;
import org.neo4j.graphdb.traversal.ExpansionSource;
import org.neo4j.graphdb.traversal.Position;
import org.neo4j.graphdb.traversal.ReturnFilter;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.Traverser;
import org.neo4j.graphdb.traversal.Uniqueness;
import org.neo4j.kernel.TraversalFactory;

public class ExperimentalAStar implements PathFinder<WeightedPath>
{
    private final TraversalDescription traversalDescription;
    private final CostEvaluator<Double> costEvaluator;

    private final EstimateEvaluator<Double> estimateEvaluator;

    public ExperimentalAStar( RelationshipExpander expander, CostEvaluator<Double> costEvaluator,
            EstimateEvaluator<Double> estimateEvaluator )
    {
        this.traversalDescription = TraversalFactory.createTraversalDescription().uniqueness(
                Uniqueness.NONE ).expand( expander );
        this.costEvaluator = costEvaluator;
        this.estimateEvaluator = estimateEvaluator;
    }
    
    public Iterable<WeightedPath> findAllPaths( Node start, final Node end )
    {
        ReturnFilter filter = new ReturnFilter()
        {
            public boolean shouldReturn( Position position )
            {
                return position.node().equals( end );
            }
        };
        
        final Traverser traverser = traversalDescription.sourceSelector(
                new SelectorFactory( end ) ).filter( filter ).traverse( start );
        return new Iterable<WeightedPath>()
        {
            public Iterator<WeightedPath> iterator()
            {
                return new StopAfterWeightIterator( traverser.paths().iterator(),
                        costEvaluator );
            }
        };
    }

    public WeightedPath findSinglePath( Node start, Node end )
    {
        Iterator<WeightedPath> paths = findAllPaths( start, end ).iterator();
        return paths.hasNext() ? paths.next() : null;
    }
    
    private static class PositionData implements Comparable<PositionData>
    {
        private final double wayLengthG;
        private final double estimateH;
        
        public PositionData( double wayLengthG, double estimateH )
        {
            this.wayLengthG = wayLengthG;
            this.estimateH = estimateH;
        }
        
        Double f()
        {
            return this.estimateH + this.wayLengthG;
        }

        public int compareTo( PositionData o )
        {
            return f().compareTo( o.f() );
        }
        
        @Override
        public String toString()
        {
            return "g:" + wayLengthG + ", h:" + estimateH;
        }
    }
    
    private class SelectorFactory extends BestFirstSelectorFactory<PositionData, Double>
    {
        private final Node end;

        SelectorFactory( Node end )
        {
            this.end = end;
        }
        
        @Override
        protected PositionData addPriority( ExpansionSource source,
                PositionData currentAggregatedValue, Double value )
        {
            return new PositionData( currentAggregatedValue.wayLengthG + value,
                    estimateEvaluator.getCost( source.node(), end ) );
        }

        @Override
        protected Double calculateValue( ExpansionSource next )
        {
            return next.depth() == 0 ? 0d :
                costEvaluator.getCost( next.relationship(), false );
        }

        @Override
        protected PositionData getStartData()
        {
            return new PositionData( 0, 0 );
        }
    }
}
