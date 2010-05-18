package org.neo4j.graphalgo.path;

import java.util.Iterator;

import org.neo4j.graphalgo.CostEvaluator;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphalgo.util.BestFirstSelectorFactory;
import org.neo4j.graphalgo.util.StopAfterWeightIterator;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipExpander;
import org.neo4j.graphdb.traversal.ExpansionSource;
import org.neo4j.graphdb.traversal.Position;
import org.neo4j.graphdb.traversal.ReturnFilter;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.Traverser;
import org.neo4j.graphdb.traversal.Uniqueness;
import org.neo4j.kernel.TraversalFactory;

/**
 * @author Tobias Ivarsson
 * @author Martin Neumann
 * @author Mattias Persson
 */
public class Dijkstra implements PathFinder<WeightedPath>
{
    private static final TraversalDescription TRAVERSAL =
            TraversalFactory.createTraversalDescription().uniqueness(
                    Uniqueness.RELATIONSHIP_GLOBAL );

    private final RelationshipExpander expander;
    private final CostEvaluator<Double> costEvaluator;

    public Dijkstra( RelationshipExpander expander, CostEvaluator<Double> costEvaluator )
    {
        this.expander = expander;
        this.costEvaluator = costEvaluator;
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
        
        final Traverser traverser = TRAVERSAL.expand( expander ).sourceSelector(
                new SelectorFactory( costEvaluator ) ).filter( filter ).traverse( start );
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
        Iterator<WeightedPath> result = findAllPaths( start, end ).iterator();
        return result.hasNext() ? result.next() : null;
    }

    private static class SelectorFactory extends BestFirstSelectorFactory
    {
        private final CostEvaluator<Double> evaluator;

        SelectorFactory( CostEvaluator<Double> evaluator )
        {
            this.evaluator = evaluator;
        }
        
        @Override
        protected double calculateValue( ExpansionSource next )
        {
            return next.depth() == 0 ? 0d : evaluator.getCost( next.relationship(), false );
        }
    }
}
