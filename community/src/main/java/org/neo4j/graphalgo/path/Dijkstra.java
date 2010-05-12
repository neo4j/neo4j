package org.neo4j.graphalgo.path;

import java.util.Iterator;

import org.neo4j.commons.iterator.IterableWrapper;
import org.neo4j.graphalgo.CostEvaluator;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphalgo.util.BestFirstSelectorFactory;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.RelationshipExpander;
import org.neo4j.graphdb.traversal.Position;
import org.neo4j.graphdb.traversal.ReturnFilter;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.Traverser;
import org.neo4j.graphdb.traversal.Uniqueness;
import org.neo4j.kernel.TraversalFactory;

/**
 *
 * @author Tobias Ivarsson
 * @author Martin Neumann
 */
public class Dijkstra implements PathFinder<WeightedPath>
{
    private static final TraversalDescription TRAVERSAL = TraversalFactory.createTraversalDescription().uniqueness(
            Uniqueness.RELATIONSHIP_GLOBAL );

    private final RelationshipExpander expander;
    private final CostEvaluator<Double> costEvaluator;

    public Dijkstra( RelationshipExpander expander, CostEvaluator<Double> costEvaluator )
    {
        this.expander = expander;
        this.costEvaluator = costEvaluator;
    }

    public Iterable<WeightedPath> findAllPaths( Node start, Node end )
    {
        Traverser traverser = TRAVERSAL.expand( expander ).sourceSelector(
                new SelectorFactory( costEvaluator ) ).filter(
                new StopCondition( end ) ).traverse( start );
        return new IterableWrapper<WeightedPath, Path>( traverser.paths() )
        {
            @Override
            protected WeightedPath underlyingObjectToObject( Path path )
            {
                return new WeightedPathImpl( costEvaluator, path );
            }
        };
    }

    public WeightedPath findSinglePath( Node start, Node end )
    {
        Iterator<WeightedPath> result = findAllPaths( start, end ).iterator();
        if ( result.hasNext() )
        {
            return result.next();
        }
        else
        {
            return null;
        }
    }

    private static class StopCondition implements ReturnFilter
    {
        private final Node stop;

        StopCondition( Node end )
        {
            this.stop = end;
        }

        public boolean shouldReturn( Position position )
        {
            return position.node().equals( stop );
        }
    }

    private static class SelectorFactory extends BestFirstSelectorFactory
    {
        private final CostEvaluator<Double> evaluator;

        SelectorFactory( CostEvaluator<Double> evaluator )
        {
            this.evaluator = evaluator;
        }
    }
}
