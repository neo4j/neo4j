package org.neo4j.graphalgo.path;

import java.util.Iterator;

import org.neo4j.graphalgo.CostEvaluator;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.RelationshipExpander;
import org.neo4j.graphdb.traversal.ExpansionSource;
import org.neo4j.graphdb.traversal.Position;
import org.neo4j.graphdb.traversal.ReturnFilter;
import org.neo4j.graphdb.traversal.SourceSelector;
import org.neo4j.graphdb.traversal.SourceSelectorFactory;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.TraversalRules;
import org.neo4j.graphdb.traversal.Uniqueness;
import org.neo4j.kernel.TraversalFactory;

/**
 *
 * @author Tobias Ivarsson
 * @author Martin Neumann
 */
public class Dijkstra implements PathFinder
{
    private static final TraversalDescription TRAVERSAL = TraversalFactory.createTraversalDescription().uniqueness(
            Uniqueness.RELATIONSHIP_GLOBAL );

    private final TraversalDescription traversal;

    public Dijkstra( RelationshipExpander expander, CostEvaluator<Double> costEvaluator )
    {
        traversal = TRAVERSAL.expand( expander ).sourceSelector(
                new Factory( costEvaluator ) );
    }

    public Iterable<Path> findAllPaths( Node start, Node end )
    {
        return traversal.filter( new StopCondition( end ) ).traverse( start ).paths();
    }

    public Path findSinglePath( Node start, Node end )
    {
        Iterator<Path> result = findAllPaths( start, end ).iterator();
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

    private static class Factory implements SourceSelectorFactory
    {
        final CostEvaluator<Double> evaluator;

        Factory( CostEvaluator<Double> evaluator )
        {
            this.evaluator = evaluator;
        }

        public SourceSelector create( ExpansionSource startSource )
        {
            return new Selector( evaluator, startSource );
        }
    }

    private static class Selector implements SourceSelector
    {
        private final CostEvaluator<Double> evaluator;
        private final ExpansionSource source;

        Selector( CostEvaluator<Double> evaluator, ExpansionSource startSource )
        {
            this.evaluator = evaluator;
            this.source = startSource;
        }

        public ExpansionSource nextPosition( TraversalRules rules )
        {
            // TODO Auto-generated method stub
            return null;
        }
    }
}
