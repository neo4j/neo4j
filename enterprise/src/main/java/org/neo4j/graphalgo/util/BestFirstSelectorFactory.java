package org.neo4j.graphalgo.util;

import org.neo4j.graphalgo.util.PriorityMap.Converter;
import org.neo4j.graphalgo.util.PriorityMap.Entry;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.traversal.ExpansionSource;
import org.neo4j.graphdb.traversal.SourceSelector;
import org.neo4j.graphdb.traversal.SourceSelectorFactory;

public abstract class BestFirstSelectorFactory implements SourceSelectorFactory
{
    public SourceSelector create( ExpansionSource startSource )
    {
        return new BestFirstSelector( startSource );
    }

    public final class BestFirstSelector implements SourceSelector
    {
        private PriorityMap<ExpansionSource, Node, Double> queue =
                PriorityMap.withNaturalOrder( CONVERTER );
        private ExpansionSource current;
        private double currentAggregatedValue;

        BestFirstSelector( ExpansionSource source )
        {
            this.current = source;
        }

        public ExpansionSource nextPosition()
        {
            // Exhaust current if not already exhausted
            while ( true )
            {
                ExpansionSource next = current.next();
                if ( next != null )
                {
                    queue.put( next, currentAggregatedValue + calculateValue( next ) );
                }
                else
                {
                    break;
                }
            }
            
            // Pop the top from priorityMap
            Entry<ExpansionSource, Double> entry = queue.pop();
            if ( entry != null )
            {
                current = entry.getEntity();
                currentAggregatedValue = entry.getPriority();
                return current;
            }
            return null;
        }
    }

    protected abstract double calculateValue( ExpansionSource next );
    
    private static final Converter<Node, ExpansionSource> CONVERTER =
            new Converter<Node, ExpansionSource>()
    {
        public Node convert( ExpansionSource source )
        {
            return source.node();
        }
    };
}
