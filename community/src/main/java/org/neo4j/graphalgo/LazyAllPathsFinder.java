package org.neo4j.graphalgo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.commons.iterator.IterableWrapper;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ReturnableEvaluator;
import org.neo4j.graphdb.StopEvaluator;
import org.neo4j.graphdb.TraversalPosition;
import org.neo4j.graphdb.Traverser;
import org.neo4j.graphdb.Traverser.Order;

/**
 * Starting from a {@link Node} this will traverse out BREADTH FIRST and find
 * all nodes up to a certain depth and return full paths to them
 * (from the start node).
 * 
 * It will instantiate a traverser and return instantly.
 */
public class LazyAllPathsFinder
{
    private final Node startNode;
    private final RelationshipType relationshipType;
    
    public LazyAllPathsFinder( Node node, RelationshipType relationshipType )
    {
        this.startNode = node;
        this.relationshipType = relationshipType;
    }
    
    public Iterable<List<PropertyContainer>> getPaths( final int maxDepth )
    {
        StopEvaluator stopEvaluator = new StopEvaluator()
        {
            public boolean isStopNode( TraversalPosition position )
            {
                return position.depth() > maxDepth;
            }
        };
        
        final Map<Node, List<PropertyContainer>> trails =
            new HashMap<Node, List<PropertyContainer>>();
        final Traverser traverser = startNode.traverse( Order.BREADTH_FIRST,
            stopEvaluator, ReturnableEvaluator.ALL_BUT_START_NODE,
            relationshipType, Direction.BOTH );
        return new IterableWrapper<List<PropertyContainer>, Node>( traverser )
        {
            @Override
            protected List<PropertyContainer> underlyingObjectToObject(
                Node node )
            {
                TraversalPosition position = traverser.currentPosition();
                if ( position.depth() > maxDepth )
                {
                    return null;
                }
                Relationship rel = position.lastRelationshipTraversed();
                List<PropertyContainer> trail = new ArrayList<PropertyContainer>();
                if ( position.depth() > 1 )
                {
                    Node parent = rel.getOtherNode( node );
                    List<PropertyContainer> parentTrail = trails.get( parent );
                    trail.addAll( parentTrail );
                    trail.add( rel );
                }
                else
                {
                    trail.add( startNode );
                    trail.add( rel );
                }
                trail.add( node );
                trails.put( node, trail );
                return trail;
            }
        };
    }
}
