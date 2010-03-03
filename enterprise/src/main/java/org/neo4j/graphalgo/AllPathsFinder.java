package org.neo4j.graphalgo;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;

/**
 * Starting from a {@link Node} this will traverse out BREADTH FIRST and find
 * all nodes up to a certain depth and return full paths to them (from the start
 * node).
 * 
 * It will collect all the paths in a list first and then return the whole list.
 */
public class AllPathsFinder
{
    private final Node startNode;
    private final RelationshipType relationshipType;
    private final Direction direction;

    public AllPathsFinder( Node node, RelationshipType relationshipType,
            Direction direction )
    {
        this.startNode = node;
        this.relationshipType = relationshipType;
        this.direction = direction;
    }

    public List<List<PropertyContainer>> getPaths( final int maxDepth )
    {
        List<List<PropertyContainer>> result = new ArrayList<List<PropertyContainer>>();
        if ( maxDepth == 0 )
        {
            return result;
        }
        Set<Long> traversedRels = new HashSet<Long>();
        LinkedList<PropertyContainer> trail = new LinkedList<PropertyContainer>();
        trail.add( startNode );
        Set<Relationship> rels = new HashSet<Relationship>();
        for ( Relationship rel : startNode.getRelationships( relationshipType, direction ) )
        {
            rels.add( rel );
            traversedRels.add( rel.getId() );
            result.add( constructPath( trail, rel, rel.getOtherNode( startNode ) ) );
        }
        for ( Relationship rel : rels )
        {
            traverse( traversedRels, result, trail, startNode, rel, 2, maxDepth );
        }
        return result;
    }

    private List<PropertyContainer> constructPath(
            LinkedList<PropertyContainer> trail, Relationship rel, Node node )
    {
        List<PropertyContainer> path = new ArrayList<PropertyContainer>( trail );
        path.add( rel );
        path.add( node );
        return path;
    }

    private void traverse( Set<Long> traversedRels,
            List<List<PropertyContainer>> result,
            LinkedList<PropertyContainer> trail, Node fromNode,
            Relationship fromRel, int depth, int maxDepth )
    {
        if ( depth > maxDepth )
        {
            return;
        }
        Node toNode = fromRel.getOtherNode( fromNode );
        trail.add( fromRel );
        trail.add( toNode );

        Set<Relationship> childRels = new HashSet<Relationship>();
        for ( Relationship childRel : toNode.getRelationships( relationshipType, direction ) )
        {
            if ( !traversedRels.add( childRel.getId() ) )
            {
                continue;
            }
            childRels.add( childRel );
            result.add( constructPath( trail, childRel,
                    childRel.getOtherNode( toNode ) ) );
        }
        for ( Relationship childRel : childRels )
        {
            traverse( traversedRels, result, trail, toNode, childRel,
                    depth + 1, maxDepth );
        }
        trail.removeLast();
        trail.removeLast();
    }
}
