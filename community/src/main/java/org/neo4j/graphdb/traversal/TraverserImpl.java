package org.neo4j.graphdb.traversal;

import java.util.Iterator;

import org.neo4j.commons.iterator.IterableWrapper;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;

class TraverserImpl implements Traverser
{
    private final TraversalDescription description;
    private final Node startNode;

    TraverserImpl( TraversalDescription description, Node startNode )
    {
        this.description = description;
        this.startNode = startNode;
    }

    public Iterator<Position> iterator()
    {
        switch ( description.order )
        {
        case DEPTH_FIRST:
            return new DepthFirstTraversal( description, startNode );
        case BREADTH_FIRST:
            return new BreadthFirstTraversal( description, startNode );
        default:
            throw new IllegalStateException( "Unknown order "
                                             + description.order );
        }
    }

    public Iterable<Node> nodes()
    {
        return new IterableWrapper<Node, Position>( this )
        {
            @Override
            protected Node underlyingObjectToObject( Position position )
            {
                return position.node();
            }
        };
    }

    public Iterable<Relationship> relationships()
    {
        return new IterableWrapper<Relationship, Position>( this )
        {
            @Override
            public Iterator<Relationship> iterator()
            {
                Iterator<Relationship> iter = super.iterator();
                iter.next(); // Skip the first, it is null
                return iter;
            }

            @Override
            protected Relationship underlyingObjectToObject( Position position )
            {
                return position.lastRelationship();
            }
        };
    }

    public Iterable<Path> paths()
    {
        return new IterableWrapper<Path, Position>( this )
        {
            @Override
            protected Path underlyingObjectToObject( Position position )
            {
                return position.path();
            }
        };
    }
}
