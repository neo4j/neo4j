package org.neo4j.kernel.impl.traversal;

import java.util.Iterator;

import org.neo4j.commons.iterator.IterableWrapper;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.traversal.Position;
import org.neo4j.graphdb.traversal.Traverser;

class TraverserImpl implements Traverser
{
    private final TraversalDescriptionImpl description;
    private final Node startNode;

    TraverserImpl( TraversalDescriptionImpl description, Node startNode )
    {
        this.description = description;
        this.startNode = startNode;
    }

    public Iterator<Position> iterator()
    {
        return new TraversalRulesImpl( description, startNode );
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
