package org.neo4j.kernel.impl.traversal;

import java.util.Iterator;
import java.util.LinkedList;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.traversal.TraversalBranch;
import org.neo4j.kernel.Traversal;

public class TraversalPath implements Path
{
    private final LinkedList<Node> nodes = new LinkedList<Node>();
    private final LinkedList<Relationship> relationships = new LinkedList<Relationship>();

    TraversalPath( TraversalBranch source )
    {
        while ( source != null )
        {
            nodes.addFirst( source.node() );
            Relationship relationship = source.relationship();
            if (relationship != null)
            {
                relationships.addFirst( relationship );
            }
            source = source.parent();
        }
    }

    public Node startNode()
    {
        return nodes.getFirst();
    }

    public Node endNode()
    {
        return nodes.getLast();
    }

    public Relationship lastRelationship()
    {
        return relationships.isEmpty() ? null : relationships.getLast();
    }

    public Iterable<Node> nodes()
    {
        return nodes;
    }

    public Iterable<Relationship> relationships()
    {
        return relationships;
    }

    public Iterator<PropertyContainer> iterator()
    {
        return new Iterator<PropertyContainer>()
        {
            Iterator<? extends PropertyContainer> current = nodes().iterator();
            Iterator<? extends PropertyContainer> next = relationships().iterator();

            public boolean hasNext()
            {
                return current.hasNext();
            }

            public PropertyContainer next()
            {
                try
                {
                    return current.next();
                }
                finally
                {
                    Iterator<? extends PropertyContainer> temp = current;
                    current = next;
                    next = temp;
                }
            }

            public void remove()
            {
                next.remove();
            }
        };
    }

    public int length()
    {
        return relationships.size();
    }

    @Override
    public String toString()
    {
        return Traversal.defaultPathToString( this );
    }

    @Override
    public int hashCode()
    {
        if ( relationships.isEmpty() )
        {
            return startNode().hashCode();
        }
        else
        {
            return relationships.hashCode();
        }
    }

    @Override
    public boolean equals( Object obj )
    {
        if ( this == obj )
        {
            return true;
        }
        else if ( obj instanceof TraversalPath )
        {
            TraversalPath other = (TraversalPath) obj;
            return startNode().equals( other.startNode() )
                   && relationships.equals( other.relationships );
        }
        else if ( obj instanceof Path )
        {
            Path other = (Path) obj;
            if ( startNode().equals( other.startNode() ) )
            {
                Iterator<Relationship> these = relationships().iterator();
                Iterator<Relationship> those = other.relationships().iterator();
                while ( these.hasNext() && those.hasNext() )
                {
                    if ( !these.next().equals( those.next() ) )
                    {
                        return false;
                    }
                }
                if ( these.hasNext() || those.hasNext() )
                {
                    return false;
                }
                return true;
            }
        }
        return false;
    }
}
