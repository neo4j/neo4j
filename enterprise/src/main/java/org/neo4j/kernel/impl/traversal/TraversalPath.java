package org.neo4j.kernel.impl.traversal;

import java.util.Iterator;
import java.util.LinkedList;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.traversal.ExpansionSource;
import org.neo4j.kernel.TraversalFactory;

public class TraversalPath implements Path
{
    private final LinkedList<Node> nodes = new LinkedList<Node>();
    private final LinkedList<Relationship> relationships = new LinkedList<Relationship>();

    TraversalPath( ExpansionSource source )
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

    public Node getStartNode()
    {
        return nodes.getFirst();
    }

    public Node getEndNode()
    {
        return nodes.getLast();
    }

    public Iterable<Node> nodes()
    {
        return nodes;
    }

    public Iterable<Relationship> relationships()
    {
        return relationships;
    }

    public int length()
    {
        return relationships.size();
    }

    @Override
    public String toString()
    {
        return TraversalFactory.defaultPathToString( this );
    }

    @Override
    public int hashCode()
    {
        if ( relationships.isEmpty() )
        {
            return getStartNode().hashCode();
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
            return getStartNode().equals( other.getStartNode() )
                   && relationships.equals( other.relationships );
        }
        else if ( obj instanceof Path )
        {
            Path other = (Path) obj;
            if ( getStartNode().equals( other.getStartNode() ) )
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
