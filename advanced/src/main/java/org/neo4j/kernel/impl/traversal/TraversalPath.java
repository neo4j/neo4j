package org.neo4j.kernel.impl.traversal;

import java.util.Iterator;
import java.util.LinkedList;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.traversal.ExpansionSource;

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
        StringBuilder builder = new StringBuilder();
        Iterator<Node> nodeIterator = this.nodes.iterator();
        Iterator<Relationship> relIterator = this.relationships.iterator();
        Node currentNode = nodeIterator.next();
        builder.append( nodeRepresentation( currentNode ) );
        while ( relIterator.hasNext() )
        {
            builder.append( relationshipRepresentation( relIterator.next(),
                    currentNode ) );
            currentNode = nodeIterator.next();
            builder.append( nodeRepresentation( currentNode ) );
        }
        return builder.toString();
    }

    private String relationshipRepresentation( Relationship relationship,
            Node fromNode )
    {
        boolean outgoing = relationship.getStartNode().equals( fromNode );
        StringBuilder builder = new StringBuilder();
        builder.append( outgoing ? "--" : "<--" );
        builder.append( "<" + relationship.getId() + ","
                        + relationship.getType().name() + ">" );
        builder.append( outgoing ? "-->" : "--" );
        return builder.toString();
    }

    private String nodeRepresentation( Node node )
    {
        return "(" + node.getId() + ")";
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
