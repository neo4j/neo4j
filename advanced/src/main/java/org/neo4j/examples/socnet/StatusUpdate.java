package org.neo4j.examples.socnet;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.Traverser;
import org.neo4j.helpers.collection.IterableWrapper;
import org.neo4j.kernel.Traversal;

import java.util.Date;

import static org.neo4j.examples.socnet.RelTypes.NEXT;
import static org.neo4j.examples.socnet.RelTypes.STATUS;

public class StatusUpdate
{
    private final Node underlyingNode;
    static final String TEXT = "TEXT";
    static final String DATE = "DATE";

    public StatusUpdate( Node underlyingNode )
    {

        this.underlyingNode = underlyingNode;
    }

    public Node getUnderlyingNode()
    {
        return underlyingNode;
    }

    public Person getPerson()
    {
        Node statusMessage = GetLastStatusMessage();
        Relationship relationship = statusMessage.getSingleRelationship( STATUS, Direction.INCOMING );
        Node personNode = relationship.getStartNode();
        return new Person( personNode );
    }

    private Node GetLastStatusMessage()
    {
        TraversalDescription traversalDescription = Traversal.description().
                depthFirst().
                relationships( NEXT, Direction.BOTH ).
                filter( Traversal.returnAll() );

        Traverser traverser = traversalDescription.traverse( getUnderlyingNode() );

        Node lastStatus = null;
        for ( Path i : traverser )
        {
            lastStatus = i.endNode();
        }

        return lastStatus;
    }

    public String getStatusText()
    {
        return (String)underlyingNode.getProperty( TEXT );
    }

    public Date getDate()
    {
        Long l = (Long)underlyingNode.getProperty( DATE );

        return new Date( l );
    }

    public StatusUpdate next()
    {
        IterableWrapper<StatusUpdate, Relationship> statusIterator = new IterableWrapper<StatusUpdate, Relationship>(
                underlyingNode.getRelationships( NEXT ) )
        {
            @Override
            protected StatusUpdate underlyingObjectToObject(
                    Relationship nextRel )
            {
                return new StatusUpdate( nextRel.getOtherNode( underlyingNode ) );
            }
        };

        if ( !statusIterator.iterator().hasNext() )
        {
            return null;
        }

        return statusIterator.iterator().next();
    }
}
