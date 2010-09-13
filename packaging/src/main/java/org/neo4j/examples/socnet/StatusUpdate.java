package org.neo4j.examples.socnet;

import static org.neo4j.examples.socnet.RelTypes.NEXT;
import static org.neo4j.examples.socnet.RelTypes.PERSON;

import java.util.Date;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.helpers.collection.IterableWrapper;

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
        return new Person( underlyingNode.getSingleRelationship( PERSON,
                Direction.OUTGOING ).getEndNode() );
    }

    public String getStatusText()
    {
        return (String) underlyingNode.getProperty( TEXT );
    }

    public Date getDate()
    {
        Long l = (Long) underlyingNode.getProperty( DATE );

        return new Date( l.longValue() );
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
