package org.neo4j.examples.socnet;

import org.neo4j.graphalgo.GraphAlgoFactory;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.helpers.collection.IterableWrapper;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.kernel.Traversal;
import org.neo4j.kernel.Uniqueness;

import java.util.Collections;
import java.util.Date;
import java.util.Iterator;

import static org.neo4j.examples.socnet.RelTypes.*;

public class Person
{
    static final String NAME = "person_name";

    // START SNIPPET: the-node
    private final Node underlyingNode;

    Person( Node personNode )
    {
        this.underlyingNode = personNode;
    }

    protected Node getUnderlyingNode()
    {
        return underlyingNode;
    }

    // END SNIPPET: the-node

    // START SNIPPET: delegate-to-the-node
    public String getName()
    {
        return (String)underlyingNode.getProperty( NAME );
    }

    // END SNIPPET: delegate-to-the-node

    // START SNIPPET: override
    @Override
    public int hashCode()
    {
        return underlyingNode.hashCode();
    }

    @Override
    public boolean equals( Object o )
    {
        if ( o instanceof Person )
        {
            return underlyingNode.equals( ( (Person)o ).getUnderlyingNode() );
        }
        return false;
    }

    @Override
    public String toString()
    {
        return "Person[" + getName() + "]";
    }

    // END SNIPPET: override

    public void addFriend( Person otherPerson )
    {
        Transaction tx = underlyingNode.getGraphDatabase().beginTx();
        try
        {
            if ( this.equals( otherPerson ) )
            {
                // ignore
                return;
            }
            Relationship friendRel = getFriendRelationshipTo( otherPerson );
            if ( friendRel == null )
            {
                underlyingNode.createRelationshipTo( otherPerson.getUnderlyingNode(), FRIEND );
            }
            tx.success();
        }
        finally
        {
            tx.finish();
        }
    }

    public int getNrOfFriends()
    {
        return IteratorUtil.count( getFriends() );
    }

    public Iterable<Person> getFriends()
    {
        return getFriendsByDepth( 1 );
    }

    public void removeFriend( Person otherPerson )
    {
        Transaction tx = underlyingNode.getGraphDatabase().beginTx();
        try
        {
            if ( this.equals( otherPerson ) )
            {
                // ignore
                return;
            }
            Relationship friendRel = getFriendRelationshipTo( otherPerson );
            if ( friendRel != null )
            {
                friendRel.delete();
            }
            tx.success();
        }
        finally
        {
            tx.finish();
        }
    }

    private Relationship getFriendRelationshipTo( Person otherPerson )
    {
        Node otherNode = otherPerson.getUnderlyingNode();
        for ( Relationship rel : underlyingNode.getRelationships( FRIEND ) )
        {
            if ( rel.getOtherNode( underlyingNode ).equals( otherNode ) )
            {
                return rel;
            }
        }
        return null;
    }

    public Iterable<Person> getFriendsOfFriends()
    {
        return getFriendsByDepth( 2 );
    }

    private Iterable<Person> getFriendsByDepth( int depth )
    {
        // return all my friends and their friends using new traversal API
        TraversalDescription travDesc = Traversal.description().breadthFirst().relationships(
                FRIEND ).uniqueness( Uniqueness.NODE_GLOBAL ).prune(
                Traversal.pruneAfterDepth( depth ) ).filter(
                Traversal.returnAllButStartNode() );

        return new IterableWrapper<Person, Path>(
                travDesc.traverse( underlyingNode ) )
        {
            @Override
            protected Person underlyingObjectToObject( Path path )
            {
                return new Person( path.endNode() );
            }
        };
    }

    public Iterable<Person> getPersonsFromMeTo( Person otherPerson,
                                                int maxDepth )
    {
        // use graph algo to calculate a shortest path
        PathFinder<Path> finder = GraphAlgoFactory.shortestPath(
                Traversal.expanderForTypes( FRIEND, Direction.BOTH ), maxDepth );

        Path path = finder.findSinglePath( underlyingNode,
                otherPerson.getUnderlyingNode() );
        return new IterableWrapper<Person, Node>( path.nodes() )
        {
            @Override
            protected Person underlyingObjectToObject( Node node )
            {
                return new Person( node );
            }
        };
    }

    public Iterable<StatusUpdate> getStatus()
    {
        Relationship firstStatus = underlyingNode.getSingleRelationship(
                STATUS, Direction.OUTGOING );
        if ( firstStatus == null )
        {
            return Collections.emptyList();
        }

        TraversalDescription traversal = Traversal.description().depthFirst().relationships(
                NEXT ).filter( Traversal.returnAll() );

        return new IterableWrapper<StatusUpdate, Path>(
                traversal.traverse( firstStatus.getEndNode() ) )
        {
            @Override
            protected StatusUpdate underlyingObjectToObject( Path path )
            {
                return new StatusUpdate( path.endNode() );
            }
        };
    }

    public Iterator<StatusUpdate> friendStatuses()
    {
        return new FriendsStatusUpdateIterator( this );
    }


    public void addStatus( String text )
    {
        Transaction tx = graphDb().beginTx();
        try
        {
            StatusUpdate oldStatus;
            if ( getStatus().iterator().hasNext() )
            {
                oldStatus = getStatus().iterator().next();
            } else
            {
                oldStatus = null;
            }

            Node newStatus = createNewStatusNode( text );

            if ( oldStatus != null )
            {
                underlyingNode.getSingleRelationship( RelTypes.STATUS, Direction.OUTGOING ).delete();
                newStatus.createRelationshipTo( oldStatus.getUnderlyingNode(), RelTypes.NEXT );
            }

            underlyingNode.createRelationshipTo( newStatus, RelTypes.STATUS );
            tx.success();
        }
        finally
        {
            tx.finish();
        }
    }

    private GraphDatabaseService graphDb()
    {
        return underlyingNode.getGraphDatabase();
    }

    private Node createNewStatusNode( String text )
    {
        Node newStatus = graphDb().createNode();
        newStatus.setProperty( StatusUpdate.TEXT, text );
        newStatus.setProperty( StatusUpdate.DATE, new Date().getTime() );
        newStatus.createRelationshipTo( underlyingNode, RelTypes.PERSON );
        return newStatus;
    }
}
