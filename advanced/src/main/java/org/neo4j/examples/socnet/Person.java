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

public class Person
{
    static final String NAME = "person_name";
    static final RelationshipType FRIEND = 
        DynamicRelationshipType.withName( "FRIEND" );

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
        return (String) underlyingNode.getProperty( NAME );
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
            return underlyingNode.equals( ( (Person) o ).getUnderlyingNode() );
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
                underlyingNode.createRelationshipTo(
                        otherPerson.getUnderlyingNode(), FRIEND );
            }
            tx.success();
        }
        finally
        {
            tx.finish();
        }
    }

    public int getNrOfFriends(){
        return IteratorUtil.count(getFriends());
    }

    public Iterable<Person> getFriends()
    {
        return new IterableWrapper<Person, Relationship>(
                underlyingNode.getRelationships( FRIEND ) )
        {
            @Override
            protected Person underlyingObjectToObject( Relationship friendRel )
            {
                return new Person( friendRel.getOtherNode( underlyingNode ) );
            }
        };
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
        // return all my friends and their friends using new traversal API
        TraversalDescription travDesc = 
                Traversal.description().
                        depthFirst().
                        relationships(FRIEND ).
                        uniqueness( Uniqueness.NODE_GLOBAL ).
                        prune(Traversal.pruneAfterDepth( 2 ) )
                        .filter(Traversal.returnAllButStartNode() );

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

    public Iterable<Person> getPersonsFromMeTo( Person otherPerson, int maxDepth )
    {
        // use graph algo to calculate a shortest path
        PathFinder<Path> finder = GraphAlgoFactory.
                shortestPath(Traversal.expanderForTypes( FRIEND, Direction.BOTH ), maxDepth );

        Path path = finder.findSinglePath( underlyingNode, 
                otherPerson.getUnderlyingNode() );
        return new IterableWrapper<Person,Node>(
                path.nodes() )
        {
            @Override
            protected Person underlyingObjectToObject( Node node )
            {
                return new Person( node );
            }
        };
    }

    public Iterable<StatusUpdate> getStatus() {
        Relationship firstStatus = underlyingNode.getSingleRelationship(PersonRepository.STATUS, Direction.OUTGOING);
        if(firstStatus == null)
            return Collections.emptyList();


        TraversalDescription traversal = Traversal.
                description().
                depthFirst().
                relationships(PersonRepository.NEXT).
                filter(Traversal.returnAll());

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
}
