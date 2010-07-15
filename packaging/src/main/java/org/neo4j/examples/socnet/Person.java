package org.neo4j.examples.socnet;

import org.neo4j.graphalgo.GraphAlgoFactory;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.Uniqueness;
import org.neo4j.helpers.collection.IterableWrapper;
import org.neo4j.kernel.Traversal;

public class Person
{
    static final String NAME = "person_name";
    static final RelationshipType FRIEND = 
        DynamicRelationshipType.withName( "FRIEND" );

    private final Node underlyingNode;

    Person( Node personNode )
    {
        this.underlyingNode = personNode;
    }

    protected Node getUnderlyingNode()
    {
        return underlyingNode;
    }

    public String getName()
    {
        return (String) underlyingNode.getProperty( NAME );
    }

    public int hashCode()
    {
        return underlyingNode.hashCode();
    }

    public boolean equals( Object o )
    {
        if ( o instanceof Person )
        {
            return underlyingNode.equals( ( (Person) o ).getUnderlyingNode() );
        }
        return false;
    }

    public String toString()
    {
        return "Person[" + getName() + "]";
    }

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
        TraversalDescription travDesc = Traversal.description().depthFirst().relationships(
                FRIEND ).uniqueness( Uniqueness.NODE_GLOBAL ).prune(
                Traversal.pruneAfterDepth( 2 ) ).filter(
                Traversal.returnAllButStartNode() );
        /* 
           // old traverser api would be something like:
           Traverser trav = underlyingNode.traverse( Order.DEPTH_FIRST, new
               StopEvaluator()
               {
                   public boolean isStopNode( TraversalPosition currentPos )
                   {
                       return currentPos.depth() == 2;
                   }
               }, ReturnableEvaluator.ALL_BUT_START_NODE, FRIEND, Direction.BOTH );
        */
        
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
        PathFinder<Path> finder = GraphAlgoFactory.shortestPath(
                Traversal.expanderForTypes( FRIEND, Direction.BOTH ), maxDepth );
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
}