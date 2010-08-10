package org.neo4j.kernel.ha;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ReturnableEvaluator;
import org.neo4j.graphdb.StopEvaluator;
import org.neo4j.graphdb.Traverser;
import org.neo4j.graphdb.Traverser.Order;

public class LockableNode implements Node
{
    private final int id;

    public LockableNode( int id )
    {
        this.id = id;
    }

    public void delete()
    {
        throw new UnsupportedOperationException( "Lockable node" );
    }

    public long getId()
    {
        return this.id;
    }

    public GraphDatabaseService getGraphDatabase()
    {
        throw new UnsupportedOperationException( "Lockable node" );
    }

    public Object getProperty( String key )
    {
        throw new UnsupportedOperationException( "Lockable node" );
    }

    public Object getProperty( String key, Object defaultValue )
    {
        throw new UnsupportedOperationException( "Lockable node" );
    }

    public Iterable<String> getPropertyKeys()
    {
        throw new UnsupportedOperationException( "Lockable node" );
    }

    public Iterable<Object> getPropertyValues()
    {
        throw new UnsupportedOperationException( "Lockable node" );
    }

    public boolean hasProperty( String key )
    {
        throw new UnsupportedOperationException( "Lockable node" );
    }

    public Object removeProperty( String key )
    {
        throw new UnsupportedOperationException( "Lockable node" );
    }

    public void setProperty( String key, Object value )
    {
        throw new UnsupportedOperationException( "Lockable node" );
    }

    public boolean equals( Object o )
    {
        if ( !(o instanceof Node) )
        {
            return false;
        }
        return this.getId() == ((Node) o).getId();
    }

    public int hashCode()
    {
        return id;
    }

    public String toString()
    {
        return "Lockable node #" + this.getId();
    }

    public Relationship createRelationshipTo( Node otherNode,
            RelationshipType type )
    {
        throw new UnsupportedOperationException( "Lockable node" );
    }

    public Iterable<Relationship> getRelationships()
    {
        throw new UnsupportedOperationException( "Lockable node" );
    }

    public Iterable<Relationship> getRelationships( RelationshipType... types )
    {
        throw new UnsupportedOperationException( "Lockable node" );
    }

    public Iterable<Relationship> getRelationships( Direction dir )
    {
        throw new UnsupportedOperationException( "Lockable node" );
    }

    public Iterable<Relationship> getRelationships( RelationshipType type,
            Direction dir )
    {
        throw new UnsupportedOperationException( "Lockable node" );
    }

    public Relationship getSingleRelationship( RelationshipType type,
            Direction dir )
    {
        throw new UnsupportedOperationException( "Lockable node" );
    }

    public boolean hasRelationship()
    {
        throw new UnsupportedOperationException( "Lockable node" );
    }

    public boolean hasRelationship( RelationshipType... types )
    {
        throw new UnsupportedOperationException( "Lockable node" );
    }

    public boolean hasRelationship( Direction dir )
    {
        throw new UnsupportedOperationException( "Lockable node" );
    }

    public boolean hasRelationship( RelationshipType type, Direction dir )
    {
        throw new UnsupportedOperationException( "Lockable node" );
    }

    public Traverser traverse( Order traversalOrder,
            StopEvaluator stopEvaluator,
            ReturnableEvaluator returnableEvaluator,
            RelationshipType relationshipType, Direction direction )
    {
        throw new UnsupportedOperationException( "Lockable node" );
    }

    public Traverser traverse( Order traversalOrder,
            StopEvaluator stopEvaluator,
            ReturnableEvaluator returnableEvaluator,
            RelationshipType firstRelationshipType, Direction firstDirection,
            RelationshipType secondRelationshipType, Direction secondDirection )
    {
        throw new UnsupportedOperationException( "Lockable node" );
    }

    public Traverser traverse( Order traversalOrder,
            StopEvaluator stopEvaluator,
            ReturnableEvaluator returnableEvaluator,
            Object... relationshipTypesAndDirections )
    {
        throw new UnsupportedOperationException( "Lockable node" );
    }
}
