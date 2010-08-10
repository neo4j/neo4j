package org.neo4j.kernel.ha;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;

public class LockableRelationship implements Relationship
{
    private final int id;

    LockableRelationship( int id )
    {
        this.id = id;
    }

    public void delete()
    {
        throw new UnsupportedOperationException( "Lockable rel" );
    }

    public Node getEndNode()
    {
        throw new UnsupportedOperationException( "Lockable rel" );
    }

    public long getId()
    {
        return this.id;
    }

    public GraphDatabaseService getGraphDatabase()
    {
        throw new UnsupportedOperationException( "Lockable rel" );
    }

    public Node[] getNodes()
    {
        throw new UnsupportedOperationException( "Lockable rel" );
    }

    public Node getOtherNode( Node node )
    {
        throw new UnsupportedOperationException( "Lockable rel" );
    }

    public Object getProperty( String key )
    {
        throw new UnsupportedOperationException( "Lockable rel" );
    }

    public Object getProperty( String key, Object defaultValue )
    {
        throw new UnsupportedOperationException( "Lockable rel" );
    }

    public Iterable<String> getPropertyKeys()
    {
        throw new UnsupportedOperationException( "Lockable rel" );
    }

    public Iterable<Object> getPropertyValues()
    {
        throw new UnsupportedOperationException( "Lockable rel" );
    }

    public Node getStartNode()
    {
        throw new UnsupportedOperationException( "Lockable rel" );
    }

    public RelationshipType getType()
    {
        throw new UnsupportedOperationException( "Lockable rel" );
    }

    public boolean isType( RelationshipType type )
    {
        throw new UnsupportedOperationException( "Lockable rel" );
    }

    public boolean hasProperty( String key )
    {
        throw new UnsupportedOperationException( "Lockable rel" );
    }

    public Object removeProperty( String key )
    {
        throw new UnsupportedOperationException( "Lockable rel" );
    }

    public void setProperty( String key, Object value )
    {
        throw new UnsupportedOperationException( "Lockable rel" );
    }

    public boolean equals( Object o )
    {
        if ( !(o instanceof Relationship) )
        {
            return false;
        }
        return this.getId() == ((Relationship) o).getId();
    }

    public int hashCode()
    {
        return id;
    }

    public String toString()
    {
        return "Lockable relationship #" + this.getId();
    }
}
