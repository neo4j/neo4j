package org.neo4j.graphalgo.shortestpath;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;

class FakeRelImpl implements Relationship
{
    public void delete()
    {
        throw new UnsupportedOperationException();
    }

    public Node getEndNode()
    {
        throw new UnsupportedOperationException();
    }

    public long getId()
    {
        throw new UnsupportedOperationException();
    }

    public Node[] getNodes()
    {
        throw new UnsupportedOperationException();
    }

    public Node getOtherNode( Node node )
    {
        throw new UnsupportedOperationException();
    }

    public Node getStartNode()
    {
        throw new UnsupportedOperationException();
    }

    public RelationshipType getType()
    {
        throw new UnsupportedOperationException();
    }

    public boolean isType( RelationshipType type )
    {
        throw new UnsupportedOperationException();
    }

    public Object getProperty( String key )
    {
        throw new UnsupportedOperationException();
    }

    public Object getProperty( String key, Object defaultValue )
    {
        throw new UnsupportedOperationException();
    }

    public Iterable<String> getPropertyKeys()
    {
        throw new UnsupportedOperationException();
    }

    public Iterable<Object> getPropertyValues()
    {
        throw new UnsupportedOperationException();
    }

    public boolean hasProperty( String key )
    {
        throw new UnsupportedOperationException();
    }

    public Object removeProperty( String key )
    {
        throw new UnsupportedOperationException();
    }

    public void setProperty( String key, Object value )
    {
        throw new UnsupportedOperationException();
    }
}
