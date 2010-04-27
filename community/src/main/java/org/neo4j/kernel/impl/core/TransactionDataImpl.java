package org.neo4j.kernel.impl.core;

import java.util.ArrayList;
import java.util.Collection;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.event.PropertyEntry;
import org.neo4j.graphdb.event.TransactionData;

class TransactionDataImpl implements TransactionData
{
    private final Collection<PropertyEntry<Node>> assignedNodeProperties = newCollection();
    private final Collection<PropertyEntry<Relationship>> assignedRelationshipProperties =
            newCollection();
    private final Collection<Node> createdNodes = newCollection();
    private final Collection<Relationship> createdRelationships = newCollection();
    private final Collection<Node> deletedNodes = newCollection();
    private final Collection<Relationship> deletedRelationships = newCollection();
    private final Collection<PropertyEntry<Node>> removedNodeProperties = newCollection();
    private final Collection<PropertyEntry<Relationship>> removedRelationshipProperties =
            newCollection();
    
    private static <T> Collection<T> newCollection()
    {
        // TODO Tweak later, better collection impl or something?
        return new ArrayList<T>();
    }

    public Iterable<PropertyEntry<Node>> assignedNodeProperties()
    {
        return this.assignedNodeProperties;
    }

    public Iterable<PropertyEntry<Relationship>> assignedRelationshipProperties()
    {
        return this.assignedRelationshipProperties;
    }

    public Iterable<Node> createdNodes()
    {
        return this.createdNodes;
    }

    public Iterable<Relationship> createdRelationships()
    {
        return this.createdRelationships;
    }

    public Iterable<Node> deletedNodes()
    {
        return this.deletedNodes;
    }

    public Iterable<Relationship> deletedRelationships()
    {
        return this.deletedRelationships;
    }

    public Iterable<PropertyEntry<Node>> removedNodeProperties()
    {
        return this.removedNodeProperties;
    }

    public Iterable<PropertyEntry<Relationship>> removedRelationshipProperties()
    {
        return this.removedRelationshipProperties;
    }

    void assignedProperty( Node node, String key, Object value,
            Object valueBeforeTransaction )
    {
        this.assignedNodeProperties.add( PropertyEntryImpl.assigned( node, key,
                value, valueBeforeTransaction ) );
    }

    void assignedProperty( Relationship relationship, String key,
            Object value, Object valueBeforeTransaction )
    {
        this.assignedRelationshipProperties.add( PropertyEntryImpl.assigned(
                relationship, key, value, valueBeforeTransaction ) );
    }

    void removedProperty( Node node, String key,
            Object valueBeforeTransaction )
    {
        this.removedNodeProperties.add( PropertyEntryImpl.removed( node, key,
                valueBeforeTransaction ) );
    }

    void removedProperty( Relationship relationship, String key,
            Object valueBeforeTransaction )
    {
        this.removedRelationshipProperties.add( PropertyEntryImpl.removed(
                relationship, key, valueBeforeTransaction ) );
    }
    
    void created( Node node )
    {
        this.createdNodes.add( node );
    }
    
    void created( Relationship relationship )
    {
        this.createdRelationships.add( relationship );
    }
    
    void deleted( Node node )
    {
        this.deletedNodes.add( node );
    }
    
    void deleted( Relationship relationship )
    {
        this.deletedRelationships.add( relationship );
    }
}
