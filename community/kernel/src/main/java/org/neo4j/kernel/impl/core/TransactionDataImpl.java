/**
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.core;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;

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
    private final Collection<Node> deletedNodes = new HashSet<Node>();
    private final Collection<Relationship> deletedRelationships = new HashSet<Relationship>();
    private final Collection<PropertyEntry<Node>> removedNodeProperties = newCollection();
    private final Collection<PropertyEntry<Relationship>> removedRelationshipProperties =
            newCollection();
    
    private <T> Collection<T> newCollection()
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
    
    public boolean isDeleted( Node node )
    {
        return this.deletedNodes.contains( node );
    }

    public Iterable<Relationship> deletedRelationships()
    {
        return this.deletedRelationships;
    }
    
    public boolean isDeleted( Relationship relationship )
    {
        return this.deletedRelationships.contains( relationship );
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
