/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.impl.api;

import java.util.List;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.kernel.PropertyTracker;
import org.neo4j.kernel.api.exceptions.PropertyNotFoundException;
import org.neo4j.kernel.api.operations.AuxiliaryStoreOperations;
import org.neo4j.kernel.api.properties.Property;
import org.neo4j.kernel.impl.core.EntityFactory;
import org.neo4j.kernel.impl.core.PropertyKeyTokenHolder;
import org.neo4j.kernel.impl.core.TokenNotFoundException;

public class LegacyAutoIndexAuxStoreOps implements AuxiliaryStoreOperations
{
    private final AuxiliaryStoreOperations delegate;
    private final List<PropertyTracker<Node>> nodeTrackers;
    private final List<PropertyTracker<Relationship>> relationshipTrackers;
    private final PropertyKeyTokenHolder propertyKeyTokenHolder;
    private final EntityFactory entityFactory;

    public LegacyAutoIndexAuxStoreOps( AuxiliaryStoreOperations delegate,
            PropertyKeyTokenHolder propertyKeyTokenHolder,
            List<PropertyTracker<Node>> nodeTrackers,
            List<PropertyTracker<Relationship>> relationshipTrackers,
            EntityFactory entityFactory )
    {
        this.delegate = delegate;
        this.propertyKeyTokenHolder = propertyKeyTokenHolder;
        this.nodeTrackers = nodeTrackers;
        this.relationshipTrackers = relationshipTrackers;
        this.entityFactory = entityFactory;
    }

    @Override
    public void nodeAddStoreProperty( long nodeId, Property property ) throws PropertyNotFoundException
    {
        if ( !nodeTrackers.isEmpty() )
        {
            Node node = entityFactory.newNodeProxyById( nodeId );
            for ( PropertyTracker<Node> tracker : nodeTrackers )
            {
                tracker.propertyAdded( node, propertyKeyName( property ), property.value() );
            }
        }
        delegate.nodeAddStoreProperty( nodeId, property );
    }

    @Override
    public void nodeChangeStoreProperty( long nodeId, Property previousProperty, Property property )
            throws PropertyNotFoundException
    {
        if ( !nodeTrackers.isEmpty() )
        {
            Node node = entityFactory.newNodeProxyById( nodeId );
            for ( PropertyTracker<Node> tracker : nodeTrackers )
            {
                tracker.propertyChanged( node, propertyKeyName( property ), previousProperty.value( null ),
                        property.value( null ) );
            }
        }
        delegate.nodeChangeStoreProperty( nodeId, previousProperty, property );
    }

    @Override
    public void relationshipAddStoreProperty( long relationshipId, Property property ) throws PropertyNotFoundException
    {
        if ( !relationshipTrackers.isEmpty() )
        {
            Relationship relationship = entityFactory.newRelationshipProxyById( relationshipId );
            for ( PropertyTracker<Relationship> tracker : relationshipTrackers )
            {
                tracker.propertyAdded( relationship, propertyKeyName( property ), property.value() );
            }
        }
        delegate.relationshipAddStoreProperty( relationshipId, property );
    }

    @Override
    public void relationshipChangeStoreProperty( long relationshipId, Property previousProperty, Property property )
            throws PropertyNotFoundException
    {
        if ( !relationshipTrackers.isEmpty() )
        {
            Relationship relationship = entityFactory.newRelationshipProxyById( relationshipId );
            for ( PropertyTracker<Relationship> tracker : relationshipTrackers )
            {
                tracker.propertyChanged( relationship, propertyKeyName( property ),
                        previousProperty.value( null ), property.value( null ) );
            }
        }
        delegate.relationshipChangeStoreProperty( relationshipId, previousProperty, property );
    }

    @Override
    public void nodeRemoveStoreProperty( long nodeId, Property property ) throws PropertyNotFoundException
    {
        if ( !nodeTrackers.isEmpty() )
        {
            Node node = entityFactory.newNodeProxyById( nodeId );
            for ( PropertyTracker<Node> tracker : nodeTrackers )
            {
                tracker.propertyRemoved( node, propertyKeyName( property ), property.value() );
            }
        }
        delegate.nodeRemoveStoreProperty( nodeId, property );
    }

    @Override
    public void relationshipRemoveStoreProperty( long relationshipId, Property property )
            throws PropertyNotFoundException
    {
        if ( !relationshipTrackers.isEmpty() )
        {
            Relationship relationship = entityFactory.newRelationshipProxyById( relationshipId );
            for ( PropertyTracker<Relationship> tracker : relationshipTrackers )
            {
                tracker.propertyRemoved( relationship, propertyKeyName( property ), property.value() );
            }
        }
        delegate.relationshipRemoveStoreProperty( relationshipId, property );
    }

    @Override
    public void graphAddStoreProperty( Property property ) throws PropertyNotFoundException
    {
        delegate.graphAddStoreProperty( property );
    }

    @Override
    public void graphChangeStoreProperty( Property previousProperty, Property property )
            throws PropertyNotFoundException
    {
        delegate.graphChangeStoreProperty( previousProperty, property );
    }

    @Override
    public void graphRemoveStoreProperty( Property property )
    {
        delegate.graphRemoveStoreProperty( property );
    }

    private String propertyKeyName( Property property )
    {
        try
        {
            return propertyKeyTokenHolder.getTokenById( (int) property.propertyKeyId() ).name();
        }
        catch ( TokenNotFoundException e )
        {
            throw new IllegalStateException( "Property key " + property.propertyKeyId() + " should exist" );
        }
    }
    
    @Override
    public void nodeDelete( long nodeId )
    {
        if ( !nodeTrackers.isEmpty() )
        {
            Node node = entityFactory.newNodeProxyById( nodeId );
            Iterable<String> propertyKeys = node.getPropertyKeys();

            for ( String key : propertyKeys )
            {
                Object value = node.getProperty( key );
                for ( PropertyTracker<Node> tracker : nodeTrackers )
                {
                    tracker.propertyRemoved( node, key, value );
                }
            }
        }
    }
    
    @Override
    public void relationshipDelete( long relationshipId )
    {
        if ( !relationshipTrackers.isEmpty() )
        {
            Relationship relationship = entityFactory.newRelationshipProxyById( relationshipId );
            Iterable<String> propertyKeys = relationship.getPropertyKeys();

            for ( String key : propertyKeys )
            {
                Object value = relationship.getProperty( key );
                for ( PropertyTracker<Relationship> tracker : relationshipTrackers )
                {
                    tracker.propertyRemoved( relationship, key, value );
                }
            }
        }

    }
}
