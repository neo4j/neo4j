/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
import java.util.Map;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.kernel.PropertyTracker;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.api.properties.Property;
import org.neo4j.kernel.impl.core.EntityFactory;
import org.neo4j.kernel.impl.core.PropertyKeyTokenHolder;
import org.neo4j.kernel.impl.core.TokenNotFoundException;

/**
 * This exists to support the legacy auto indexing feature, and will be removed once that feature has been removed.
 */
public class LegacyPropertyTrackers
{
    private final List<PropertyTracker<Node>> nodeTrackers;
    private final List<PropertyTracker<Relationship>> relationshipTrackers;
    private final PropertyKeyTokenHolder propertyKeyTokenHolder;
    private final EntityFactory entityFactory;

    public LegacyPropertyTrackers( PropertyKeyTokenHolder propertyKeyTokenHolder,
                                   List<PropertyTracker<Node>> nodeTrackers,
                                   List<PropertyTracker<Relationship>> relationshipTrackers,
                                   EntityFactory entityFactory )
    {
        this.propertyKeyTokenHolder = propertyKeyTokenHolder;
        this.nodeTrackers = nodeTrackers;
        this.relationshipTrackers = relationshipTrackers;
        this.entityFactory = entityFactory;
    }

    public void nodeAddStoreProperty( long nodeId, DefinedProperty property )
    {
        if ( !nodeTrackers.isEmpty() )
        {
            Node node = entityFactory.newNodeProxyById( nodeId );
            for ( PropertyTracker<Node> tracker : nodeTrackers )
            {
                tracker.propertyAdded( node, propertyKeyName( property ), property.value() );
            }
        }
    }

    public void nodeChangeStoreProperty( long nodeId, DefinedProperty previousProperty, DefinedProperty property )
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
    }

    public void relationshipAddStoreProperty( long relationshipId, DefinedProperty property )
    {
        if ( !relationshipTrackers.isEmpty() )
        {
            Relationship relationship = entityFactory.newRelationshipProxyById( relationshipId );
            for ( PropertyTracker<Relationship> tracker : relationshipTrackers )
            {
                tracker.propertyAdded( relationship, propertyKeyName( property ), property.value() );
            }
        }
    }

    public void relationshipChangeStoreProperty( long relationshipId, DefinedProperty previousProperty, DefinedProperty property )
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
    }

    public void nodeRemoveStoreProperty( long nodeId, DefinedProperty property )
    {
        if ( !nodeTrackers.isEmpty() )
        {
            Node node = entityFactory.newNodeProxyById( nodeId );
            for ( PropertyTracker<Node> tracker : nodeTrackers )
            {
                tracker.propertyRemoved( node, propertyKeyName( property ), property.value() );
            }
        }
    }

    public void relationshipRemoveStoreProperty( long relationshipId, DefinedProperty property )
    {
        if ( !relationshipTrackers.isEmpty() )
        {
            Relationship relationship = entityFactory.newRelationshipProxyById( relationshipId );
            for ( PropertyTracker<Relationship> tracker : relationshipTrackers )
            {
                tracker.propertyRemoved( relationship, propertyKeyName( property ), property.value() );
            }
        }
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
    
    public void nodeDelete( long nodeId )
    {
        if ( !nodeTrackers.isEmpty() )
        {
            Node node = entityFactory.newNodeProxyById( nodeId );
            for ( Map.Entry<String, Object> property : node.getAllProperties().entrySet() )
            {
                Object value = property.getValue();
                for ( PropertyTracker<Node> tracker : nodeTrackers )
                {
                    tracker.propertyRemoved( node, property.getKey(), value );
                }
            }
        }
    }

    public void relationshipDelete( long relationshipId )
    {
        if ( !relationshipTrackers.isEmpty() )
        {
            Relationship relationship = entityFactory.newRelationshipProxyById( relationshipId );

            for ( Map.Entry<String, Object> property : relationship.getAllProperties().entrySet() )
            {
                Object value = property.getValue();
                for ( PropertyTracker<Relationship> tracker : relationshipTrackers )
                {
                    tracker.propertyRemoved( relationship, property.getKey(), value );
                }
            }
        }

    }
}
