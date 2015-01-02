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
package org.neo4j.kernel.impl.event;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.event.PropertyEntry;
import org.neo4j.graphdb.event.TransactionData;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

class ExpectedTransactionData
{
    final Set<Node> expectedCreatedNodes = new HashSet<Node>();
    final Set<Relationship> expectedCreatedRelationships = new HashSet<Relationship>();
    final Set<Node> expectedDeletedNodes = new HashSet<Node>();
    final Set<Relationship> expectedDeletedRelationships = new HashSet<Relationship>();
    final Map<Node, Map<String, PropertyEntryImpl<Node>>> expectedAssignedNodeProperties =
            new HashMap<Node, Map<String, PropertyEntryImpl<Node>>>();
    final Map<Relationship, Map<String, PropertyEntryImpl<Relationship>>> expectedAssignedRelationshipProperties =
            new HashMap<Relationship, Map<String, PropertyEntryImpl<Relationship>>>();
    final Map<Node, Map<String, PropertyEntryImpl<Node>>> expectedRemovedNodeProperties =
            new HashMap<Node, Map<String, PropertyEntryImpl<Node>>>();
    final Map<Relationship, Map<String, PropertyEntryImpl<Relationship>>> expectedRemovedRelationshipProperties =
            new HashMap<Relationship, Map<String, PropertyEntryImpl<Relationship>>>();
    
    void assignedProperty( Node node, String key, Object value, Object valueBeforeTx )
    {
        putInMap( this.expectedAssignedNodeProperties, node, key, value, valueBeforeTx );
    }
    
    void assignedProperty( Relationship rel, String key, Object value, Object valueBeforeTx )
    {
        putInMap( this.expectedAssignedRelationshipProperties, rel, key, value, valueBeforeTx );
    }
    
    void removedProperty( Node node, String key, Object value, Object valueBeforeTx )
    {
        putInMap( this.expectedRemovedNodeProperties, node, key, value, valueBeforeTx );
    }
    
    void removedProperty( Relationship rel, String key, Object value, Object valueBeforeTx )
    {
        putInMap( this.expectedRemovedRelationshipProperties, rel, key, value, valueBeforeTx );
    }
    
    <T extends PropertyContainer> void putInMap( Map<T, Map<String, PropertyEntryImpl<T>>> map,
            T entity, String key, Object value, Object valueBeforeTx )
    {
        Map<String, PropertyEntryImpl<T>> innerMap = map.get( entity );
        if ( innerMap == null )
        {
            innerMap = new HashMap<String, PropertyEntryImpl<T>>();
            map.put( entity, innerMap );
        }
        innerMap.put( key, new PropertyEntryImpl<T>( entity, key, value, valueBeforeTx ) );
    }
    
    void compareTo( TransactionData data )
    {
        Set<Node> expectedCreatedNodes = new HashSet<Node>( this.expectedCreatedNodes );
        Set<Relationship> expectedCreatedRelationships = new HashSet<Relationship>( this.expectedCreatedRelationships );
        Set<Node> expectedDeletedNodes = new HashSet<Node>( this.expectedDeletedNodes );
        Set<Relationship> expectedDeletedRelationships = new HashSet<Relationship>( this.expectedDeletedRelationships );
        Map<Node, Map<String, PropertyEntryImpl<Node>>> expectedAssignedNodeProperties =
                clone( this.expectedAssignedNodeProperties );
        Map<Relationship, Map<String, PropertyEntryImpl<Relationship>>> expectedAssignedRelationshipProperties =
                clone( this.expectedAssignedRelationshipProperties );
        Map<Node, Map<String, PropertyEntryImpl<Node>>> expectedRemovedNodeProperties =
                clone( this.expectedRemovedNodeProperties );
        Map<Relationship, Map<String, PropertyEntryImpl<Relationship>>> expectedRemovedRelationshipProperties =
                clone( this.expectedRemovedRelationshipProperties );
        
        for ( Node node : data.createdNodes() )
        {
            assertTrue( expectedCreatedNodes.remove( node ) );
            assertFalse( data.isDeleted( node ) );
        }
        assertTrue( "Expected some created nodes that weren't seen: " + expectedCreatedNodes,
                expectedCreatedNodes.isEmpty() );
        
        for ( Relationship rel : data.createdRelationships() )
        {
            assertTrue( expectedCreatedRelationships.remove( rel ) );
            assertFalse( data.isDeleted( rel ) );
        }
        assertTrue( expectedCreatedRelationships.isEmpty() );
        
        for ( Node node : data.deletedNodes() )
        {
            assertTrue( "Unexpected deleted node " + node, expectedDeletedNodes.remove( node ) );
            assertTrue( data.isDeleted( node ) );
        }
        assertTrue( expectedDeletedNodes.isEmpty() );
        
        for ( Relationship rel : data.deletedRelationships() )
        {
            assertTrue( expectedDeletedRelationships.remove( rel ) );
            assertTrue( data.isDeleted( rel ) );
        }
        assertTrue( expectedDeletedRelationships.isEmpty() );
        
        for ( PropertyEntry<Node> entry : data.assignedNodeProperties() )
        {
            checkAssigned( expectedAssignedNodeProperties, entry );
            assertFalse( data.isDeleted( entry.entity() ) );
        }
        assertTrue( "Expected node properties not encountered " + expectedAssignedNodeProperties,
                expectedAssignedNodeProperties.isEmpty() );

        for ( PropertyEntry<Relationship> entry : data.assignedRelationshipProperties() )
        {
            checkAssigned( expectedAssignedRelationshipProperties, entry );
            assertFalse( data.isDeleted( entry.entity() ) );
        }
        assertTrue( expectedAssignedRelationshipProperties.isEmpty() );

        for ( PropertyEntry<Node> entry : data.removedNodeProperties() )
        {
            checkRemoved( expectedRemovedNodeProperties, entry );
        }
        assertTrue( expectedRemovedNodeProperties.isEmpty() );

        for ( PropertyEntry<Relationship> entry : data.removedRelationshipProperties() )
        {
            checkRemoved( expectedRemovedRelationshipProperties, entry );
        }
        assertTrue( expectedRemovedRelationshipProperties.isEmpty() );
    }
    
    private <KEY extends PropertyContainer> Map<KEY, Map<String, PropertyEntryImpl<KEY>>> clone(
            Map<KEY, Map<String, PropertyEntryImpl<KEY>>> map )
    {
        Map<KEY, Map<String, PropertyEntryImpl<KEY>>> result = new HashMap<>();
        for ( KEY key : map.keySet() )
        {
            result.put( key, new HashMap<>( map.get( key ) ) );
        }
        return result;
    }

    <T extends PropertyContainer> void checkAssigned(
            Map<T, Map<String, PropertyEntryImpl<T>>> map, PropertyEntry<T> entry )
    {
        fetchExpectedPropertyEntry( map, entry ).compareToAssigned( entry );
    }

    <T extends PropertyContainer> void checkRemoved(
            Map<T, Map<String, PropertyEntryImpl<T>>> map, PropertyEntry<T> entry )
    {
        fetchExpectedPropertyEntry( map, entry ).compareToRemoved( entry );
    }
    
    <T extends PropertyContainer> PropertyEntryImpl<T> fetchExpectedPropertyEntry(
            Map<T, Map<String, PropertyEntryImpl<T>>> map, PropertyEntry<T> entry )
    {
        Map<String, PropertyEntryImpl<T>> innerMap = map.get( entry.entity() );
        assertNotNull( innerMap );
        PropertyEntryImpl<T> expectedEntry = innerMap.remove( entry.key() );
        assertNotNull( "Unexpacted property entry " + entry, expectedEntry );
        if ( innerMap.isEmpty() )
        {
            map.remove( entry.entity() );
        }
        return expectedEntry;
    }
}
