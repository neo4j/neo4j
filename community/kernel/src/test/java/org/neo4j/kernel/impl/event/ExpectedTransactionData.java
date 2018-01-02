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
package org.neo4j.kernel.impl.event;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.event.LabelEntry;
import org.neo4j.graphdb.event.PropertyEntry;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.kernel.impl.util.AutoCreatingHashMap;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import static org.neo4j.kernel.impl.util.AutoCreatingHashMap.nested;

class ExpectedTransactionData
{
    final Set<Node> expectedCreatedNodes = new HashSet<>();
    final Set<Relationship> expectedCreatedRelationships = new HashSet<>();
    final Set<Node> expectedDeletedNodes = new HashSet<>();
    final Set<Relationship> expectedDeletedRelationships = new HashSet<>();
    final Map<Node, Map<String, PropertyEntryImpl<Node>>> expectedAssignedNodeProperties =
            new AutoCreatingHashMap<>( nested( String.class, AutoCreatingHashMap.<PropertyEntryImpl<Node>>dontCreate() ) );
    final Map<Relationship, Map<String, PropertyEntryImpl<Relationship>>> expectedAssignedRelationshipProperties =
            new AutoCreatingHashMap<>( nested( String.class, AutoCreatingHashMap.<PropertyEntryImpl<Relationship>>dontCreate() ) );
    final Map<Node, Map<String, PropertyEntryImpl<Node>>> expectedRemovedNodeProperties =
            new AutoCreatingHashMap<>( nested( String.class, AutoCreatingHashMap.<PropertyEntryImpl<Node>>dontCreate() ) );
    final Map<Relationship, Map<String, PropertyEntryImpl<Relationship>>> expectedRemovedRelationshipProperties =
            new AutoCreatingHashMap<>( nested( String.class, AutoCreatingHashMap.<PropertyEntryImpl<Relationship>>dontCreate() ) );
    final Map<Node, Set<String>> expectedAssignedLabels =
            new AutoCreatingHashMap<>( AutoCreatingHashMap.<String>valuesOfTypeHashSet() );
    final Map<Node, Set<String>> expectedRemovedLabels =
            new AutoCreatingHashMap<>( AutoCreatingHashMap.<String>valuesOfTypeHashSet() );
    private final boolean ignoreAdditionalData;

    /**
     * @param ignoreAdditionalData if {@code true} then only compare the expected data. If the transaction data
     * contains data in addition to that, then ignore that data. The reason is that for some scenarios
     * it's hard to anticipate the full extent of the transaction data. F.ex. deleting a node will
     * have all its committed properties seen as removed as well. To tell this instance about that expectancy
     * is difficult if there have been other property changes for that node within the same transaction
     * before deleting that node. It's possible, it's just that it will require some tedious state keeping
     * on the behalf of the test.
     */
    ExpectedTransactionData( boolean ignoreAdditionalData )
    {
        this.ignoreAdditionalData = ignoreAdditionalData;
    }

    ExpectedTransactionData()
    {
        this( false );
    }

    void clear()
    {
        expectedAssignedNodeProperties.clear();
        expectedAssignedRelationshipProperties.clear();
        expectedCreatedNodes.clear();
        expectedCreatedRelationships.clear();
        expectedDeletedNodes.clear();
        expectedDeletedRelationships.clear();
        expectedRemovedNodeProperties.clear();
        expectedRemovedRelationshipProperties.clear();
        expectedAssignedLabels.clear();
        expectedRemovedLabels.clear();
    }

    void createdNode( Node node )
    {
        expectedCreatedNodes.add( node );
    }

    void deletedNode( Node node )
    {
        if ( !expectedCreatedNodes.remove( node ) )
        {
            expectedDeletedNodes.add( node );
        }
        expectedAssignedNodeProperties.remove( node );
        expectedAssignedLabels.remove( node );
        expectedRemovedNodeProperties.remove( node );
        expectedRemovedLabels.remove( node );
    }

    void createdRelationship( Relationship relationship )
    {
        expectedCreatedRelationships.add( relationship );
    }

    void deletedRelationship( Relationship relationship )
    {
        if ( !expectedCreatedRelationships.remove( relationship ) )
        {
            expectedDeletedRelationships.add( relationship );
        }
        expectedAssignedRelationshipProperties.remove( relationship );
        expectedRemovedRelationshipProperties.remove( relationship );
    }

    void assignedProperty( Node node, String key, Object value, Object valueBeforeTx )
    {
        valueBeforeTx = removeProperty( expectedRemovedNodeProperties, node, key, valueBeforeTx );
        Map<String,PropertyEntryImpl<Node>> map = expectedAssignedNodeProperties.get( node );
        PropertyEntryImpl<Node> prev = map.get( key );
        map.put( key, property( node, key, value, prev != null ? prev.previouslyCommitedValue() : valueBeforeTx ) );
    }

    void assignedProperty( Relationship rel, String key, Object value, Object valueBeforeTx )
    {
        valueBeforeTx = removeProperty( expectedRemovedRelationshipProperties, rel, key, valueBeforeTx );
        Map<String,PropertyEntryImpl<Relationship>> map = expectedAssignedRelationshipProperties.get( rel );
        PropertyEntryImpl<Relationship> prev = map.get( key );
        map.put( key, property( rel, key, value, prev != null ? prev.previouslyCommitedValue() : valueBeforeTx ) );
    }

    void assignedLabel( Node node, Label label )
    {
        if ( removeLabel( expectedRemovedLabels, node, label ) )
        {
            expectedAssignedLabels.get( node ).add( label.name() );
        }
    }

    void removedLabel( Node node, Label label )
    {
        if ( removeLabel( expectedAssignedLabels, node, label ) )
        {
            expectedRemovedLabels.get( node ).add( label.name() );
        }
    }

    /**
     * @return {@code true} if this property should be expected to come as removed property in the event
     */
    private boolean removeLabel( Map<Node,Set<String>> map, Node node, Label label )
    {
        if ( map.containsKey( node ) )
        {
            Set<String> set = map.get( node );
            if ( !set.remove( label.name() ) )
            {
                return true;
            }
            if ( set.isEmpty() )
            {
                map.remove( node );
            }
        }
        return false;
    }

    void removedProperty( Node node, String key, Object valueBeforeTx )
    {
        if ( (valueBeforeTx = removeProperty( expectedAssignedNodeProperties, node, key, valueBeforeTx )) != null )
        {
            expectedRemovedNodeProperties.get( node ).put( key, property( node, key, null, valueBeforeTx ) );
        }
    }

    void removedProperty( Relationship rel, String key, Object valueBeforeTx )
    {
        if ( (valueBeforeTx = removeProperty( expectedAssignedRelationshipProperties, rel, key, valueBeforeTx )) != null )
        {
            expectedRemovedRelationshipProperties.get( rel ).put( key, property( rel, key, null, valueBeforeTx ) );
        }
    }

    /**
     * @return {@code non-null} if this property should be expected to come as removed property in the event
     */
    private <E extends PropertyContainer> Object removeProperty( Map<E,Map<String,PropertyEntryImpl<E>>> map,
            E entity, String key, Object valueBeforeTx )
    {
        if ( map.containsKey( entity ) )
        {
            Map<String,PropertyEntryImpl<E>> inner = map.get( entity );
            PropertyEntryImpl<E> entry = inner.remove( key );
            if ( entry == null )
            {   // this means that we've been called to remove an existing property
                return valueBeforeTx;
            }

            if ( inner.isEmpty() )
            {
                map.remove( entity );
            }
            if ( entry.previouslyCommitedValue() != null )
            {   // this means that we're removing a previously changed property, i.e. there's a value to remove
                return entry.previouslyCommitedValue();
            }
            return null;
        }
        return valueBeforeTx;
    }

    private <E extends PropertyContainer> PropertyEntryImpl<E> property( E entity, String key, Object value,
            Object valueBeforeTx )
    {
        return new PropertyEntryImpl<>( entity, key, value, valueBeforeTx );
    }

    void compareTo( TransactionData data )
    {
        Set<Node> expectedCreatedNodes = new HashSet<>( this.expectedCreatedNodes );
        Set<Relationship> expectedCreatedRelationships = new HashSet<>( this.expectedCreatedRelationships );
        Set<Node> expectedDeletedNodes = new HashSet<Node>( this.expectedDeletedNodes );
        Set<Relationship> expectedDeletedRelationships = new HashSet<>( this.expectedDeletedRelationships );
        Map<Node, Map<String, PropertyEntryImpl<Node>>> expectedAssignedNodeProperties =
                clone( this.expectedAssignedNodeProperties );
        Map<Relationship, Map<String, PropertyEntryImpl<Relationship>>> expectedAssignedRelationshipProperties =
                clone( this.expectedAssignedRelationshipProperties );
        Map<Node, Map<String, PropertyEntryImpl<Node>>> expectedRemovedNodeProperties =
                clone( this.expectedRemovedNodeProperties );
        Map<Relationship, Map<String, PropertyEntryImpl<Relationship>>> expectedRemovedRelationshipProperties =
                clone( this.expectedRemovedRelationshipProperties );
        Map<Node,Set<String>> expectedAssignedLabels = cloneLabelData( this.expectedAssignedLabels );
        Map<Node,Set<String>> expectedRemovedLabels = cloneLabelData( this.expectedRemovedLabels );

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
        assertTrue( "Expected created relationships not encountered " + expectedCreatedRelationships,
                expectedCreatedRelationships.isEmpty() );

        for ( Node node : data.deletedNodes() )
        {
            assertTrue( "Unexpected deleted node " + node, expectedDeletedNodes.remove( node ) );
            assertTrue( data.isDeleted( node ) );
        }
        assertTrue( "Expected deleted nodes: " + expectedDeletedNodes, expectedDeletedNodes.isEmpty() );

        for ( Relationship rel : data.deletedRelationships() )
        {
            assertTrue( expectedDeletedRelationships.remove( rel ) );
            assertTrue( data.isDeleted( rel ) );
        }
        assertTrue( "Expected deleted relationships not encountered " + expectedDeletedRelationships,
                expectedDeletedRelationships.isEmpty() );

        for ( PropertyEntry<Node> entry : data.assignedNodeProperties() )
        {
            checkAssigned( expectedAssignedNodeProperties, entry );
            assertFalse( data.isDeleted( entry.entity() ) );
        }
        assertTrue( "Expected assigned node properties not encountered " + expectedAssignedNodeProperties,
                expectedAssignedNodeProperties.isEmpty() );

        for ( PropertyEntry<Relationship> entry : data.assignedRelationshipProperties() )
        {
            checkAssigned( expectedAssignedRelationshipProperties, entry );
            assertFalse( data.isDeleted( entry.entity() ) );
        }
        assertTrue( "Expected assigned relationship properties not encountered " + expectedAssignedRelationshipProperties,
                expectedAssignedRelationshipProperties.isEmpty() );

        for ( PropertyEntry<Node> entry : data.removedNodeProperties() )
        {
            checkRemoved( expectedRemovedNodeProperties, entry );
        }
        assertTrue( "Expected removed node properties not encountered " + expectedRemovedNodeProperties,
                expectedRemovedNodeProperties.isEmpty() );

        for ( PropertyEntry<Relationship> entry : data.removedRelationshipProperties() )
        {
            checkRemoved( expectedRemovedRelationshipProperties, entry );
        }
        assertTrue( "Expected removed relationship properties not encountered " + expectedRemovedRelationshipProperties,
                expectedRemovedRelationshipProperties.isEmpty() );

        for ( LabelEntry entry : data.assignedLabels() )
        {
            check( expectedAssignedLabels, entry );
        }
        assertTrue( "Expected assigned labels not encountered " + expectedAssignedLabels,
                expectedAssignedLabels.isEmpty() );

        for ( LabelEntry entry : data.removedLabels() )
        {
            check( expectedRemovedLabels, entry );
        }
        assertTrue( "Expected removed labels not encountered " + expectedRemovedLabels,
                expectedRemovedLabels.isEmpty() );
    }

    private Map<Node,Set<String>> cloneLabelData( Map<Node,Set<String>> map )
    {
        Map<Node,Set<String>> clone = new HashMap<>();
        for ( Map.Entry<Node,Set<String>> entry : map.entrySet() )
        {
            clone.put( entry.getKey(), new HashSet<>( entry.getValue() ) );
        }
        return clone;
    }

    private void check( Map<Node,Set<String>> expected, LabelEntry entry )
    {
        Node node = entry.node();
        String labelName = entry.label().name();
        boolean hasEntity = expected.containsKey( node );
        if ( !hasEntity && ignoreAdditionalData )
        {
            return;
        }
        assertTrue( "Unexpected node " + node, hasEntity );
        Set<String> labels = expected.get( node );
        boolean hasLabel = labels.remove( labelName );
        if ( !hasLabel && ignoreAdditionalData )
        {
            return;
        }
        assertTrue( "Unexpected label " + labelName + " for " + node, hasLabel );
        if ( labels.isEmpty() )
        {
            expected.remove( node );
        }
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
        PropertyEntryImpl<T> expected = fetchExpectedPropertyEntry( map, entry );
        if ( expected != null )
        {   // To handle the ignore flag (read above)
            expected.compareToAssigned( entry );
        }
    }

    <T extends PropertyContainer> void checkRemoved(
            Map<T, Map<String, PropertyEntryImpl<T>>> map, PropertyEntry<T> entry )
    {
        PropertyEntryImpl<T> expected = fetchExpectedPropertyEntry( map, entry );
        if ( expected != null )
        {   // To handle the ignore flag (read above)
            expected.compareToRemoved( entry );
        }
    }

    <T extends PropertyContainer> PropertyEntryImpl<T> fetchExpectedPropertyEntry(
            Map<T, Map<String, PropertyEntryImpl<T>>> map, PropertyEntry<T> entry )
    {
        T entity = entry.entity();
        boolean hasEntity = map.containsKey( entity );
        if ( ignoreAdditionalData && !hasEntity )
        {
            return null;
        }
        assertTrue( "Unexpected entity " + entry, hasEntity );
        Map<String, PropertyEntryImpl<T>> innerMap = map.get( entity );
        PropertyEntryImpl<T> expectedEntry = innerMap.remove( entry.key() );
        if ( expectedEntry == null && ignoreAdditionalData )
        {
            return null;
        }
        assertNotNull( "Unexpected property entry " + entry, expectedEntry );
        if ( innerMap.isEmpty() )
        {
            map.remove( entity );
        }
        return expectedEntry;
    }
}
