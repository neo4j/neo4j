/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.api.index;

import org.junit.Test;

import java.util.Optional;

import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.api.properties.Property;
import org.neo4j.kernel.api.schema_new.index.NewIndexDescriptor;
import org.neo4j.kernel.api.schema_new.index.NewIndexDescriptorFactory;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class NodeUpdatesTest
{
    private static final long nodeId = 0;
    private static final int labelId = 0;
    private static final int propertyKeyId1 = 0;
    private static final int propertyKeyId2 = 1;
    private static final long[] labels = new long[]{labelId};
    private static final long[] empty = new long[]{};
    private static final NewIndexDescriptor index1 = NewIndexDescriptorFactory.forLabel( labelId, propertyKeyId1 );
    private static final NewIndexDescriptor index2 = NewIndexDescriptorFactory.forLabel( labelId, propertyKeyId2 );
    private static final NewIndexDescriptor index12 = NewIndexDescriptorFactory.forLabel( labelId, propertyKeyId1, propertyKeyId2 );
    private static final DefinedProperty property1 = Property.stringProperty( propertyKeyId1, "Neo" );
    private static final DefinedProperty property2 = Property.longProperty( propertyKeyId2, 100L );
    private static final Object[] values12 = new Object[]{property1.value(), property2.value()};

    @Test
    public void shouldNotGenerateUpdatesForEmptyNodeUpdates()
    {
        // When
        NodeUpdates updates = NodeUpdates.forNode( nodeId ).build();

        // Then
        assertFalse( "Expected no updates", updates.hasIndexingAppropriateUpdates() );
        assertFalse( "Expected no index updates", updates.forIndex( index1 ).isPresent() );
        assertFalse( "Expected no index updates", updates.forIndex( index2 ).isPresent() );
        assertFalse( "Expected no index updates", updates.forIndex( index12 ).isPresent() );
    }

    @Test
    public void shouldNotGenerateUpdateForMultipleExistingPropertiesAndLabels()
    {
        // When
        NodeUpdates updates = NodeUpdates.forNode( nodeId, labels )
                .buildWithExistingProperties( property1, property2 );

        // Then
        assertFalse( "Expected no updates", updates.hasIndexingAppropriateUpdates() );
        assertFalse( "Expected no index updates", updates.forIndex( index1 ).isPresent() );
        assertFalse( "Expected no index updates", updates.forIndex( index2 ).isPresent() );
        assertFalse( "Expected no index updates", updates.forIndex( index12 ).isPresent() );
    }

    @Test
    public void shouldNotGenerateUpdatesForLabelAdditionWithNoProperties()
    {
        // When
        NodeUpdates updates = NodeUpdates.forNode( nodeId, empty, labels ).build();

        // Then
        assertFalse( "Expected no updates", updates.hasIndexingAppropriateUpdates() );
        assertFalse( "Expected no index updates", updates.forIndex( index1 ).isPresent() );
        assertFalse( "Expected no index updates", updates.forIndex( index2 ).isPresent() );
        assertFalse( "Expected no index updates", updates.forIndex( index12 ).isPresent() );
    }

    @Test
    public void shouldGenerateUpdateForLabelAdditionWithExistingProperty()
    {
        // When
        NodeUpdates updates =
                NodeUpdates.forNode( nodeId, empty, labels ).buildWithExistingProperties( property1 );

        // Then
        assertTrue( "Expected to have updates", updates.hasIndexingAppropriateUpdates() );

        // And also
        Optional<IndexEntryUpdate> indexEntryUpdate = updates.forIndex( index1 );
        assertTrue( "Expected an index update for index1", indexEntryUpdate.isPresent() );
        assertThat( indexEntryUpdate.get(), equalTo( IndexEntryUpdate.add( nodeId, index1, property1.value() ) ) );

        // And also
        assertFalse( "Expected no index updates for index2", updates.forIndex( index2 ).isPresent() );

        // And also
        assertFalse( "Expected no index updates", updates.forIndex( index12 ).isPresent() );
    }

    @Test
    public void shouldGenerateUpdatesForLabelAdditionWithExistingProperties()
    {
        // When
        NodeUpdates updates =
                NodeUpdates.forNode( nodeId, empty, labels ).buildWithExistingProperties( property1, property2 );

        // Then
        assertTrue( "Expected to have updates", updates.hasIndexingAppropriateUpdates() );

        // And also
        Optional<IndexEntryUpdate> indexEntryUpdate1 = updates.forIndex( index1 );
        assertTrue( "Expected an index update for index1", indexEntryUpdate1.isPresent() );
        assertThat( indexEntryUpdate1.get(), equalTo( IndexEntryUpdate.add( nodeId, index1, property1.value() ) ) );

        // And also
        Optional<IndexEntryUpdate> indexEntryUpdate2 = updates.forIndex( index2 );
        assertTrue( "Expected an index update for index2", indexEntryUpdate2.isPresent() );
        assertThat( indexEntryUpdate2.get(), equalTo( IndexEntryUpdate.add( nodeId, index2, property2.value() ) ) );

        // And also
        Optional<IndexEntryUpdate> indexEntryUpdate12 = updates.forIndex( index12 );
        assertTrue( "Expected an index update for index12", indexEntryUpdate12.isPresent() );
        assertThat( indexEntryUpdate12.get(), equalTo( IndexEntryUpdate.add( nodeId, index12, values12 ) ) );
    }

    @Test
    public void shouldNotGenerateUpdatesForLabelRemovalWithNoProperties()
    {
        // When
        NodeUpdates updates = NodeUpdates.forNode( nodeId, labels, empty ).build();

        // Then
        assertFalse( "Expected no updates", updates.hasIndexingAppropriateUpdates() );
        assertFalse( "Expected no index updates", updates.forIndex( index1 ).isPresent() );
        assertFalse( "Expected no index updates", updates.forIndex( index2 ).isPresent() );
        assertFalse( "Expected no index updates", updates.forIndex( index12 ).isPresent() );
    }

    @Test
    public void shouldGenerateUpdateForLabelRemovalWithExistingProperty()
    {
        // When
        NodeUpdates updates =
                NodeUpdates.forNode( nodeId, labels, empty ).buildWithExistingProperties( property1 );

        // Then
        assertTrue( "Expected to have updates", updates.hasIndexingAppropriateUpdates() );

        // And also
        Optional<IndexEntryUpdate> indexEntryUpdate = updates.forIndex( index1 );
        assertTrue( "Expected an index update for index1", indexEntryUpdate.isPresent() );
        assertThat( indexEntryUpdate.get(), equalTo( IndexEntryUpdate.remove( nodeId, index1, property1.value() ) ) );

        // And also
        assertFalse( "Expected no index updates for index2", updates.forIndex( index2 ).isPresent() );

        // And also
        assertFalse( "Expected no index updates for index12", updates.forIndex( index12 ).isPresent() );
    }

    @Test
    public void shouldGenerateUpdatesForLabelRemovalWithExistingProperties()
    {
        // When
        NodeUpdates updates =
                NodeUpdates.forNode( nodeId, labels, empty ).buildWithExistingProperties( property1, property2 );

        // Then
        assertTrue( "Expected to have updates", updates.hasIndexingAppropriateUpdates() );

        // And also
        Optional<IndexEntryUpdate> indexEntryUpdate1 = updates.forIndex( index1 );
        assertTrue( "Expected an index update for index1", indexEntryUpdate1.isPresent() );
        assertThat( indexEntryUpdate1.get(), equalTo( IndexEntryUpdate.remove( nodeId, index1, property1.value() ) ) );

        // And also
        Optional<IndexEntryUpdate> indexEntryUpdate2 = updates.forIndex( index2 );
        assertTrue( "Expected an index update for index2", indexEntryUpdate2.isPresent() );
        assertThat( indexEntryUpdate2.get(), equalTo( IndexEntryUpdate.remove( nodeId, index2, property2.value() ) ) );

        // And also
        Optional<IndexEntryUpdate> indexEntryUpdate12 = updates.forIndex( index12 );
        assertTrue( "Expected an index update for index12", indexEntryUpdate12.isPresent() );
        assertThat( indexEntryUpdate12.get(), equalTo( IndexEntryUpdate.remove( nodeId, index12, values12 ) ) );
    }

    @Test
    public void shouldNotGenerateUpdatesForPropertyAdditionWithNoLabels()
    {
        // When
        NodeUpdates updates = NodeUpdates.forNode( nodeId )
                .added( property1.propertyKeyId(), property1.value() )
                .build();

        // Then
        assertFalse( "Expected no updates", updates.hasIndexingAppropriateUpdates() );
        assertFalse( "Expected no index updates", updates.forIndex( index1 ).isPresent() );
        assertFalse( "Expected no index updates", updates.forIndex( index2 ).isPresent() );
        assertFalse( "Expected no index updates", updates.forIndex( index12 ).isPresent() );
    }

    @Test
    public void shouldGenerateUpdatesForSinglePropertyAdditionWithLabels()
    {
        // When
        NodeUpdates updates = NodeUpdates.forNode( nodeId, labels )
                .added( property1.propertyKeyId(), property1.value() )
                .build();

        // Then
        assertTrue( "Expected to have updates", updates.hasIndexingAppropriateUpdates() );

        // And also
        Optional<IndexEntryUpdate> indexEntryUpdate = updates.forIndex( index1 );
        assertTrue( "Expected an index update for index1", indexEntryUpdate.isPresent() );
        assertThat( indexEntryUpdate.get(), equalTo( IndexEntryUpdate.add( nodeId, index1, property1.value() ) ) );

        // And also
        assertFalse( "Expected no index updates for index2", updates.forIndex( index2 ).isPresent() );

        // And also
        assertFalse( "Expected no index updates for index12", updates.forIndex( index12 ).isPresent() );
    }

    @Test
    public void shouldGenerateUpdatesForMultiplePropertyAdditionWithLabels()
    {
        // When
        NodeUpdates updates = NodeUpdates.forNode( nodeId, labels )
                .added( property1.propertyKeyId(), property1.value() )
                .added( property2.propertyKeyId(), property2.value() )
                .build();

        // Then
        assertTrue( "Expected to have updates", updates.hasIndexingAppropriateUpdates() );

        // And also
        Optional<IndexEntryUpdate> indexEntryUpdate = updates.forIndex( index1 );
        assertTrue( "Expected an index update for index1", indexEntryUpdate.isPresent() );
        assertThat( indexEntryUpdate.get(), equalTo( IndexEntryUpdate.add( nodeId, index1, property1.value() ) ) );

        // And also
        Optional<IndexEntryUpdate> indexEntryUpdate2 = updates.forIndex( index2 );
        assertTrue( "Expected an index update for index1", indexEntryUpdate2.isPresent() );
        assertThat( indexEntryUpdate2.get(), equalTo( IndexEntryUpdate.add( nodeId, index2, property2.value() ) ) );

        // And also
        Optional<IndexEntryUpdate> indexEntryUpdate12 = updates.forIndex( index12 );
        assertTrue( "Expected an index update for index12", indexEntryUpdate12.isPresent() );
        assertThat( indexEntryUpdate12.get(), equalTo( IndexEntryUpdate.add( nodeId, index12, values12 ) ) );
    }

    @Test
    public void shouldNotGenerateUpdatesForLabelAddAndPropertyRemove()
    {
        // When
        NodeUpdates updates = NodeUpdates.forNode( nodeId, empty, labels )
                .removed( property1.propertyKeyId(), property1.value() )
                .removed( property2.propertyKeyId(), property2.value() )
                .build();

        // Then
        assertFalse( "Expected no updates", updates.hasIndexingAppropriateUpdates() );
        assertFalse( "Expected no index updates", updates.forIndex( index1 ).isPresent() );
        assertFalse( "Expected no index updates", updates.forIndex( index2 ).isPresent() );
        assertFalse( "Expected no index updates", updates.forIndex( index12 ).isPresent() );
    }

    @Test
    public void shouldNotGenerateUpdatesForLabelRemoveAndPropertyAdd()
    {
        // When
        NodeUpdates updates = NodeUpdates.forNode( nodeId, labels, empty )
                .added( property1.propertyKeyId(), property1.value() )
                .added( property2.propertyKeyId(), property2.value() )
                .build();

        // Then
        assertFalse( "Expected no updates", updates.hasIndexingAppropriateUpdates() );
        assertFalse( "Expected no index updates", updates.forIndex( index1 ).isPresent() );
        assertFalse( "Expected no index updates", updates.forIndex( index2 ).isPresent() );
        assertFalse( "Expected no index updates", updates.forIndex( index12 ).isPresent() );
    }

}
