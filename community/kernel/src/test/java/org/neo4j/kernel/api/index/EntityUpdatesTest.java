/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.common.EntityType;
import org.neo4j.internal.schema.LabelSchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.storageengine.api.EntityUpdates;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.storageengine.api.PropertyKeyValue;
import org.neo4j.storageengine.api.StorageProperty;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.storageengine.api.StubStorageCursors;
import org.neo4j.token.api.NamedToken;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static java.util.Collections.singleton;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class EntityUpdatesTest
{
    private static final long nodeId = 0;
    private static final int labelId1 = 0;
    private static final int labelId2 = 1;
    private static final int unusedLabelId = 2;
    private static final int propertyKeyId1 = 0;
    private static final int propertyKeyId2 = 1;
    private static final int propertyKeyId3 = 2;
    private static final long[] label = new long[]{labelId1};
    private static final long[] allLabels = new long[]{labelId1, labelId2};
    private static final long[] empty = new long[]{};

    private static final LabelSchemaDescriptor index1 = SchemaDescriptor.forLabel( labelId1, propertyKeyId1 );
    private static final LabelSchemaDescriptor index2 = SchemaDescriptor.forLabel( labelId1, propertyKeyId2 );
    private static final LabelSchemaDescriptor index3 = SchemaDescriptor.forLabel( labelId1, propertyKeyId3 );
    private static final LabelSchemaDescriptor index123 = SchemaDescriptor.forLabel( labelId1, propertyKeyId1, propertyKeyId2, propertyKeyId3 );
    private static final List<LabelSchemaDescriptor> indexes = Arrays.asList( index1, index2, index3, index123 );
    private static final SchemaDescriptor nonSchemaIndex = SchemaDescriptor.fulltext( EntityType.NODE, new int[]{labelId1, labelId2},
            new int[]{propertyKeyId1, propertyKeyId2, propertyKeyId3} );

    private static final StorageProperty property1 = new PropertyKeyValue( propertyKeyId1, Values.of( "Neo" ) );
    private static final StorageProperty property2 = new PropertyKeyValue( propertyKeyId2, Values.of( 100L ) );
    private static final StorageProperty property3 = new PropertyKeyValue( propertyKeyId3, Values.pointValue( CoordinateReferenceSystem.WGS84, 12.3, 45.6 ) );
    private static final Value[] values123 = new Value[]{property1.value(), property2.value(), property3.value()};

    @Test
    void shouldNotGenerateUpdatesForEmptyNodeUpdates()
    {
        // When
        EntityUpdates updates = EntityUpdates.forEntity( nodeId, false ).build();

        // Then
        assertThat( updates.forIndexKeys( indexes, assertNoLoading(), EntityType.NODE ) ).isEmpty();
    }

    @Test
    void shouldNotGenerateUpdateForMultipleExistingPropertiesAndLabels()
    {
        // When
        EntityUpdates updates = EntityUpdates.forEntity( nodeId, false ).withTokens( label )
                .existing( propertyKeyId1, Values.of( "Neo" ) )
                .existing( propertyKeyId2, Values.of( 100L ) )
                .existing( propertyKeyId3, Values.pointValue( CoordinateReferenceSystem.WGS84, 12.3, 45.6 ) )
                .build();

        // Then
        assertThat( updates.forIndexKeys( indexes, assertNoLoading(), EntityType.NODE ) ).isEmpty();
    }

    @Test
    void shouldNotGenerateUpdatesForLabelAdditionWithNoProperties()
    {
        // When
        EntityUpdates updates = EntityUpdates.forEntity( nodeId, false ).withTokens( empty ).withTokensAfter( label ).build();

        // Then
        assertThat( updates.forIndexKeys( indexes, propertyLoader(), EntityType.NODE ) ).isEmpty();
    }

    @Test
    void shouldGenerateUpdateForLabelAdditionWithExistingProperty()
    {
        // When
        EntityUpdates updates = EntityUpdates.forEntity( nodeId, false ).withTokens( empty ).withTokensAfter( label ).build();

        // Then
        assertThat( updates.forIndexKeys( indexes, propertyLoader( property1 ), EntityType.NODE ) ).contains(
                IndexEntryUpdate.add( nodeId, index1, property1.value() ) );
    }

    @Test
    void shouldGenerateUpdatesForLabelAdditionWithExistingProperties()
    {
        // When
        EntityUpdates updates =
                EntityUpdates.forEntity( nodeId, false ).withTokens( empty ).withTokensAfter( label )
                        .existing( propertyKeyId1, Values.of( "Neo" ) )
                        .existing( propertyKeyId2, Values.of( 100L ) )
                        .existing( propertyKeyId3, Values.pointValue( CoordinateReferenceSystem.WGS84, 12.3, 45.6 ) )
                        .build();

        // Then
        assertThat( updates.forIndexKeys( indexes, propertyLoader( property1, property2, property3 ), EntityType.NODE ) ).contains(
                IndexEntryUpdate.add( nodeId, index1, property1.value() ), IndexEntryUpdate.add( nodeId, index2, property2.value() ),
                IndexEntryUpdate.add( nodeId, index3, property3.value() ), IndexEntryUpdate.add( nodeId, index123, values123 ) );
    }

    @Test
    void shouldNotGenerateUpdateForPartialCompositeSchemaIndexUpdate()
    {
        // When
        EntityUpdates updates =
                EntityUpdates.forEntity( nodeId, false ).withTokens( label ).withTokensAfter( label )
                        .added( propertyKeyId1, Values.of( "Neo" ) )
                        .added( propertyKeyId3, Values.pointValue( CoordinateReferenceSystem.WGS84, 12.3, 45.6 ) )
                        .build();

        // Then
        assertThat( updates.forIndexKeys( singleton( index123 ), propertyLoader(), EntityType.NODE ) ).isEmpty();
    }

    @Test
    void shouldGenerateUpdateForWhenCompletingCompositeSchemaIndexUpdate()
    {
        // When
        EntityUpdates updates =
                EntityUpdates.forEntity( nodeId, false ).withTokens( label ).withTokensAfter( label )
                        .added( propertyKeyId1, Values.of( "Neo" ) )
                        .added( propertyKeyId3, Values.pointValue( CoordinateReferenceSystem.WGS84, 12.3, 45.6 ) )
                        .build();

        // Then
        assertThat( updates.forIndexKeys( singleton( index123 ), propertyLoader( property2 ), EntityType.NODE ) ).contains(
                IndexEntryUpdate.add( nodeId, index123, values123 ) );
    }

    @Test
    void shouldNotGenerateUpdatesForLabelRemovalWithNoProperties()
    {
        // When
        EntityUpdates updates = EntityUpdates.forEntity( nodeId, false ).withTokens( label ).withTokensAfter( empty ).build();

        // Then
        assertThat( updates.forIndexKeys( indexes, propertyLoader(), EntityType.NODE ) ).isEmpty();
    }

    @Test
    void shouldGenerateUpdateForLabelRemovalWithExistingProperty()
    {
        // When
        EntityUpdates updates =
                EntityUpdates.forEntity( nodeId, false ).withTokens( label ).withTokensAfter( empty ).build();

        // Then
        assertThat( updates.forIndexKeys( indexes, propertyLoader( property1 ), EntityType.NODE ) ).contains(
                IndexEntryUpdate.remove( nodeId, index1, property1.value() ) );
    }

    @Test
    void shouldGenerateUpdatesForLabelRemovalWithExistingProperties()
    {
        // When
        EntityUpdates updates =
                EntityUpdates.forEntity( nodeId, false ).withTokens( label ).withTokensAfter( empty ).build();

        // Then
        assertThat( updates.forIndexKeys( indexes, propertyLoader( property1, property2, property3 ), EntityType.NODE ) ).contains(
                IndexEntryUpdate.remove( nodeId, index1, property1.value() ), IndexEntryUpdate.remove( nodeId, index2, property2.value() ),
                IndexEntryUpdate.remove( nodeId, index3, property3.value() ), IndexEntryUpdate.remove( nodeId, index123, values123 ) );
    }

    @Test
    void shouldNotGenerateUpdatesForPropertyAdditionWithNoLabels()
    {
        // When
        EntityUpdates updates = EntityUpdates.forEntity( nodeId, false )
                .added( property1.propertyKeyId(), property1.value() )
                .build();

        // Then
        assertThat( updates.forIndexKeys( indexes, assertNoLoading(), EntityType.NODE ) ).isEmpty();
    }

    @Test
    void shouldGenerateUpdatesForSinglePropertyAdditionWithLabels()
    {
        // When
        EntityUpdates updates = EntityUpdates.forEntity( nodeId, false ).withTokens( label )
                .added( property1.propertyKeyId(), property1.value() )
                .build();

        // Then
        assertThat( updates.forIndexKeys( indexes, propertyLoader(), EntityType.NODE ) ).contains( IndexEntryUpdate.add( nodeId, index1, property1.value() ) );
    }

    @Test
    void shouldGenerateUpdatesForMultiplePropertyAdditionWithLabels()
    {
        // When
        EntityUpdates updates = EntityUpdates.forEntity( nodeId, false ).withTokens( label )
                .added( property1.propertyKeyId(), property1.value() )
                .added( property2.propertyKeyId(), property2.value() )
                .added( property3.propertyKeyId(), property3.value() )
                .build();

        // Then
        assertThat( updates.forIndexKeys( indexes, propertyLoader( property1, property2, property3 ), EntityType.NODE ) ).contains(
                IndexEntryUpdate.add( nodeId, index1, property1.value() ), IndexEntryUpdate.add( nodeId, index2, property2.value() ),
                IndexEntryUpdate.add( nodeId, index3, property3.value() ), IndexEntryUpdate.add( nodeId, index123, values123 ) );
    }

    @Test
    void shouldNotGenerateUpdatesForLabelAddAndPropertyRemove()
    {
        // When
        EntityUpdates updates = EntityUpdates.forEntity( nodeId, false ).withTokens( empty ).withTokensAfter( label )
                .removed( property1.propertyKeyId(), property1.value() )
                .removed( property2.propertyKeyId(), property2.value() )
                .removed( property3.propertyKeyId(), property3.value() )
                .build();

        // Then
        assertThat( updates.forIndexKeys( indexes, assertNoLoading(), EntityType.NODE ) ).isEmpty();
    }

    @Test
    void shouldNotGenerateUpdatesForLabelRemoveAndPropertyAdd()
    {
        // When
        EntityUpdates updates = EntityUpdates.forEntity( nodeId, false ).withTokens( label ).withTokensAfter( empty )
                .added( property1.propertyKeyId(), property1.value() )
                .added( property2.propertyKeyId(), property2.value() )
                .added( property3.propertyKeyId(), property3.value() )
                .build();

        // Then
        assertThat( updates.forIndexKeys( indexes, assertNoLoading(), EntityType.NODE ) ).isEmpty();
    }

    @Test
    void shouldNotLoadPropertyForLabelsAndNoPropertyChanges()
    {
        // When
        EntityUpdates updates = EntityUpdates.forEntity( nodeId, false ).withTokens( label ).build();

        // Then
        assertThat( updates.forIndexKeys( singleton( index1 ), assertNoLoading(), EntityType.NODE ) ).isEmpty();
    }

    @Test
    void shouldNotLoadPropertyForNoLabelsAndButPropertyAddition()
    {
        // When
        EntityUpdates updates = EntityUpdates.forEntity( nodeId, false ).withTokens( empty )
                .added( property1.propertyKeyId(), property1.value() )
                .build();

        // Then
        assertThat( updates.forIndexKeys( singleton( index1 ), assertNoLoading(), EntityType.NODE ) ).isEmpty();
    }

    @Test
    void shouldGenerateUpdateForPartialNonSchemaIndexUpdate()
    {
        // When
        EntityUpdates updates =
                EntityUpdates.forEntity( nodeId, false ).withTokens( label ).withTokensAfter( label )
                        .added( propertyKeyId1, Values.of( "Neo" ) )
                        .build();

        // Then
        assertThat( updates.forIndexKeys( singleton( nonSchemaIndex ), propertyLoader(), EntityType.NODE ) ).contains(
                IndexEntryUpdate.add( nodeId, nonSchemaIndex, property1.value(), null, null ) );
    }

    @Test
    void shouldGenerateUpdateForFullNonSchemaIndexUpdate()
    {
        // When
        EntityUpdates updates =
                EntityUpdates.forEntity( nodeId, false ).withTokens( label ).withTokensAfter( label )
                        .added( property1.propertyKeyId(), property1.value() )
                        .added( property2.propertyKeyId(), property2.value() )
                        .added( property3.propertyKeyId(), property3.value() )
                        .build();

        // Then
        assertThat( updates.forIndexKeys( singleton( nonSchemaIndex ), propertyLoader(), EntityType.NODE ) ).contains(
                IndexEntryUpdate.add( nodeId, nonSchemaIndex, values123 ) );
    }

    @Test
    void shouldGenerateUpdateForSingleChangeNonSchemaIndex()
    {
        // When
        Value newValue2 = Values.of( 10L );
        EntityUpdates updates =
                EntityUpdates.forEntity( nodeId, false ).withTokens( label ).withTokensAfter( label )
                        .changed( property2.propertyKeyId(), property2.value(), newValue2 )
                        .build();

        // Then
        assertThat( updates.forIndexKeys( singleton( nonSchemaIndex ), propertyLoader( property1, property2, property3 ), EntityType.NODE ) ).contains(
                IndexEntryUpdate.change( nodeId, nonSchemaIndex, values123, new Value[]{property1.value(), newValue2, property3.value()} ) );
    }

    @Test
    void shouldGenerateUpdateForAllChangedNonSchemaIndex()
    {
        // When
        Value newValue1 = Values.of( "Nio" );
        Value newValue2 = Values.of( 10L );
        Value newValue3 = Values.pointValue( CoordinateReferenceSystem.WGS84, 32.3, 15.6 );
        EntityUpdates updates =
                EntityUpdates.forEntity( nodeId, false ).withTokens( label ).withTokensAfter( label )
                        .changed( property1.propertyKeyId(), property1.value(), newValue1 )
                        .changed( property2.propertyKeyId(), property2.value(), newValue2 )
                        .changed( property3.propertyKeyId(), property3.value(), newValue3 )
                        .build();

        // Then
        assertThat( updates.forIndexKeys( singleton( nonSchemaIndex ), propertyLoader( property1, property2, property3 ), EntityType.NODE ) ).contains(
                IndexEntryUpdate.change( nodeId, nonSchemaIndex, values123, new Value[]{newValue1, newValue2, newValue3} ) );
    }

    @Test
    void shouldGenerateUpdateWhenRemovingLastPropForNonSchemaIndex()
    {
        // When
        EntityUpdates updates =
                EntityUpdates.forEntity( nodeId, false ).withTokens( label ).withTokensAfter( label )
                        .removed( property2.propertyKeyId(), property2.value() )
                        .build();

        // Then
        assertThat( updates.forIndexKeys( singleton( nonSchemaIndex ), propertyLoader( property2 ), EntityType.NODE ) ).contains(
                IndexEntryUpdate.remove( nodeId, nonSchemaIndex, null, property2.value(), null ) );
    }

    @Test
    void shouldGenerateUpdateWhenRemovingOnePropertyForNonSchemaIndex()
    {
        // When
        EntityUpdates updates =
                EntityUpdates.forEntity( nodeId, false ).withTokens( label ).withTokensAfter( label )
                        .removed( property2.propertyKeyId(), property2.value() )
                        .build();

        // Then
        assertThat( updates.forIndexKeys( singleton( nonSchemaIndex ), propertyLoader( property1, property2, property3 ), EntityType.NODE ) ).contains(
                IndexEntryUpdate.change( nodeId, nonSchemaIndex, values123, new Value[]{property1.value(), null, property3.value()} ) );
    }

    @Test
    void shouldGenerateUpdateWhenAddingOneTokenForNonSchemaIndex()
    {
        // When
        EntityUpdates updates =
                EntityUpdates.forEntity( nodeId, false ).withTokens( empty ).withTokensAfter( label ).build();

        // Then
        assertThat( updates.forIndexKeys( singleton( nonSchemaIndex ), propertyLoader( property1, property2, property3 ), EntityType.NODE ) ).contains(
                IndexEntryUpdate.add( nodeId, nonSchemaIndex, values123 ) );
    }

    @Test
    void shouldGenerateUpdateWhenAddingMultipleTokensForNonSchemaIndex()
    {
        // When
        EntityUpdates updates =
                EntityUpdates.forEntity( nodeId, false ).withTokens( empty ).withTokensAfter( allLabels ).build();

        // Then
        assertThat( updates.forIndexKeys( singleton( nonSchemaIndex ), propertyLoader( property1, property2, property3 ), EntityType.NODE ) ).contains(
                IndexEntryUpdate.add( nodeId, nonSchemaIndex, values123 ) );
    }

    @Test
    void shouldNotGenerateUpdateWhenAddingAnotherTokenForNonSchemaIndex()
    {
        // When
        EntityUpdates updates =
                EntityUpdates.forEntity( nodeId, false ).withTokens( label ).withTokensAfter( allLabels ).build();

        // Then
        assertThat( updates.forIndexKeys( singleton( nonSchemaIndex ), propertyLoader( property1, property2, property3 ), EntityType.NODE ) ).isEmpty();
    }

    @Test
    void shouldNotGenerateUpdateWhenAddingAnotherUselessTokenForNonSchemaIndex()
    {
        // When
        EntityUpdates updates =
                EntityUpdates.forEntity( nodeId, false ).withTokens( label ).withTokensAfter( labelId1, unusedLabelId ).build();

        // Then
        assertThat( updates.forIndexKeys( singleton( nonSchemaIndex ), propertyLoader( property1, property2, property3 ), EntityType.NODE ) ).isEmpty();
    }

    @Test
    void shouldGenerateUpdateWhenSwitchingToUselessTokenForNonSchemaIndex()
    {
        // When
        EntityUpdates updates =
                EntityUpdates.forEntity( nodeId, false ).withTokens( label ).withTokensAfter( unusedLabelId ).build();

        // Then
        assertThat( updates.forIndexKeys( singleton( nonSchemaIndex ), propertyLoader( property1, property2, property3 ), EntityType.NODE ) ).contains(
                IndexEntryUpdate.remove( nodeId, nonSchemaIndex, values123 ) );
    }

    @Test
    void shouldNotGenerateUpdateWhenRemovingOneTokenForNonSchemaIndex()
    {
        // When
        EntityUpdates updates =
                EntityUpdates.forEntity( nodeId, false ).withTokens( allLabels ).withTokensAfter( label ).build();

        // Then
        assertThat( updates.forIndexKeys( singleton( nonSchemaIndex ), propertyLoader( property1, property2, property3 ), EntityType.NODE ) ).isEmpty();
    }

    @Test
    void shouldGenerateUpdateWhenRemovingLastTokenForNonSchemaIndex()
    {
        // When
        EntityUpdates updates =
                EntityUpdates.forEntity( nodeId, false ).withTokens( label ).withTokensAfter( empty ).build();

        // Then
        assertThat( updates.forIndexKeys( singleton( nonSchemaIndex ), propertyLoader( property1, property2, property3 ), EntityType.NODE ) ).contains(
                IndexEntryUpdate.remove( nodeId, nonSchemaIndex, values123 ) );
    }

    private static StorageReader propertyLoader( StorageProperty... properties )
    {
        StubStorageCursors stub = new StubStorageCursors();
        for ( StorageProperty property : properties )
        {
            stub.propertyKeyTokenHolder().addToken( new NamedToken( String.valueOf( property.propertyKeyId() ), property.propertyKeyId() ) );
        }
        Map<String,Value> propertyMap = new HashMap<>();
        for ( StorageProperty p : properties )
        {
            propertyMap.put( String.valueOf( p.propertyKeyId() ), p.value() );
        }
        stub.withNode( nodeId ).properties( propertyMap );
        return stub;
    }

    private static StorageReader assertNoLoading()
    {
        StorageReader reader = mock( StorageReader.class );
        IllegalStateException exception = new IllegalStateException( "Should never attempt to load properties!" );
        when( reader.allocateNodeCursor() ).thenThrow( exception );
        when( reader.allocateRelationshipScanCursor() ).thenThrow( exception );
        when( reader.allocateRelationshipTraversalCursor() ).thenThrow( exception );
        when( reader.allocateRelationshipGroupCursor() ).thenThrow( exception );
        when( reader.allocatePropertyCursor() ).thenThrow( exception );
        return reader;
    }
}
