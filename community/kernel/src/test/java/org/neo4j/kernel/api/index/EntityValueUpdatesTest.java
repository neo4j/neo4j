/*
 * Copyright (c) "Neo4j"
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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.common.EntityType;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.storageengine.api.EntityUpdates;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.storageengine.api.PropertyKeyValue;
import org.neo4j.storageengine.api.StorageNodeCursor;
import org.neo4j.storageengine.api.StorageProperty;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.storageengine.api.StorageRelationshipScanCursor;
import org.neo4j.storageengine.api.StubStorageCursors;
import org.neo4j.token.api.NamedToken;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static java.util.Collections.singleton;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.io.pagecache.context.CursorContext.NULL;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

/**
 * NOTE: Test methods are parametrised to be executed for both nodes an relationship where it makes sense.
 * In other words, if a test is executed just for a node, it means the operation does not make semantically sense
 * for a relationship.
 * The Set of operations that can be executed on a node is much larger than relationship equivalent.
 * The biggest difference is that relationships can have exactly one token and adding or removing
 * it equals adding or removing the relationships itself, so some situation tested for nodes cannot
 * happen for relationships. A couple of examples of such operations for illustration:
 * <ul>
 *     <li>Adding a token to an entity with existing properties</li>
 *     <li>Removing a token, but leaving the properties unchanged</li>
 *     <li>Any operation with more than one token</li>
 *     <li>...</li>
 * </ul>
 */
class EntityValueUpdatesTest
{
    private static final long ENTITY_ID = 0;
    private static final int TOKEN_ID_1 = 0;
    private static final int TOKEN_ID_2 = 1;
    private static final int UNUSED_TOKEN_ID = 2;
    private static final int PROPERTY_KEY_ID_1 = 0;
    private static final int PROPERTY_KEY_ID_2 = 1;
    private static final int PROPERTY_KEY_ID_3 = 2;
    private static final long[] TOKEN = new long[]{TOKEN_ID_1};
    private static final long[] ALL_TOKENS = new long[]{TOKEN_ID_1, TOKEN_ID_2};
    private static final long[] EMPTY = new long[]{};

    private static final SchemaDescriptor NODE_INDEX_1 = SchemaDescriptor.forLabel( TOKEN_ID_1, PROPERTY_KEY_ID_1 );
    private static final SchemaDescriptor NODE_INDEX_2 = SchemaDescriptor.forLabel( TOKEN_ID_1, PROPERTY_KEY_ID_2 );
    private static final SchemaDescriptor NODE_INDEX_3 = SchemaDescriptor.forLabel( TOKEN_ID_1, PROPERTY_KEY_ID_3 );
    private static final SchemaDescriptor NODE_INDEX_123 = SchemaDescriptor.forLabel( TOKEN_ID_1, PROPERTY_KEY_ID_1, PROPERTY_KEY_ID_2, PROPERTY_KEY_ID_3 );
    private static final List<SchemaDescriptor> NODE_INDEXES = Arrays.asList( NODE_INDEX_1, NODE_INDEX_2, NODE_INDEX_3, NODE_INDEX_123 );
    private static final SchemaDescriptor NON_SCHEMA_NODE_INDEX = SchemaDescriptor.fulltext( EntityType.NODE, new int[]{TOKEN_ID_1, TOKEN_ID_2},
            new int[]{PROPERTY_KEY_ID_1, PROPERTY_KEY_ID_2, PROPERTY_KEY_ID_3} );

    private static final StorageProperty PROPERTY_1 = new PropertyKeyValue( PROPERTY_KEY_ID_1, Values.of( "Neo" ) );
    private static final StorageProperty PROPERTY_2 = new PropertyKeyValue( PROPERTY_KEY_ID_2, Values.of( 100L ) );
    private static final StorageProperty PROPERTY_3 =
            new PropertyKeyValue( PROPERTY_KEY_ID_3, Values.pointValue( CoordinateReferenceSystem.WGS84, 12.3, 45.6 ) );
    private static final Value[] VALUES_123 = new Value[]{PROPERTY_1.value(), PROPERTY_2.value(), PROPERTY_3.value()};

    @ParameterizedTest
    @EnumSource( Entity.class )
    void shouldNotGenerateUpdatesForEmptyEntityUpdates( Entity entity )
    {
        // When
        EntityUpdates updates = EntityUpdates.forEntity( ENTITY_ID, false ).build();

        // Then
        assertThat( updates.valueUpdatesForIndexKeys( entity.indexes(), assertNoLoading(), entity.type(), NULL, INSTANCE ) ).isEmpty();
    }

    @Test
    void useProvidedCursorForPropertiesOnNodesLoad()
    {
        var cursorContext = mock( CursorContext.class );
        var nodeCursor = mock( StorageNodeCursor.class );
        var storageReader = mock( StorageReader.class, RETURNS_MOCKS );
        when( nodeCursor.hasProperties() ).thenReturn( true );
        when( nodeCursor.next() ).thenReturn( true );
        when( storageReader.allocateNodeCursor( any() ) ).thenReturn( nodeCursor );

        EntityUpdates updates = EntityUpdates.forEntity( ENTITY_ID, false ).withTokens( EMPTY ).withTokensAfter( TOKEN ).build();
        updates.valueUpdatesForIndexKeys( NODE_INDEXES, storageReader, EntityType.NODE, cursorContext, INSTANCE );

        verify( storageReader ).allocateNodeCursor( cursorContext );
        verify( storageReader ).allocatePropertyCursor( cursorContext, INSTANCE );
    }

    @Test
    void useProvidedCursorForPropertiesOnRelationshipLoad()
    {
        var cursorContext = mock( CursorContext.class );
        var relationshipCursor = mock( StorageRelationshipScanCursor.class );
        var storageReader = mock( StorageReader.class, RETURNS_MOCKS );
        when( relationshipCursor.hasProperties() ).thenReturn( true );
        when( relationshipCursor.next() ).thenReturn( true );
        when( storageReader.allocateRelationshipScanCursor( any() ) ).thenReturn( relationshipCursor );

        EntityUpdates updates = EntityUpdates.forEntity( ENTITY_ID, false ).withTokens( EMPTY ).withTokensAfter( TOKEN ).build();
        updates.valueUpdatesForIndexKeys( NODE_INDEXES, storageReader, EntityType.RELATIONSHIP, cursorContext, INSTANCE );

        verify( storageReader ).allocateRelationshipScanCursor( cursorContext );
        verify( storageReader ).allocatePropertyCursor( cursorContext, INSTANCE );
    }

    @ParameterizedTest
    @EnumSource( Entity.class )
    void shouldNotGenerateUpdateForMultipleExistingPropertiesAndTokens( Entity entity )
    {
        // When
        EntityUpdates updates = EntityUpdates.forEntity( ENTITY_ID, false ).withTokens( TOKEN )
                                             .existing( PROPERTY_KEY_ID_1, Values.of( "Neo" ) )
                                             .existing( PROPERTY_KEY_ID_2, Values.of( 100L ) )
                                             .existing( PROPERTY_KEY_ID_3, Values.pointValue( CoordinateReferenceSystem.WGS84, 12.3, 45.6 ) )
                                             .build();

        // Then
        assertThat( updates.valueUpdatesForIndexKeys( entity.indexes(), assertNoLoading(), entity.type(), NULL, INSTANCE ) ).isEmpty();
    }

    @ParameterizedTest
    @EnumSource( Entity.class )
    void shouldNotGenerateUpdatesForTokenAdditionWithNoProperties( Entity entity )
    {
        // When
        EntityUpdates updates = EntityUpdates.forEntity( ENTITY_ID, false ).withTokens( EMPTY ).withTokensAfter( TOKEN ).build();

        // Then
        assertThat( updates.valueUpdatesForIndexKeys( entity.indexes(), propertyLoader(), entity.type(), NULL, INSTANCE ) ).isEmpty();
    }

    @Test
    void shouldGenerateUpdateForLabelAdditionWithExistingProperty()
    {
        // When
        EntityUpdates updates = EntityUpdates.forEntity( ENTITY_ID, false ).withTokens( EMPTY ).withTokensAfter( TOKEN ).build();

        // Then
        assertThat( updates.valueUpdatesForIndexKeys( NODE_INDEXES, propertyLoader( PROPERTY_1 ), EntityType.NODE, NULL, INSTANCE ) ).contains(
                IndexEntryUpdate.add( ENTITY_ID, NODE_INDEX_1, PROPERTY_1.value() ) );
    }

    @Test
    void shouldGenerateUpdatesForLabelAdditionWithExistingProperties()
    {
        // When
        EntityUpdates updates =
                EntityUpdates.forEntity( ENTITY_ID, false ).withTokens( EMPTY ).withTokensAfter( TOKEN )
                             .existing( PROPERTY_KEY_ID_1, Values.of( "Neo" ) )
                             .existing( PROPERTY_KEY_ID_2, Values.of( 100L ) )
                             .existing( PROPERTY_KEY_ID_3, Values.pointValue( CoordinateReferenceSystem.WGS84, 12.3, 45.6 ) )
                             .build();

        // Then
        assertThat( updates.valueUpdatesForIndexKeys( NODE_INDEXES, propertyLoader( PROPERTY_1, PROPERTY_2, PROPERTY_3 ), EntityType.NODE, NULL, INSTANCE ) )
                .contains(
                        IndexEntryUpdate.add( ENTITY_ID, NODE_INDEX_1, PROPERTY_1.value() ),
                        IndexEntryUpdate.add( ENTITY_ID, NODE_INDEX_2, PROPERTY_2.value() ),
                        IndexEntryUpdate.add( ENTITY_ID, NODE_INDEX_3, PROPERTY_3.value() ),
                        IndexEntryUpdate.add( ENTITY_ID, NODE_INDEX_123, VALUES_123 )
                );
    }

    @ParameterizedTest
    @EnumSource( Entity.class )
    void shouldNotGenerateUpdateForPartialCompositeSchemaIndexUpdate( Entity entity )
    {
        // When
        EntityUpdates updates =
                EntityUpdates.forEntity( ENTITY_ID, false ).withTokens( TOKEN ).withTokensAfter( TOKEN )
                             .added( PROPERTY_KEY_ID_1, Values.of( "Neo" ) )
                             .added( PROPERTY_KEY_ID_3, Values.pointValue( CoordinateReferenceSystem.WGS84, 12.3, 45.6 ) )
                             .build();

        // Then
        assertThat( updates.valueUpdatesForIndexKeys( singleton( entity.index123() ), propertyLoader(), entity.type(), NULL, INSTANCE ) ).isEmpty();
    }

    @ParameterizedTest
    @EnumSource( Entity.class )
    void shouldGenerateUpdateWhenCompletingCompositeSchemaIndexUpdate( Entity entity )
    {
        // When
        EntityUpdates updates =
                EntityUpdates.forEntity( ENTITY_ID, false ).withTokens( TOKEN ).withTokensAfter( TOKEN )
                             .added( PROPERTY_KEY_ID_1, Values.of( "Neo" ) )
                             .added( PROPERTY_KEY_ID_3, Values.pointValue( CoordinateReferenceSystem.WGS84, 12.3, 45.6 ) )
                             .build();

        // Then
        assertThat( updates.valueUpdatesForIndexKeys( singleton( entity.index123() ), propertyLoader( PROPERTY_2 ), entity.type(), NULL, INSTANCE ) )
                .contains( IndexEntryUpdate.add( ENTITY_ID, entity.index123(), VALUES_123 ) );
    }

    @ParameterizedTest
    @EnumSource( Entity.class )
    void shouldNotGenerateUpdatesForTokenRemovalWithNoProperties( Entity entity )
    {
        // When
        EntityUpdates updates = EntityUpdates.forEntity( ENTITY_ID, false ).withTokens( TOKEN ).withTokensAfter( EMPTY ).build();

        // Then
        assertThat( updates.valueUpdatesForIndexKeys( entity.indexes(), propertyLoader(), entity.type(), NULL, INSTANCE ) ).isEmpty();
    }

    @Test
    void shouldGenerateUpdateForLabelRemovalWithExistingProperty()
    {
        // When
        EntityUpdates updates =
                EntityUpdates.forEntity( ENTITY_ID, false ).withTokens( TOKEN ).withTokensAfter( EMPTY ).build();

        // Then
        assertThat( updates.valueUpdatesForIndexKeys( NODE_INDEXES, propertyLoader( PROPERTY_1 ), EntityType.NODE, NULL, INSTANCE ) )
                .contains( IndexEntryUpdate.remove( ENTITY_ID, NODE_INDEX_1, PROPERTY_1.value() ) );
    }

    @Test
    void shouldGenerateUpdatesForLabelRemovalWithExistingProperties()
    {
        // When
        EntityUpdates updates =
                EntityUpdates.forEntity( ENTITY_ID, false ).withTokens( TOKEN ).withTokensAfter( EMPTY ).build();

        // Then
        assertThat( updates.valueUpdatesForIndexKeys( NODE_INDEXES, propertyLoader( PROPERTY_1, PROPERTY_2, PROPERTY_3 ), EntityType.NODE, NULL, INSTANCE ) )
                .contains(
                        IndexEntryUpdate.remove( ENTITY_ID, NODE_INDEX_1, PROPERTY_1.value() ),
                        IndexEntryUpdate.remove( ENTITY_ID, NODE_INDEX_2, PROPERTY_2.value() ),
                        IndexEntryUpdate.remove( ENTITY_ID, NODE_INDEX_3, PROPERTY_3.value() ),
                        IndexEntryUpdate.remove( ENTITY_ID, NODE_INDEX_123, VALUES_123 )
                );
    }

    @Test
    void shouldNotGenerateUpdatesForPropertyAdditionWithNoLabels()
    {
        // When
        EntityUpdates updates = EntityUpdates.forEntity( ENTITY_ID, false )
                                             .added( PROPERTY_1.propertyKeyId(), PROPERTY_1.value() )
                                             .build();

        // Then
        assertThat( updates.valueUpdatesForIndexKeys( NODE_INDEXES, assertNoLoading(), EntityType.NODE, NULL, INSTANCE ) ).isEmpty();
    }

    @ParameterizedTest
    @EnumSource( Entity.class )
    void shouldGenerateUpdatesForSinglePropertyAdditionWithToken( Entity entity )
    {
        // When
        EntityUpdates updates = EntityUpdates.forEntity( ENTITY_ID, false ).withTokens( TOKEN )
                                             .added( PROPERTY_1.propertyKeyId(), PROPERTY_1.value() )
                                             .build();

        // Then
        assertThat( updates.valueUpdatesForIndexKeys( entity.indexes(), propertyLoader(), entity.type(), NULL, INSTANCE ) )
                .contains( IndexEntryUpdate.add( ENTITY_ID, entity.index1(), PROPERTY_1.value() ) );
    }

    @ParameterizedTest
    @EnumSource( Entity.class )
    void shouldGenerateUpdatesForMultiplePropertyAdditionWithToken( Entity entity )
    {
        // When
        EntityUpdates updates = EntityUpdates.forEntity( ENTITY_ID, false ).withTokens( TOKEN )
                                             .added( PROPERTY_1.propertyKeyId(), PROPERTY_1.value() )
                                             .added( PROPERTY_2.propertyKeyId(), PROPERTY_2.value() )
                                             .added( PROPERTY_3.propertyKeyId(), PROPERTY_3.value() )
                                             .build();

        // Then
        assertThat( updates.valueUpdatesForIndexKeys( entity.indexes(), propertyLoader( PROPERTY_1, PROPERTY_2, PROPERTY_3 ), entity.type(), NULL,
                INSTANCE ) )
                .contains(
                        IndexEntryUpdate.add( ENTITY_ID, entity.index1(), PROPERTY_1.value() ),
                        IndexEntryUpdate.add( ENTITY_ID, entity.index2(), PROPERTY_2.value() ),
                        IndexEntryUpdate.add( ENTITY_ID, entity.index3(), PROPERTY_3.value() ),
                        IndexEntryUpdate.add( ENTITY_ID, entity.index123(), VALUES_123 )
                );
    }

    @Test
    void shouldNotGenerateUpdatesForLabelAddAndPropertyRemove()
    {
        // When
        EntityUpdates updates = EntityUpdates.forEntity( ENTITY_ID, false ).withTokens( EMPTY ).withTokensAfter( TOKEN )
                                             .removed( PROPERTY_1.propertyKeyId(), PROPERTY_1.value() )
                                             .removed( PROPERTY_2.propertyKeyId(), PROPERTY_2.value() )
                                             .removed( PROPERTY_3.propertyKeyId(), PROPERTY_3.value() )
                                             .build();

        // Then
        assertThat( updates.valueUpdatesForIndexKeys( NODE_INDEXES, assertNoLoading(), EntityType.NODE, NULL, INSTANCE ) ).isEmpty();
    }

    @Test
    void shouldNotGenerateUpdatesForLabelRemoveAndPropertyAdd()
    {
        // When
        EntityUpdates updates = EntityUpdates.forEntity( ENTITY_ID, false ).withTokens( TOKEN ).withTokensAfter( EMPTY )
                                             .added( PROPERTY_1.propertyKeyId(), PROPERTY_1.value() )
                                             .added( PROPERTY_2.propertyKeyId(), PROPERTY_2.value() )
                                             .added( PROPERTY_3.propertyKeyId(), PROPERTY_3.value() )
                                             .build();

        // Then
        assertThat( updates.valueUpdatesForIndexKeys( NODE_INDEXES, assertNoLoading(), EntityType.NODE, NULL, INSTANCE ) ).isEmpty();
    }

    @ParameterizedTest
    @EnumSource( Entity.class )
    void shouldNotLoadPropertyForNoTokenAndNoPropertyChanges( Entity entity )
    {
        // When
        EntityUpdates updates = EntityUpdates.forEntity( ENTITY_ID, false ).withTokens( TOKEN ).build();

        // Then
        assertThat( updates.valueUpdatesForIndexKeys( singleton( entity.index1() ), assertNoLoading(), entity.type(), NULL, INSTANCE ) ).isEmpty();
    }

    @Test
    void shouldNotLoadPropertyForNoLabelsAndButPropertyAddition()
    {
        // When
        EntityUpdates updates = EntityUpdates.forEntity( ENTITY_ID, false ).withTokens( EMPTY )
                                             .added( PROPERTY_1.propertyKeyId(), PROPERTY_1.value() )
                                             .build();

        // Then
        assertThat( updates.valueUpdatesForIndexKeys( singleton( NODE_INDEX_1 ), assertNoLoading(), EntityType.NODE, NULL, INSTANCE ) ).isEmpty();
    }

    @ParameterizedTest
    @EnumSource( Entity.class )
    void shouldGenerateUpdateForPartialNonSchemaIndexUpdate( Entity entity )
    {
        // When
        EntityUpdates updates = EntityUpdates.forEntity( ENTITY_ID, false ).withTokens( TOKEN ).withTokensAfter( TOKEN )
                                             .added( PROPERTY_KEY_ID_1, Values.of( "Neo" ) )
                                             .build();

        // Then
        assertThat( updates.valueUpdatesForIndexKeys( singleton( entity.nonSchemaIndex() ), propertyLoader(), entity.type(), NULL, INSTANCE ) )
                .contains( IndexEntryUpdate.add( ENTITY_ID, entity.nonSchemaIndex(), PROPERTY_1.value(), null, null ) );
    }

    @ParameterizedTest
    @EnumSource( Entity.class )
    void shouldGenerateUpdateForFullNonSchemaIndexUpdate( Entity entity )
    {
        // When
        EntityUpdates updates =
                EntityUpdates.forEntity( ENTITY_ID, false ).withTokens( TOKEN ).withTokensAfter( TOKEN )
                             .added( PROPERTY_1.propertyKeyId(), PROPERTY_1.value() )
                             .added( PROPERTY_2.propertyKeyId(), PROPERTY_2.value() )
                             .added( PROPERTY_3.propertyKeyId(), PROPERTY_3.value() )
                             .build();

        // Then
        assertThat( updates.valueUpdatesForIndexKeys( singleton( entity.nonSchemaIndex() ), propertyLoader(), entity.type(), NULL, INSTANCE ) ).contains(
                IndexEntryUpdate.add( ENTITY_ID, entity.nonSchemaIndex(), VALUES_123 ) );
    }

    @ParameterizedTest
    @EnumSource( Entity.class )
    void shouldGenerateUpdateForSingleChangeNonSchemaIndex( Entity entity )
    {
        // When
        Value newValue2 = Values.of( 10L );
        EntityUpdates updates =
                EntityUpdates.forEntity( ENTITY_ID, false ).withTokens( TOKEN ).withTokensAfter( TOKEN )
                             .changed( PROPERTY_2.propertyKeyId(), PROPERTY_2.value(), newValue2 )
                             .build();

        // Then
        assertThat( updates.valueUpdatesForIndexKeys( singleton( entity.nonSchemaIndex() ),
                propertyLoader( PROPERTY_1, PROPERTY_2, PROPERTY_3 ), entity.type(), NULL, INSTANCE ) )
                .contains( IndexEntryUpdate.change( ENTITY_ID, entity.nonSchemaIndex(), VALUES_123,
                        new Value[]{PROPERTY_1.value(), newValue2, PROPERTY_3.value()} ) );
    }

    @ParameterizedTest
    @EnumSource( Entity.class )
    void shouldGenerateUpdateForAllChangedNonSchemaIndex( Entity entity )
    {
        // When
        Value newValue1 = Values.of( "Nio" );
        Value newValue2 = Values.of( 10L );
        Value newValue3 = Values.pointValue( CoordinateReferenceSystem.WGS84, 32.3, 15.6 );
        EntityUpdates updates =
                EntityUpdates.forEntity( ENTITY_ID, false ).withTokens( TOKEN ).withTokensAfter( TOKEN )
                             .changed( PROPERTY_1.propertyKeyId(), PROPERTY_1.value(), newValue1 )
                             .changed( PROPERTY_2.propertyKeyId(), PROPERTY_2.value(), newValue2 )
                             .changed( PROPERTY_3.propertyKeyId(), PROPERTY_3.value(), newValue3 )
                             .build();

        // Then
        assertThat( updates.valueUpdatesForIndexKeys( singleton( entity.nonSchemaIndex() ), propertyLoader( PROPERTY_1, PROPERTY_2, PROPERTY_3 ),
                entity.type(), NULL, INSTANCE ) )
                .contains( IndexEntryUpdate.change( ENTITY_ID, entity.nonSchemaIndex(), VALUES_123, new Value[]{newValue1, newValue2, newValue3} ) );
    }

    @ParameterizedTest
    @EnumSource( Entity.class )
    void shouldGenerateUpdateWhenRemovingLastPropForNonSchemaIndex( Entity entity )
    {
        // When
        EntityUpdates updates =
                EntityUpdates.forEntity( ENTITY_ID, false ).withTokens( TOKEN ).withTokensAfter( TOKEN )
                             .removed( PROPERTY_2.propertyKeyId(), PROPERTY_2.value() )
                             .build();

        // Then
        assertThat( updates.valueUpdatesForIndexKeys( singleton( entity.nonSchemaIndex() ), propertyLoader( PROPERTY_2 ), entity.type(), NULL, INSTANCE ) )
                .contains( IndexEntryUpdate.remove( ENTITY_ID, entity.nonSchemaIndex(), null, PROPERTY_2.value(), null ) );
    }

    @ParameterizedTest
    @EnumSource( Entity.class )
    void shouldGenerateUpdateWhenRemovingOnePropertyForNonSchemaIndex( Entity entity )
    {
        // When
        EntityUpdates updates =
                EntityUpdates.forEntity( ENTITY_ID, false ).withTokens( TOKEN ).withTokensAfter( TOKEN )
                             .removed( PROPERTY_2.propertyKeyId(), PROPERTY_2.value() )
                             .build();

        // Then
        assertThat( updates.valueUpdatesForIndexKeys( singleton( entity.nonSchemaIndex() ), propertyLoader( PROPERTY_1, PROPERTY_2, PROPERTY_3 ), entity.type(),
                NULL, INSTANCE ) )
                .contains(
                        IndexEntryUpdate.change( ENTITY_ID, entity.nonSchemaIndex(), VALUES_123, new Value[]{PROPERTY_1.value(), null, PROPERTY_3.value()} ) );
    }

    @Test
    void shouldGenerateUpdateWhenAddingOneTokenForNonSchemaIndex()
    {
        // When
        EntityUpdates updates =
                EntityUpdates.forEntity( ENTITY_ID, false ).withTokens( EMPTY ).withTokensAfter( TOKEN ).build();

        // Then
        assertThat( updates.valueUpdatesForIndexKeys( singleton( NON_SCHEMA_NODE_INDEX ), propertyLoader( PROPERTY_1, PROPERTY_2, PROPERTY_3 ), EntityType.NODE,
                NULL, INSTANCE ) )
                .contains( IndexEntryUpdate.add( ENTITY_ID, NON_SCHEMA_NODE_INDEX, VALUES_123 ) );
    }

    @Test
    void shouldGenerateUpdateWhenAddingMultipleTokensForNonSchemaIndex()
    {
        // When
        EntityUpdates updates =
                EntityUpdates.forEntity( ENTITY_ID, false ).withTokens( EMPTY ).withTokensAfter( ALL_TOKENS ).build();

        // Then
        assertThat( updates.valueUpdatesForIndexKeys( singleton( NON_SCHEMA_NODE_INDEX ), propertyLoader( PROPERTY_1, PROPERTY_2, PROPERTY_3 ), EntityType.NODE,
                NULL, INSTANCE ) )
                .contains( IndexEntryUpdate.add( ENTITY_ID, NON_SCHEMA_NODE_INDEX, VALUES_123 ) );
    }

    @Test
    void shouldNotGenerateUpdateWhenAddingAnotherTokenForNonSchemaIndex()
    {
        // When
        EntityUpdates updates =
                EntityUpdates.forEntity( ENTITY_ID, false ).withTokens( TOKEN ).withTokensAfter( ALL_TOKENS ).build();

        // Then
        assertThat( updates.valueUpdatesForIndexKeys( singleton( NON_SCHEMA_NODE_INDEX ), propertyLoader( PROPERTY_1, PROPERTY_2, PROPERTY_3 ), EntityType.NODE,
                NULL, INSTANCE ) )
                .isEmpty();
    }

    @Test
    void shouldNotGenerateUpdateWhenAddingAnotherUselessTokenForNonSchemaIndex()
    {
        // When
        EntityUpdates updates =
                EntityUpdates.forEntity( ENTITY_ID, false ).withTokens( TOKEN ).withTokensAfter( TOKEN_ID_1, UNUSED_TOKEN_ID ).build();

        // Then
        assertThat( updates.valueUpdatesForIndexKeys( singleton( NON_SCHEMA_NODE_INDEX ), propertyLoader( PROPERTY_1, PROPERTY_2, PROPERTY_3 ), EntityType.NODE,
                NULL, INSTANCE ) )
                .isEmpty();
    }

    @Test
    void shouldGenerateUpdateWhenSwitchingToUselessTokenForNonSchemaIndex()
    {
        // When
        EntityUpdates updates =
                EntityUpdates.forEntity( ENTITY_ID, false ).withTokens( TOKEN ).withTokensAfter( UNUSED_TOKEN_ID ).build();

        // Then
        assertThat( updates.valueUpdatesForIndexKeys( singleton( NON_SCHEMA_NODE_INDEX ), propertyLoader( PROPERTY_1, PROPERTY_2, PROPERTY_3 ), EntityType.NODE,
                NULL, INSTANCE ) )
                .contains( IndexEntryUpdate.remove( ENTITY_ID, NON_SCHEMA_NODE_INDEX, VALUES_123 ) );
    }

    @Test
    void shouldNotGenerateUpdateWhenRemovingOneTokenForNonSchemaIndex()
    {
        // When
        EntityUpdates updates =
                EntityUpdates.forEntity( ENTITY_ID, false ).withTokens( ALL_TOKENS ).withTokensAfter( TOKEN ).build();

        // Then
        assertThat( updates.valueUpdatesForIndexKeys( singleton( NON_SCHEMA_NODE_INDEX ), propertyLoader( PROPERTY_1, PROPERTY_2, PROPERTY_3 ), EntityType.NODE,
                NULL, INSTANCE ) )
                .isEmpty();
    }

    @Test
    void shouldGenerateUpdateWhenRemovingLastTokenForNonSchemaIndex()
    {
        // When
        EntityUpdates updates =
                EntityUpdates.forEntity( ENTITY_ID, false ).withTokens( TOKEN ).withTokensAfter( EMPTY ).build();

        // Then
        assertThat( updates.valueUpdatesForIndexKeys( singleton( NON_SCHEMA_NODE_INDEX ), propertyLoader( PROPERTY_1, PROPERTY_2, PROPERTY_3 ), EntityType.NODE,
                NULL, INSTANCE ) )
                .contains( IndexEntryUpdate.remove( ENTITY_ID, NON_SCHEMA_NODE_INDEX, VALUES_123 ) );
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
        stub.withNode( ENTITY_ID ).properties( propertyMap );
        stub.withRelationship( ENTITY_ID, 1, 1, 2 ).properties( propertyMap );
        return stub;
    }

    private static StorageReader assertNoLoading()
    {
        StorageReader reader = mock( StorageReader.class );
        IllegalStateException exception = new IllegalStateException( "Should never attempt to load properties!" );
        when( reader.allocateNodeCursor( any() ) ).thenThrow( exception );
        when( reader.allocateRelationshipScanCursor( any() ) ).thenThrow( exception );
        when( reader.allocateRelationshipTraversalCursor( any() ) ).thenThrow( exception );
        when( reader.allocatePropertyCursor( any(), any() ) ).thenThrow( exception );
        return reader;
    }

    private enum Entity
    {

        NODE
                {
                    @Override
                    List<SchemaDescriptor> indexes()
                    {
                        return NODE_INDEXES;
                    }

                    @Override
                    EntityType type()
                    {
                        return EntityType.NODE;
                    }

                    @Override
                    SchemaDescriptor index1()
                    {
                        return NODE_INDEX_1;
                    }

                    @Override
                    SchemaDescriptor index2()
                    {
                        return NODE_INDEX_2;
                    }

                    @Override
                    SchemaDescriptor index3()
                    {
                        return NODE_INDEX_3;
                    }

                    @Override
                    SchemaDescriptor index123()
                    {
                        return NODE_INDEX_123;
                    }

                    @Override
                    SchemaDescriptor nonSchemaIndex()
                    {
                        return NON_SCHEMA_NODE_INDEX;
                    }
                },
        RELATIONSHIP
                {

                    private final SchemaDescriptor index1 = SchemaDescriptor.forRelType( TOKEN_ID_1, PROPERTY_KEY_ID_1 );
                    private final SchemaDescriptor index2 = SchemaDescriptor.forRelType( TOKEN_ID_1, PROPERTY_KEY_ID_2 );
                    private final SchemaDescriptor index3 = SchemaDescriptor.forRelType( TOKEN_ID_1, PROPERTY_KEY_ID_3 );
                    private final SchemaDescriptor index123 =
                            SchemaDescriptor.forLabel( TOKEN_ID_1, PROPERTY_KEY_ID_1, PROPERTY_KEY_ID_2, PROPERTY_KEY_ID_3 );
                    private final List<SchemaDescriptor> indexes = Arrays.asList( index1, index2, index3, index123 );
                    private final SchemaDescriptor nonSchemaIndex = SchemaDescriptor.fulltext( EntityType.RELATIONSHIP, new int[]{TOKEN_ID_1, TOKEN_ID_2},
                            new int[]{PROPERTY_KEY_ID_1, PROPERTY_KEY_ID_2, PROPERTY_KEY_ID_3} );

                    @Override
                    List<SchemaDescriptor> indexes()
                    {
                        return indexes;
                    }

                    @Override
                    EntityType type()
                    {
                        return EntityType.RELATIONSHIP;
                    }

                    @Override
                    SchemaDescriptor index1()
                    {
                        return index1;
                    }

                    @Override
                    SchemaDescriptor index2()
                    {
                        return index2;
                    }

                    @Override
                    SchemaDescriptor index3()
                    {
                        return index3;
                    }

                    @Override
                    SchemaDescriptor index123()
                    {
                        return index123;
                    }

                    @Override
                    SchemaDescriptor nonSchemaIndex()
                    {
                        return nonSchemaIndex;
                    }
                };

        abstract List<SchemaDescriptor> indexes();

        abstract EntityType type();

        abstract SchemaDescriptor index1();

        abstract SchemaDescriptor index2();

        abstract SchemaDescriptor index3();

        abstract SchemaDescriptor index123();

        abstract SchemaDescriptor nonSchemaIndex();
    }
}
