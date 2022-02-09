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
package org.neo4j.kernel.impl.storemigration;

import org.apache.commons.lang3.mutable.MutableInt;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.stream.Stream;

import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.recordstorage.SimpleTokenCreator;
import org.neo4j.internal.schema.IndexConfig;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.kernel.impl.storemigration.legacy.SchemaStore44Reader;
import org.neo4j.storageengine.api.SchemaRule44;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.storageengine.migration.SchemaRuleMigrationAccess;
import org.neo4j.token.DelegatingTokenHolder;
import org.neo4j.token.TokenHolders;
import org.neo4j.token.api.TokenHolder;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.impl.storemigration.RecordStorageMigrator.filterOutBtreeIndexes;
import static org.neo4j.storageengine.api.SchemaRule44.ConstraintRuleType.UNIQUE;
import static org.neo4j.storageengine.api.SchemaRule44.ConstraintRuleType.UNIQUE_EXISTS;
import static org.neo4j.storageengine.api.SchemaRule44.IndexType.BTREE;
import static org.neo4j.storageengine.api.SchemaRule44.IndexType.RANGE;

class RecordStorageMizgratorTest
{
    private static final String MISSING_REPLACEMENT_MESSAGE =
            "All BTREE indexes and constraints backed by BTREE indexes will be removed during migration. " +
            "To make sure no indexes or constraints are deleted unknowingly, migration is prevented if there are " +
            "any indexes that hasn't been replaced by a RANGE, TEXT or POINT index or any constraints that hasn't " +
            "been replaced by a constraint backed by a RANGE index. Please drop the indexes and constraints associated " +
            "with BTREE or replace them as described above. For more details, please refer to the upgrade guide in documentation. " +
            "The indexes and constraints without replacement are: ";
    private static final String NAME_ONE = "Index one";
    private static final String NAME_TWO = "Index two";
    private final MutableInt schemaId = new MutableInt();
    private int labelOne;
    private int propOne;
    private TokenHolders tokenHolders;

    @BeforeEach
    private void setup() throws KernelException
    {
        tokenHolders = new TokenHolders(
                new DelegatingTokenHolder( new SimpleTokenCreator(), TokenHolder.TYPE_PROPERTY_KEY ),
                new DelegatingTokenHolder( new SimpleTokenCreator(), TokenHolder.TYPE_LABEL ),
                new DelegatingTokenHolder( new SimpleTokenCreator(), TokenHolder.TYPE_RELATIONSHIP_TYPE )
        );
        labelOne = tokenHolders.labelTokens().getOrCreateId( "LabelOne" );
        propOne = tokenHolders.propertyKeyTokens().getOrCreateId( "propOne" );
    }

    @Test
    void shouldThrowOnNonReplacedBtreeIndex()
    {
        // Given
        var index = index( BTREE, labelOne, propOne, NAME_ONE );
        var reader = mock( SchemaStore44Reader.class );
        var storeCursors = mock( StoreCursors.class );
        var access = mock( SchemaRuleMigrationAccess.class );
        when( reader.loadAllSchemaRules( any( StoreCursors.class ) ) ).thenReturn( List.of( index ) );

        // When
        var e = assertThrows( IllegalStateException.class, () -> filterOutBtreeIndexes( reader, storeCursors, access, tokenHolders, false ) );

        // Then
        assertThat( e ).hasMessageContaining( MISSING_REPLACEMENT_MESSAGE )
                       .hasMessageContaining( index.userDescription( tokenHolders ) );
    }

    @ParameterizedTest
    @EnumSource( value = SchemaRule44.ConstraintRuleType.class, names = {"UNIQUE", "UNIQUE_EXISTS"} )
    void shouldThrowOnNonReplacedBtreeBackedConstraint( SchemaRule44.ConstraintRuleType constraintType )
    {
        // Given
        var indexConstraint = constraint( constraintType, BTREE, labelOne, propOne, NAME_ONE );
        var index = indexConstraint.index();
        var constraint = indexConstraint.constraint();
        var reader = mock( SchemaStore44Reader.class );
        var storeCursors = mock( StoreCursors.class );
        var access = mock( SchemaRuleMigrationAccess.class );
        when( reader.loadAllSchemaRules( any( StoreCursors.class ) ) ).thenReturn( List.of( index, constraint ) );

        // When
        var e = assertThrows( IllegalStateException.class, () -> filterOutBtreeIndexes( reader, storeCursors, access, tokenHolders, false ) );

        // Then
        assertThat( e ).hasMessageContaining( MISSING_REPLACEMENT_MESSAGE )
                       .hasMessageContaining( constraint.userDescription( tokenHolders ) );
    }

    @ParameterizedTest
    @MethodSource( value = "nonReplacingConstraintCombinations" )
    void shouldThrowOnNonReplacedBtreeBackedConstraintWithConstraintOfDifferentTypeOnSameSchema( SchemaRule44.ConstraintRuleType btreeConstraint,
                                                                                                 SchemaRule44.ConstraintRuleType otherConstraint )
    {
        // Given
        var btree = constraint( btreeConstraint, BTREE, labelOne, propOne, NAME_ONE );
        var rangeNodeKey = constraint( otherConstraint, RANGE, labelOne, propOne, NAME_TWO );
        var reader = mock( SchemaStore44Reader.class );
        var storeCursors = mock( StoreCursors.class );
        var access = mock( SchemaRuleMigrationAccess.class );
        when( reader.loadAllSchemaRules( any( StoreCursors.class ) ) ).thenReturn( List.of( btree.index(), btree.constraint(), rangeNodeKey.index(), rangeNodeKey.constraint() ) );

        // When
        var e = assertThrows( IllegalStateException.class, () -> filterOutBtreeIndexes( reader, storeCursors, access, tokenHolders, false ) );

        // Then
        assertThat( e ).hasMessageContaining( MISSING_REPLACEMENT_MESSAGE )
                       .hasMessageContaining( btree.constraint().userDescription( tokenHolders ) );
    }

    @ParameterizedTest
    @EnumSource( value = SchemaRule44.IndexType.class, names = {"RANGE", "POINT", "TEXT"} )
    void shouldRemoveBtreeIndexIfReplaced( SchemaRule44.IndexType indexType ) throws KernelException
    {
        // Given
        var btree = index( BTREE, labelOne, propOne, NAME_ONE );
        var range = index( indexType, labelOne, propOne, NAME_TWO );
        var reader = mock( SchemaStore44Reader.class );
        var storeCursors = mock( StoreCursors.class );
        var access = mock( SchemaRuleMigrationAccess.class );
        when( reader.loadAllSchemaRules( any( StoreCursors.class ) ) ).thenReturn( List.of( btree, range ) );

        // When
        filterOutBtreeIndexes( reader, storeCursors, access, tokenHolders, false );

        // Then
        verify( reader ).loadAllSchemaRules( any( StoreCursors.class ) );
        verify( access ).deleteSchemaRule( btree.id() );
        verifyNoMoreInteractions( access );
    }

    @ParameterizedTest
    @EnumSource( value = SchemaRule44.ConstraintRuleType.class, names = {"UNIQUE", "UNIQUE_EXISTS"} )
    void shouldRemoveBtreeConstraintsIfReplaced( SchemaRule44.ConstraintRuleType constraintType ) throws KernelException
    {
        // Given
        var btreeUnique = constraint( constraintType, BTREE, labelOne, propOne, NAME_ONE );
        var rangeUnique = constraint( constraintType, RANGE, labelOne, propOne, NAME_TWO );
        var reader = mock( SchemaStore44Reader.class );
        var storeCursors = mock( StoreCursors.class );
        var access = mock( SchemaRuleMigrationAccess.class );
        when( reader.loadAllSchemaRules( any( StoreCursors.class ) ) ).thenReturn( List.of( btreeUnique.index(), btreeUnique.constraint(), rangeUnique.index(), rangeUnique.constraint() ) );

        // When
        filterOutBtreeIndexes( reader, storeCursors, access, tokenHolders, false );

        // Then
        verify( reader ).loadAllSchemaRules( any( StoreCursors.class ) );
        verify( access ).deleteSchemaRule( btreeUnique.index().id() );
        verify( access ).deleteSchemaRule( btreeUnique.constraint().id() );
        verifyNoMoreInteractions( access );
    }

    @Test
    void shouldIgnoreSystemDb() throws KernelException
    {
        // Given
        var reader = mock( SchemaStore44Reader.class );
        var storeCursors = mock( StoreCursors.class );
        var access = mock( SchemaRuleMigrationAccess.class );

        // When
        filterOutBtreeIndexes( reader, storeCursors, access, tokenHolders, true );

        // Then
        verifyNoInteractions( access );
    }

    private SchemaRule44.Index index( SchemaRule44.IndexType indexType, int label, int property, String name )
    {
        var labelSchemaDescriptor = SchemaDescriptors.forLabel( label, property );
        return new SchemaRule44.Index( schemaId.getAndIncrement(), labelSchemaDescriptor, false, name, indexType, IndexProviderDescriptor.UNDECIDED,
                                       IndexConfig.empty(), null );
    }

    private SchemaRule44.Index uniqueIndex( SchemaRule44.IndexType indexType, int label, int property, String name, long constraintId )
    {
        var labelSchemaDescriptor = SchemaDescriptors.forLabel( label, property );
        return new SchemaRule44.Index( schemaId.getAndIncrement(), labelSchemaDescriptor, true, name, indexType, IndexProviderDescriptor.UNDECIDED,
                                       IndexConfig.empty(), constraintId );
    }

    private ConstraintPair constraint( SchemaRule44.ConstraintRuleType constraintType, SchemaRule44.IndexType indexType, int label, int property, String name )
    {
        assertThat( constraintType.isIndexBacked() ).isTrue();
        var constraintId = schemaId.getAndIncrement();
        var index = uniqueIndex( indexType, label, property, name, constraintId );
        var constraint = new SchemaRule44.Constraint( constraintId, index.schema(), name, constraintType, index.id(), index.indexType() );
        return new ConstraintPair( index, constraint );
    }

    private static Stream<Arguments> nonReplacingConstraintCombinations()
    {
        return Stream.of(
                Arguments.of( UNIQUE, UNIQUE_EXISTS ),
                Arguments.of( UNIQUE_EXISTS, UNIQUE )
        );
    }

    private record ConstraintPair(SchemaRule44.Index index, SchemaRule44.Constraint constraint)
    {
    }
}
