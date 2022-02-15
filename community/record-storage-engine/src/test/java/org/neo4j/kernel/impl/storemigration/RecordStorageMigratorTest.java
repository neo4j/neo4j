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

import org.neo4j.common.EntityType;
import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.recordstorage.SimpleTokenCreator;
import org.neo4j.internal.schema.IndexConfig;
import org.neo4j.internal.schema.IndexDescriptor;
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
import static org.neo4j.kernel.impl.storemigration.RecordStorageMigrator.persistNodeLabelIndex;
import static org.neo4j.storageengine.api.SchemaRule44.ConstraintRuleType.UNIQUE;
import static org.neo4j.storageengine.api.SchemaRule44.ConstraintRuleType.UNIQUE_EXISTS;
import static org.neo4j.storageengine.api.SchemaRule44.IndexType.BTREE;
import static org.neo4j.storageengine.api.SchemaRule44.IndexType.POINT;
import static org.neo4j.storageengine.api.SchemaRule44.IndexType.RANGE;
import static org.neo4j.storageengine.api.SchemaRule44.IndexType.TEXT;

class RecordStorageMigratorTest
{
    private static final String MISSING_REPLACEMENT_MESSAGE =
            "Migration will remove all BTREE indexes and constraints backed by BTREE indexes. " +
            "To guard from unintentionally removing indexes or constraints, " +
            "all BTREE indexes or constraints backed by BTREE indexes must either have been removed before this migration or " +
            "need to have a valid replacement. " +
            "Indexes can be replaced by RANGE, TEXT or POINT index and constraints can be replaced by constraints backed by RANGE index. " +
            "Please drop your indexes and constraints or create replacements and retry the migration. " +
            "The indexes and constraints without replacement are: ";
    private static final String NAME_ONE = "Index one";
    private static final String NAME_TWO = "Index two";
    private final MutableInt schemaId = new MutableInt();
    private int[] labels;
    private int[] props;
    private TokenHolders tokenHolders;

    @BeforeEach
    private void setup() throws KernelException
    {
        tokenHolders = new TokenHolders(
                new DelegatingTokenHolder( new SimpleTokenCreator(), TokenHolder.TYPE_PROPERTY_KEY ),
                new DelegatingTokenHolder( new SimpleTokenCreator(), TokenHolder.TYPE_LABEL ),
                new DelegatingTokenHolder( new SimpleTokenCreator(), TokenHolder.TYPE_RELATIONSHIP_TYPE )
        );
        var nbrOfTokens = 8;
        labels = new int[nbrOfTokens];
        props = new int[nbrOfTokens];
        for ( int i = 0; i < nbrOfTokens; i++ )
        {
            labels[i] = tokenHolders.labelTokens().getOrCreateId( "Label" + i );
            props[i] = tokenHolders.propertyKeyTokens().getOrCreateId( "prop" + i );
        }
    }

    @Test
    void filterOutBtreeIndexesShouldThrowOnNonReplacedBtreeIndex()
    {
        // Given
        var index = index( BTREE, labels[0], props[0], NAME_ONE );
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
    void filterOutBtreeIndexesShouldThrowOnNonReplacedBtreeBackedConstraint( SchemaRule44.ConstraintRuleType constraintType )
    {
        // Given
        var indexConstraint = constraint( constraintType, BTREE, labels[0], props[0], NAME_ONE );
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
    void filterOutBtreeIndexesShouldThrowOnNonReplacedBtreeBackedConstraintWithConstraintOfDifferentTypeOnSameSchema(
            SchemaRule44.ConstraintRuleType btreeConstraint, SchemaRule44.ConstraintRuleType otherConstraint )
    {
        // Given
        var btree = constraint( btreeConstraint, BTREE, labels[0], props[0], NAME_ONE );
        var rangeNodeKey = constraint( otherConstraint, RANGE, labels[0], props[0], NAME_TWO );
        var reader = mock( SchemaStore44Reader.class );
        var storeCursors = mock( StoreCursors.class );
        var access = mock( SchemaRuleMigrationAccess.class );
        when( reader.loadAllSchemaRules( any( StoreCursors.class ) ) ).thenReturn(
                List.of( btree.index(), btree.constraint(), rangeNodeKey.index(), rangeNodeKey.constraint() ) );

        // When
        var e = assertThrows( IllegalStateException.class, () -> filterOutBtreeIndexes( reader, storeCursors, access, tokenHolders, false ) );

        // Then
        assertThat( e ).hasMessageContaining( MISSING_REPLACEMENT_MESSAGE )
                       .hasMessageContaining( btree.constraint().userDescription( tokenHolders ) );
    }

    @Test
    void filterOutBtreeIndexesShouldIncludeAllSchemaRulesThatLackReplacementInException()
    {
        // Given
        var btreeIndex1 = index( BTREE, labels[0], props[0], "btreeIndex1" );
        var btreeIndex2 = index( BTREE, labels[1], props[2], "btreeIndex2" );
        var btreeConstraintUnique = constraint( UNIQUE, BTREE, labels[2], props[2], "btreeConstraintUnique" );
        var btreeConstraintNodeKey = constraint( UNIQUE_EXISTS, BTREE, labels[3], props[3], "btreeConstraintNodeKey" );

        var reader = mock( SchemaStore44Reader.class );
        var storeCursors = mock( StoreCursors.class );
        var access = mock( SchemaRuleMigrationAccess.class );
        when( reader.loadAllSchemaRules( any( StoreCursors.class ) ) ).thenReturn(
                List.of(
                        btreeIndex1, btreeIndex2,
                        btreeConstraintUnique.index, btreeConstraintUnique.constraint,
                        btreeConstraintNodeKey.index, btreeConstraintNodeKey.constraint
                ) );

        // When
        var e = assertThrows( IllegalStateException.class, () -> filterOutBtreeIndexes( reader, storeCursors, access, tokenHolders, false ) );

        // Then
        assertThat( e ).hasMessageContaining( MISSING_REPLACEMENT_MESSAGE )
                       .hasMessageContaining( btreeIndex1.userDescription( tokenHolders ) )
                       .hasMessageContaining( btreeIndex2.userDescription( tokenHolders ) )
                       .hasMessageContaining( btreeConstraintUnique.constraint.userDescription( tokenHolders ) )
                       .hasMessageContaining( btreeConstraintNodeKey.constraint.userDescription( tokenHolders ) )
                       .hasMessageNotContaining( btreeConstraintUnique.index.userDescription( tokenHolders ) )
                       .hasMessageNotContaining( btreeConstraintNodeKey.index.userDescription( tokenHolders ) );
    }

    @Test
    void filterOutBtreeIndexesShouldNotRemoveAnythingIfSomeRulesLackReplacement()
    {
        // Given
        var btreeIndexReplaced = index( BTREE, labels[0], props[0], "btreeIndexReplaced" );
        var replacingIndex = index( RANGE, labels[0], props[0], "replacingIndex" );

        var btreeConstraintReplaced = constraint( UNIQUE, BTREE, labels[1], props[1], "btreeConstraintReplaced" );
        var replacingConstraint = constraint( UNIQUE, RANGE, labels[1], props[1], "replacingConstraint" );

        var btreeIndexWithoutReplacement = index( BTREE, labels[2], props[2], "btreeIndexWithoutReplacement" );
        var btreeConstraintWithoutReplacement = constraint( UNIQUE, BTREE, labels[3], props[3], "btreeConstraintWithoutReplacement" );

        var reader = mock( SchemaStore44Reader.class );
        var storeCursors = mock( StoreCursors.class );
        var access = mock( SchemaRuleMigrationAccess.class );
        when( reader.loadAllSchemaRules( any( StoreCursors.class ) ) ).thenReturn(
                List.of(
                        btreeIndexReplaced, replacingIndex,
                        btreeConstraintReplaced.index, btreeConstraintReplaced.constraint,
                        replacingConstraint.index, replacingConstraint.constraint,
                        btreeIndexWithoutReplacement,
                        btreeConstraintWithoutReplacement.index, btreeConstraintWithoutReplacement.constraint
                ) );

        // When
        var e = assertThrows( IllegalStateException.class, () -> filterOutBtreeIndexes( reader, storeCursors, access, tokenHolders, false ) );

        // Then
        assertThat( e ).hasMessageContaining( MISSING_REPLACEMENT_MESSAGE )
                       .hasMessageContaining( btreeIndexWithoutReplacement.userDescription( tokenHolders ) )
                       .hasMessageContaining( btreeConstraintWithoutReplacement.constraint.userDescription( tokenHolders ) )
                       .hasMessageNotContaining( btreeIndexReplaced.userDescription( tokenHolders ) )
                       .hasMessageNotContaining( replacingIndex.userDescription( tokenHolders ) )
                       .hasMessageNotContaining( btreeConstraintReplaced.constraint.userDescription( tokenHolders ) )
                       .hasMessageNotContaining( replacingConstraint.constraint.userDescription( tokenHolders ) );
        verify( reader ).loadAllSchemaRules( any( StoreCursors.class ) );
        verifyNoInteractions( access );
    }

    @Test
    void filterOutBtreeIndexesShouldNotBeAffectedByExistsConstraint() throws KernelException
    {
        // Given
        var btreeIndexReplacedByRange = index( BTREE, labels[0], props[0], "btreeIndexReplacedByRange" );
        var replacingRangeIndex = index( RANGE, labels[0], props[0], "replacingRangeIndex" );
        var existsConstraintOnIndexSchema = existsConstraint( labels[0], props[0], "existsConstraintOnIndexSchema" );

        // exists constraint on same schema as replaced constraint
        var btreeConstraintUnique = constraint( UNIQUE, BTREE, labels[1], props[1], "btreeConstraintUnique" );
        var replacingRangeConstraintUnique = constraint( UNIQUE, RANGE, labels[1], props[1], "replacingRangeConstraintUnique" );
        var existsConstraintOnConstraintSchema = existsConstraint( labels[1], props[1], "existsConstraintOnConstraintSchema" );

        var reader = mock( SchemaStore44Reader.class );
        var storeCursors = mock( StoreCursors.class );
        var access = mock( SchemaRuleMigrationAccess.class );
        when( reader.loadAllSchemaRules( any( StoreCursors.class ) ) ).thenReturn(
                List.of(
                        btreeIndexReplacedByRange, replacingRangeIndex, existsConstraintOnIndexSchema,
                        btreeConstraintUnique.index, btreeConstraintUnique.constraint,
                        replacingRangeConstraintUnique.index, replacingRangeConstraintUnique.constraint,
                        existsConstraintOnConstraintSchema
                ) );

        // When
        filterOutBtreeIndexes( reader, storeCursors, access, tokenHolders, false );

        // Then
        verify( reader ).loadAllSchemaRules( any( StoreCursors.class ) );
        verify( access ).deleteSchemaRule( btreeIndexReplacedByRange.id() );
        verify( access ).deleteSchemaRule( btreeConstraintUnique.index.id() );
        verify( access ).deleteSchemaRule( btreeConstraintUnique.constraint.id() );
        verifyNoMoreInteractions( access );
    }

    @ParameterizedTest
    @EnumSource( value = SchemaRule44.IndexType.class, names = {"RANGE", "POINT", "TEXT"} )
    void filterOutBtreeIndexesShouldRemoveBtreeIndexIfReplaced( SchemaRule44.IndexType indexType ) throws KernelException
    {
        // Given
        var btree = index( BTREE, labels[0], props[0], NAME_ONE );
        var range = index( indexType, labels[0], props[0], NAME_TWO );
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
    void filterOutBtreeIndexesShouldRemoveBtreeConstraintsIfReplaced( SchemaRule44.ConstraintRuleType constraintType ) throws KernelException
    {
        // Given
        var btreeUnique = constraint( constraintType, BTREE, labels[0], props[0], NAME_ONE );
        var rangeUnique = constraint( constraintType, RANGE, labels[0], props[0], NAME_TWO );
        var reader = mock( SchemaStore44Reader.class );
        var storeCursors = mock( StoreCursors.class );
        var access = mock( SchemaRuleMigrationAccess.class );
        when( reader.loadAllSchemaRules( any( StoreCursors.class ) ) ).thenReturn(
                List.of( btreeUnique.index(), btreeUnique.constraint(), rangeUnique.index(), rangeUnique.constraint() ) );

        // When
        filterOutBtreeIndexes( reader, storeCursors, access, tokenHolders, false );

        // Then
        verify( reader ).loadAllSchemaRules( any( StoreCursors.class ) );
        verify( access ).deleteSchemaRule( btreeUnique.index().id() );
        verify( access ).deleteSchemaRule( btreeUnique.constraint().id() );
        verifyNoMoreInteractions( access );
    }

    @Test
    void filterOutBtreeIndexesShouldRemoveMultipleIndexesAndConstraints() throws KernelException
    {
        // Given
        var btreeIndexReplacedByRange = index( BTREE, labels[0], props[0], "btreeIndexReplacedByRange" );
        var replacingRangeIndex = index( RANGE, labels[0], props[0], "replacingRangeIndex" );

        var btreeIndexReplacedByText = index( BTREE, labels[1], props[1], "btreeIndexReplacedByText" );
        var replacingTextIndex = index( TEXT, labels[1], props[1], "replacingTextIndex" );

        var btreeIndexReplacedByPoint = index( BTREE, labels[2], props[2], "btreeIndexReplacedByPoint" );
        var replacingPointIndex = index( POINT, labels[2], props[2], "replacingPointIndex" );

        var rangeIndex = index( RANGE, labels[3], props[3], "rangeIndex" );
        var textIndex = index( TEXT, labels[3], props[3], "textIndex" );
        var pointIndex = index( POINT, labels[3], props[3], "pointIndex" );

        var btreeConstraintUnique = constraint( UNIQUE, BTREE, labels[4], props[4], "btreeConstraintUnique" );
        var replacingRangeConstraintUnique = constraint( UNIQUE, RANGE, labels[4], props[4], "replacingRangeConstraintUnique" );

        var btreeConstraintNodeKey = constraint( UNIQUE_EXISTS, BTREE, labels[5], props[5], "btreeConstraintNodeKey" );
        var replacingRangeConstraintNodeKey = constraint( UNIQUE_EXISTS, RANGE, labels[5], props[5], "replacingRangeConstraintNodeKey" );

        var rangeConstraintUnique = constraint( UNIQUE, RANGE, labels[6], props[6], "rangeConstraintUnique" );
        var rangeConstraintNodeKey = constraint( UNIQUE_EXISTS, RANGE, labels[7], props[7], "rangeConstraintNodeKey" );

        var reader = mock( SchemaStore44Reader.class );
        var storeCursors = mock( StoreCursors.class );
        var access = mock( SchemaRuleMigrationAccess.class );
        when( reader.loadAllSchemaRules( any( StoreCursors.class ) ) ).thenReturn(
                List.of(
                        btreeIndexReplacedByRange, replacingRangeIndex,
                        btreeIndexReplacedByText, replacingTextIndex,
                        btreeIndexReplacedByPoint, replacingPointIndex,
                        rangeIndex, textIndex, pointIndex,
                        btreeConstraintUnique.index, btreeConstraintUnique.constraint,
                        replacingRangeConstraintUnique.index, replacingRangeConstraintUnique.constraint,
                        btreeConstraintNodeKey.index, btreeConstraintNodeKey.constraint,
                        replacingRangeConstraintNodeKey.index, replacingRangeConstraintNodeKey.constraint,
                        rangeConstraintUnique.index, rangeConstraintUnique.constraint, rangeConstraintNodeKey.index, rangeConstraintNodeKey.constraint
                ) );

        // When
        filterOutBtreeIndexes( reader, storeCursors, access, tokenHolders, false );

        // Then
        verify( reader ).loadAllSchemaRules( any( StoreCursors.class ) );
        verify( access ).deleteSchemaRule( btreeIndexReplacedByRange.id() );
        verify( access ).deleteSchemaRule( btreeIndexReplacedByText.id() );
        verify( access ).deleteSchemaRule( btreeIndexReplacedByPoint.id() );
        verify( access ).deleteSchemaRule( btreeConstraintUnique.index.id() );
        verify( access ).deleteSchemaRule( btreeConstraintUnique.constraint.id() );
        verify( access ).deleteSchemaRule( btreeConstraintNodeKey.index.id() );
        verify( access ).deleteSchemaRule( btreeConstraintNodeKey.constraint.id() );
        verifyNoMoreInteractions( access );
    }

    @Test
    void filterOutBtreeIndexesShouldIgnoreSystemDb() throws KernelException
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

    @Test
    void shouldPersistNodeLabelIndexWhenLoadingFormerLabelScanStoreWithoutId() throws KernelException
    {
        // Given
        var id = 15L;
        var formerLabelScanStoreWithoutId = SchemaStore44Reader.constructFormerLabelScanStoreSchemaRule();
        var reader = mock( SchemaStore44Reader.class );
        var storeCursors = mock( StoreCursors.class );
        var access = mock( SchemaRuleMigrationAccess.class );
        when( reader.loadAllSchemaRules( any( StoreCursors.class ) ) ).thenReturn(
                List.of( formerLabelScanStoreWithoutId )
        );
        when( access.nextId() ).thenReturn( id );

        // When
        persistNodeLabelIndex( reader, storeCursors, access );

        // Then
        verify( access ).nextId();
        verify( access ).writeSchemaRule( IndexDescriptor.NLI_PROTOTYPE.materialise( id ) );
        verifyNoMoreInteractions( access );
    }

    @Test
    void shouldPersistNodeLabelIndexWhenLoadingFormerLabelScanStoreWithId() throws KernelException
    {
        // Given
        var id = 15L;
        var formerLabelScanStoreWithId = SchemaStore44Reader.constructFormerLabelScanStoreSchemaRule( id );
        var reader = mock( SchemaStore44Reader.class );
        var storeCursors = mock( StoreCursors.class );
        var access = mock( SchemaRuleMigrationAccess.class );
        when( reader.loadAllSchemaRules( any( StoreCursors.class ) ) ).thenReturn(
                List.of( formerLabelScanStoreWithId )
        );

        // When
        persistNodeLabelIndex( reader, storeCursors, access );

        // Then
        verify( access ).writeSchemaRule( IndexDescriptor.NLI_PROTOTYPE.materialise( id ) );
        verifyNoMoreInteractions( access );
    }

    @Test
    void shouldNotPersisNodeLabelIndexWhenLoadingExistingNodeLabelIndex() throws KernelException
    {
        // Given
        var existingNli = new SchemaRule44.Index( 15L, SchemaDescriptors.forAnyEntityTokens( EntityType.NODE ), false, "My nli",
                                                  SchemaRule44.IndexType.LOOKUP, new IndexProviderDescriptor( "token-lookup", "1.0" ),
                                                  IndexConfig.empty(), null );
        var reader = mock( SchemaStore44Reader.class );
        var storeCursors = mock( StoreCursors.class );
        var access = mock( SchemaRuleMigrationAccess.class );
        when( reader.loadAllSchemaRules( any( StoreCursors.class ) ) ).thenReturn( List.of( existingNli ) );

        // When
        persistNodeLabelIndex( reader, storeCursors, access );

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

    private SchemaRule44.Constraint existsConstraint( int label, int property, String name )
    {
        var schema = SchemaDescriptors.forLabel( label, property );
        return new SchemaRule44.Constraint( schemaId.getAndIncrement(), schema, name, SchemaRule44.ConstraintRuleType.EXISTS, null, null );
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
