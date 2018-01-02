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
package org.neo4j.kernel.impl.api.state;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphdb.Direction;
import org.neo4j.helpers.Pair;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.kernel.api.constraints.NodePropertyConstraint;
import org.neo4j.kernel.api.constraints.RelationshipPropertyExistenceConstraint;
import org.neo4j.kernel.api.constraints.UniquenessConstraint;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.api.properties.Property;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.kernel.api.txstate.TxStateVisitor;
import org.neo4j.kernel.impl.api.RelationshipVisitor;
import org.neo4j.kernel.impl.api.store.RelationshipIterator;
import org.neo4j.kernel.impl.util.diffsets.ReadableDiffSets;
import org.neo4j.test.RandomizedTestRule;
import org.neo4j.test.RepeatRule;

import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.helpers.Pair.of;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;
import static org.neo4j.kernel.api.properties.Property.booleanProperty;
import static org.neo4j.kernel.api.properties.Property.noNodeProperty;
import static org.neo4j.kernel.api.properties.Property.numberProperty;
import static org.neo4j.kernel.api.properties.Property.stringProperty;

public class TxStateTest
{
    public final RandomizedTestRule random = new RandomizedTestRule();

    @Rule
    public final TestRule repeatWithDifferentRandomization()
    {
        return RuleChain.outerRule( new RepeatRule() ).around( random );
    }

    //region node label update tests

    @Test
    public void shouldGetAddedLabels() throws Exception
    {
        // GIVEN
        state.nodeDoAddLabel( 1, 0 );
        state.nodeDoAddLabel( 1, 1 );
        state.nodeDoAddLabel( 2, 1 );

        // WHEN
        Set<Integer> addedLabels = state.nodeStateLabelDiffSets( 1 ).getAdded();

        // THEN
        assertEquals( asSet( 1, 2 ), addedLabels );
    }

    @Test
    public void shouldGetRemovedLabels() throws Exception
    {
        // GIVEN
        state.nodeDoRemoveLabel( 1, 0 );
        state.nodeDoRemoveLabel( 1, 1 );
        state.nodeDoRemoveLabel( 2, 1 );

        // WHEN
        Set<Integer> removedLabels = state.nodeStateLabelDiffSets( 1 ).getRemoved();

        // THEN
        assertEquals( asSet( 1, 2 ), removedLabels );
    }

    @Test
    public void removeAddedLabelShouldRemoveFromAdded() throws Exception
    {
        // GIVEN
        state.nodeDoAddLabel( 1, 0 );
        state.nodeDoAddLabel( 1, 1 );
        state.nodeDoAddLabel( 2, 1 );

        // WHEN
        state.nodeDoRemoveLabel( 1, 1 );

        // THEN
        assertEquals( asSet( 2 ), state.nodeStateLabelDiffSets( 1 ).getAdded() );
    }

    @Test
    public void addRemovedLabelShouldRemoveFromRemoved() throws Exception
    {
        // GIVEN
        state.nodeDoRemoveLabel( 1, 0 );
        state.nodeDoRemoveLabel( 1, 1 );
        state.nodeDoRemoveLabel( 2, 1 );

        // WHEN
        state.nodeDoAddLabel( 1, 1 );

        // THEN
        assertEquals( asSet( 2 ), state.nodeStateLabelDiffSets( 1 ).getRemoved() );
    }

    @Test
    public void shouldMapFromRemovedLabelToNodes() throws Exception
    {
        // GIVEN
        state.nodeDoRemoveLabel( 1, 0 );
        state.nodeDoRemoveLabel( 2, 0 );
        state.nodeDoRemoveLabel( 1, 1 );
        state.nodeDoRemoveLabel( 3, 1 );
        state.nodeDoRemoveLabel( 2, 2 );

        // WHEN
        Set<Long> nodes = state.nodesWithLabelChanged( 2 ).getRemoved();

        // THEN
        assertEquals( asSet( 0L, 2L ), asSet( nodes ) );
    }

    //endregion

    //region index rule tests

    @Test
    public void shouldAddAndGetByLabel() throws Exception
    {
        // WHEN
        state.indexRuleDoAdd( indexOn_1_1 );
        state.indexRuleDoAdd( indexOn_2_1 );

        // THEN
        assertEquals( asSet( indexOn_1_1 ), state.indexDiffSetsByLabel( labelId1 ).getAdded() );
    }

    @Test
    public void shouldAddAndGetByRuleId() throws Exception
    {
        // GIVEN
        state.indexRuleDoAdd( indexOn_1_1 );

        // THEN
        assertEquals( asSet( indexOn_1_1 ), state.indexChanges().getAdded() );
    }

    // endregion

    //region scan and seek index update tests

    @Test
    public void shouldComputeIndexUpdatesForScanOrSeekOnAnEmptyTxState() throws Exception
    {
        // WHEN
        ReadableDiffSets<Long> diffSets = state.indexUpdatesForScanOrSeek( indexOn_1_1, null );

        // THEN
        assertTrue( diffSets.isEmpty() );
    }

    @Test
    public void shouldComputeIndexUpdatesForScanWhenThereAreNewNodes() throws Exception
    {
        // GIVEN
        addNodesToIndex( indexOn_1_1 ).withDefaultStringProperties( 42L, 43L );
        addNodesToIndex( indexOn_1_2 ).withDefaultStringProperties( 44L );

        // WHEN
        ReadableDiffSets<Long> diffSets = state.indexUpdatesForScanOrSeek( indexOn_1_1, null );

        // THEN
        assertEquals( asSet( 42L, 43L ), diffSets.getAdded() );
    }

    @Test
    public void shouldComputeIndexUpdatesForSeekWhenThereAreNewNodes() throws Exception
    {
        // GIVEN
        addNodesToIndex( indexOn_1_1 ).withDefaultStringProperties( 42L, 43L );
        addNodesToIndex( indexOn_1_2 ).withDefaultStringProperties( 44L );

        // WHEN
        ReadableDiffSets<Long> diffSets = state.indexUpdatesForScanOrSeek( indexOn_1_1, "value43" );

        // THEN
        assertEquals( asSet( 43L ), diffSets.getAdded() );
    }

    //endregion

    //region range seek by number index update tests

    @Test
    public void shouldComputeIndexUpdatesForBetweenRangeSeekByNumberWhenThereAreNoMatchingNodes() throws Exception
    {
        // GIVEN
        addNodesToIndex( indexOn_1_1 ).withNumberProperties( asList( of( 42L, 500 ), of( 43L, 550 ) ) );
        addNodesToIndex( indexOn_1_2 ).withNumberProperties( singletonList( of( 44L, 520 ) ) );

        // WHEN
        ReadableDiffSets<Long> diffSets = state.indexUpdatesForRangeSeekByNumber( indexOn_1_1, 660, false, 800, true );

        // THEN
        assertEquals( emptySet(), diffSets.getAdded() );
    }

    @Test
    public void shouldComputeIndexUpdatesForBetweenRangeSeekByNumberWhenThereAreNewNodesCreatedInSingleBatch()
            throws Exception
    {
        // GIVEN
        addNodesToIndex( indexOn_1_1 ).withNumberProperties( asList( of( 42L, 500 ), of( 43L, 550 ) ) );
        addNodesToIndex( indexOn_1_2 ).withNumberProperties( singletonList( of( 44L, 520 ) ) );

        // WHEN
        ReadableDiffSets<Long> diffSets = state.indexUpdatesForRangeSeekByNumber( indexOn_1_1, 510, true, 600, true );

        // THEN
        assertEquals( asSet( 43L ), diffSets.getAdded() );
    }

    @Test
    public void shouldComputeIndexUpdatesForBetweenRangeSeekByNumberWhenThereAreNewNodesCreatedInTwoBatches()
            throws Exception
    {
        // GIVEN
        addNodesToIndex( indexOn_1_1 ).withNumberProperties( singletonList( of( 42L, 500 ) ) );
        addNodesToIndex( indexOn_1_2 ).withNumberProperties( singletonList( of( 44L, 520 ) ) );
        addNodesToIndex( indexOn_1_1 ).withNumberProperties( singletonList( of( 43L, 550 ) ) );

        // WHEN
        ReadableDiffSets<Long> diffSets = state.indexUpdatesForRangeSeekByNumber( indexOn_1_1, 510, true, 600, true );

        // THEN
        assertEquals( asSet( 43L ), diffSets.getAdded() );
    }

    @Test
    public void shouldComputeIndexUpdatesForBetweenRangeSeekByNumberWithIncludeLowerAndIncludeUpper()
            throws Exception
    {
        // GIVEN
        addNodesToIndex( indexOn_1_1 ).withNumberProperties( asList(
            of( 42L, 500 ), of( 43L, 510 ), of( 44L, 520 ), of ( 45L, 530 ),
            of( 47L, 540 ), of( 48L, 550 ), of( 49L, 560) )
        );
        addNodesToIndex( indexOn_1_2 ).withNumberProperties( singletonList( of( 46L, 520 ) ) );

        // WHEN
        ReadableDiffSets<Long> diffSets = state.indexUpdatesForRangeSeekByNumber( indexOn_1_1, 510, true, 550, true );

        // THEN
        assertEquals( asSet( 43L, 44L, 45L, 47L, 48L ), diffSets.getAdded() );
    }

    @Test
    public void shouldComputeIndexUpdatesForBetweenRangeSeekByNumberWithIncludeLowerAndExcludeUpper() throws Exception
    {
        // GIVEN
        addNodesToIndex( indexOn_1_1 ).withNumberProperties( asList(
            of( 42L, 500 ), of( 43L, 510 ), of( 44L, 520 ), of ( 45L, 530 ),
            of( 47L, 540 ), of( 48L, 550 ), of( 49L, 560) )
        );
        addNodesToIndex( indexOn_1_2 ).withNumberProperties( singletonList( of( 46L, 520 ) ) );

        // WHEN
        ReadableDiffSets<Long> diffSets = state.indexUpdatesForRangeSeekByNumber( indexOn_1_1, 510, true, 550, false );

        // THEN
        assertEquals( asSet( 43L, 44L, 45L, 47L ), diffSets.getAdded() );
    }

    @Test
    public void shouldComputeIndexUpdatesForBetweenRangeSeekByNumberWithExcludeLowerAndIncludeUpper() throws Exception
    {
        // GIVEN
        addNodesToIndex( indexOn_1_1 ).withNumberProperties( asList(
            of( 42L, 500 ), of( 43L, 510 ), of( 44L, 520 ), of ( 45L, 530 ),
            of( 47L, 540 ), of( 48L, 550 ), of( 49L, 560) )
        );
        addNodesToIndex( indexOn_1_2 ).withNumberProperties( singletonList( of( 46L, 520 ) ) );

        // WHEN
        ReadableDiffSets<Long> diffSets = state.indexUpdatesForRangeSeekByNumber( indexOn_1_1, 510, false, 550, true );

        // THEN
        assertEquals( asSet( 44L, 45L, 47L, 48L ), diffSets.getAdded() );
    }

    @Test
    public void shouldComputeIndexUpdatesForBetweenRangeSeekByNumberWithExcludeLowerAndExcludeUpper() throws Exception
    {
        // GIVEN
        addNodesToIndex( indexOn_1_1 ).withNumberProperties( asList(
            of( 42L, 500 ), of( 43L, 510 ), of( 44L, 520 ), of ( 45L, 530 ),
            of( 47L, 540 ), of( 48L, 550 ), of( 49L, 560) )
        );
        addNodesToIndex( indexOn_1_2 ).withNumberProperties( singletonList( of( 46L, 520 ) ) );

        // WHEN
        ReadableDiffSets<Long> diffSets = state.indexUpdatesForRangeSeekByNumber( indexOn_1_1, 510, false, 550, false );

        // THEN
        assertEquals( asSet( 44L, 45L, 47L ), diffSets.getAdded() );
    }

    @Test
    public void shouldComputeIndexUpdatesForBetweenRangeSeekByNumberWithUnboundedLowerExcludeLowerAndIncludeUpper()
            throws Exception
    {
        // GIVEN
        addNodesToIndex( indexOn_1_1 ).withBooleanProperties( asList(
            of( 39L, true ), of( 38L, false )
        ) );
        addNodesToIndex( indexOn_1_1 ).withNumberProperties( asList(
            of( 42L, 500 ), of( 43L, 510 ), of( 44L, 520 ), of ( 45L, 530 ),
            of( 47L, 540 ), of( 48L, 550 ), of( 49L, 560) )
        );
        addNodesToIndex( indexOn_1_2 ).withNumberProperties( singletonList( of( 46L, 520 ) ) );

        // WHEN
        ReadableDiffSets<Long> diffSets = state.indexUpdatesForRangeSeekByNumber( indexOn_1_1, null, false, 550, true );

        // THEN
        assertEquals( asSet( 42L, 43L, 44L, 45L, 47L, 48L ), diffSets.getAdded() );
    }

    @Test
    public void shouldComputeIndexUpdatesForBetweenRangeSeekByNumberWithUnboundedLowerIncludeLowerAndIncludeUpper()
            throws Exception
    {
        // GIVEN
        addNodesToIndex( indexOn_1_1 ).withBooleanProperties( asList(
            of( 39L, true ), of( 38L, false )
        ) );
        addNodesToIndex( indexOn_1_1 ).withNumberProperties( asList(
            of( 42L, 500 ), of( 43L, 510 ), of( 44L, 520 ), of ( 45L, 530 ), of( 47L, 540 ), of( 48L, 550 ), of( 49L, 560) )
        );
        addNodesToIndex( indexOn_1_2 ).withNumberProperties( singletonList( of( 46L, 520 ) ) );

        // WHEN
        ReadableDiffSets<Long> diffSets = state.indexUpdatesForRangeSeekByNumber( indexOn_1_1, null, true, 550, true );

        // THEN
        assertEquals( asSet( 42L, 43L, 44L, 45L, 47L, 48L ), diffSets.getAdded() );
    }

    @Test
    public void shouldComputeIndexUpdatesForBetweenRangeSeekByNumberWithUnboundedLowerExcludeLowerAndExcludeUpper()
            throws Exception
    {
        // GIVEN
        addNodesToIndex( indexOn_1_1 ).withBooleanProperties( asList(
            of( 39L, true ), of( 38L, false )
        ) );
        addNodesToIndex( indexOn_1_1 ).withNumberProperties( asList(
            of( 42L, 500 ), of( 43L, 510 ), of( 44L, 520 ), of ( 45L, 530 ),
            of( 47L, 540 ), of( 48L, 550 ), of( 49L, 560) )
        );
        addNodesToIndex( indexOn_1_2 ).withNumberProperties( singletonList( of( 46L, 520 ) ) );

        // WHEN
        ReadableDiffSets<Long> diffSets =
                state.indexUpdatesForRangeSeekByNumber( indexOn_1_1, null, false, 550, false );

        // THEN
        assertEquals( asSet( 42L, 43L, 44L, 45L, 47L ), diffSets.getAdded() );
    }

    @Test
    public void shouldComputeIndexUpdatesForBetweenRangeSeekByNumberWithUnboundedLowerIncludeLowerAndExcludeUpper()
            throws Exception
    {
        // GIVEN
        addNodesToIndex( indexOn_1_1 ).withBooleanProperties( asList(
                of( 39L, true ), of( 38L, false )
        ) );
        addNodesToIndex( indexOn_1_1 ).withNumberProperties( asList(
            of( 42L, 500 ), of( 43L, 510 ), of( 44L, 520 ), of ( 45L, 530 ),
            of( 47L, 540 ), of( 48L, 550 ), of( 49L, 560) )
        );
        addNodesToIndex( indexOn_1_2 ).withNumberProperties( singletonList( of( 46L, 520 ) ) );

        // WHEN
        ReadableDiffSets<Long> diffSets = state.indexUpdatesForRangeSeekByNumber( indexOn_1_1, null, true, 550, false );

        // THEN
        assertEquals( asSet( 42L, 43L, 44L, 45L, 47L ), diffSets.getAdded() );
    }

    @Test
    public void shouldComputeIndexUpdatesForBetweenRangeSeekByNumberWithUnboundedUpperIncludeLowerAndIncludeUpper()
            throws Exception
    {
        // GIVEN
        addNodesToIndex( indexOn_1_1 ).withBooleanProperties( asList(
            of( 39L, true ), of( 38L, false )
        ) );
        addNodesToIndex( indexOn_1_1 ).withNumberProperties( asList(
            of( 42L, 500 ), of( 43L, 510 ), of( 44L, 520 ), of ( 45L, 530 ),
            of( 47L, 540 ), of( 48L, 550 ), of( 49L, 560) )
        );
        addNodesToIndex( indexOn_1_2 ).withNumberProperties( singletonList( of( 46L, 520 ) ) );

        // WHEN
        ReadableDiffSets<Long> diffSets = state.indexUpdatesForRangeSeekByNumber( indexOn_1_1, 540, true, null, true );

        // THEN
        assertEquals( asSet( 47L, 48L, 49L ), diffSets.getAdded() );
    }

    @Test
    public void shouldComputeIndexUpdatesForBetweenRangeSeekByNumberWithUnboundedUpperIncludeLowerAndExcludeUpper()
            throws Exception
    {
        // GIVEN
        addNodesToIndex( indexOn_1_1 ).withBooleanProperties( asList(
            of( 39L, true ), of( 38L, false )
        ) );
        addNodesToIndex( indexOn_1_1 ).withNumberProperties( asList(
            of( 42L, 500 ), of( 43L, 510 ), of( 44L, 520 ), of ( 45L, 530 ), of( 47L, 540 ), of( 48L, 550 ), of( 49L, 560) )
        );
        addNodesToIndex( indexOn_1_2 ).withNumberProperties( singletonList( of( 46L, 520 ) ) );

        // WHEN
        ReadableDiffSets<Long> diffSets = state.indexUpdatesForRangeSeekByNumber( indexOn_1_1, 540, true, null, false );

        // THEN
        assertEquals( asSet( 47L, 48L, 49L ), diffSets.getAdded() );
    }

    @Test
    public void shouldComputeIndexUpdatesForBetweenRangeSeekByNumberWithUnboundedUpperExcludeLowerAndIncludeUpper()
            throws Exception
    {
        // GIVEN
        addNodesToIndex( indexOn_1_1 ).withBooleanProperties( asList(
            of( 39L, true ), of( 38L, false )
        ) );
        addNodesToIndex( indexOn_1_1 ).withNumberProperties( asList(
            of( 42L, 500 ), of( 43L, 510 ), of( 44L, 520 ), of ( 45L, 530 ),
            of( 47L, 540 ), of( 48L, 550 ), of( 49L, 560) )
        );
        addNodesToIndex( indexOn_1_2 ).withNumberProperties( singletonList( of( 46L, 520 ) ) );

        // WHEN
        ReadableDiffSets<Long> diffSets = state.indexUpdatesForRangeSeekByNumber( indexOn_1_1, 540, false, null, true );

        // THEN
        assertEquals( asSet( 48L, 49L ), diffSets.getAdded() );
    }

    @Test
    public void shouldComputeIndexUpdatesForBetweenRangeSeekByNumberWithUnboundedUpperExcludeLowerAndExcludeUpper()
            throws Exception
    {
        // GIVEN
        addNodesToIndex( indexOn_1_1 ).withBooleanProperties( asList(
            of( 39L, true ), of( 38L, false )
        ) );
        addNodesToIndex( indexOn_1_1 ).withNumberProperties( asList(
            of( 42L, 500 ), of( 43L, 510 ), of( 44L, 520 ), of ( 45L, 530 ),
            of( 47L, 540 ), of( 48L, 550 ), of( 49L, 560) )
        );
        addNodesToIndex( indexOn_1_2 ).withNumberProperties( singletonList( of( 46L, 520 ) ) );

        // WHEN
        ReadableDiffSets<Long> diffSets =
            state.indexUpdatesForRangeSeekByNumber( indexOn_1_1, 540, false, null, false );

        // THEN
        assertEquals( asSet( 48L, 49L ), diffSets.getAdded() );
    }

    @Test
    public void shouldComputeIndexUpdatesForBetweenRangeSeekByNumberWithNoBounds() throws Exception
    {
        // GIVEN
        addNodesToIndex( indexOn_1_1 ).withBooleanProperties( asList(
            of( 39L, true ), of( 38L, false )
        ) );
        addNodesToIndex( indexOn_1_1 ).withNumberProperties( asList(
            of( 42L, 500 ), of( 43L, 510 ), of( 44L, 520 ) )
        );
        addNodesToIndex( indexOn_1_2 ).withNumberProperties( singletonList( of( 46L, 520 ) ) );

        // WHEN
        ReadableDiffSets<Long> diffSets = state.indexUpdatesForRangeSeekByNumber( indexOn_1_1, null, true, null, true );

        // THEN
        assertEquals( asSet( 42L, 43L, 44L ), diffSets.getAdded() );
    }

    //endregion

    //region range seek by string index update tests

    @Test
    public void shouldComputeIndexUpdatesForBetweenRangeSeekByStringWhenThereAreNoMatchingNodes() throws Exception
    {
        // GIVEN
        addNodesToIndex( indexOn_1_1 ).withStringProperties( asList( of( 42L, "Agatha" ), of( 43L, "Barbara" ) ) );
        addNodesToIndex( indexOn_1_2 ).withStringProperties( singletonList( of( 44L, "Andreas" ) ) );

        // WHEN
        ReadableDiffSets<Long> diffSets =
            state.indexUpdatesForRangeSeekByString( indexOn_1_1, "Cindy", false, "William", true );

        // THEN
        assertEquals( emptySet(), diffSets.getAdded() );
    }

    @Test
    public void shouldComputeIndexUpdatesForBetweenRangeSeekByStringWhenThereAreNewNodesCreatedInSingleBatch() throws Exception
    {
        // GIVEN
        addNodesToIndex( indexOn_1_1 ).withStringProperties(asList(of( 42L, "Agatha"), of(43L, "Barbara")) );
        addNodesToIndex( indexOn_1_2 ).withStringProperties(singletonList(of( 44L, "Andreas")) );

        // WHEN
        ReadableDiffSets<Long> diffSets =
            state.indexUpdatesForRangeSeekByString( indexOn_1_1, "Amy", true, "Cathy", true );

        // THEN
        assertEquals( asSet( 43L ), diffSets.getAdded() );
    }

    @Test
    public void shouldComputeIndexUpdatesForBetweenRangeSeekByStringWhenThereAreNewNodesCreatedInTwoBatches()
            throws Exception
    {
        // GIVEN
        addNodesToIndex( indexOn_1_1 ).withStringProperties(singletonList(of( 42L, "Agatha" )));
        addNodesToIndex( indexOn_1_2 ).withStringProperties(singletonList(of( 44L, "Andreas" )));
        addNodesToIndex( indexOn_1_1 ).withStringProperties(singletonList(of( 43L, "Barbara" )));

        // WHEN
        ReadableDiffSets<Long> diffSets =
            state.indexUpdatesForRangeSeekByString( indexOn_1_1, "Amy", true, "Cathy", true );

        // THEN
        assertEquals( asSet( 43L ), diffSets.getAdded() );
    }

    @Test
    public void shouldComputeIndexUpdatesForBetweenRangeSeekByStringWithIncludeLowerAndIncludeUpper() throws Exception
    {
        // GIVEN
        addNodesToIndex( indexOn_1_1 ).withStringProperties(asList(
            of( 42L, "Agatha" ), of( 43L, "Amy" ), of( 44L, "Andreas" ), of( 45L, "Aristotle" ),
            of( 47L, "Arthur" ), of( 48L, "Arwen" ), of( 49L, "Ashley" ) )
        );
        addNodesToIndex( indexOn_1_2 ).withStringProperties( singletonList ( of( 46L, "Andreas" ) ) );

        // WHEN
        ReadableDiffSets<Long> diffSets =
            state.indexUpdatesForRangeSeekByString( indexOn_1_1, "Amy", true, "Arwen", true );

        // THEN
        assertEquals( asSet( 43L, 44L, 45L, 47L, 48L ), diffSets.getAdded() );
    }

    @Test
    public void shouldComputeIndexUpdatesForBetweenRangeSeekByStringWithIncludeLowerAndExcludeUpper() throws Exception
    {
        // GIVEN
        addNodesToIndex( indexOn_1_1 ).withStringProperties(asList(
            of( 42L, "Agatha" ), of( 43L, "Amy" ), of( 44L, "Andreas" ), of( 45L, "Aristotle" ),
            of( 47L, "Arthur" ), of( 48L, "Arwen" ), of( 49L, "Ashley" ) )
        );
        addNodesToIndex( indexOn_1_2 ).withStringProperties( singletonList ( of( 46L, "Andreas" ) ) );

        // WHEN
        ReadableDiffSets<Long> diffSets =
            state.indexUpdatesForRangeSeekByString( indexOn_1_1, "Amy", true, "Arwen", false) ;

        // THEN
        assertEquals( asSet( 43L, 44L, 45L, 47L ), diffSets.getAdded() );
    }

    @Test
    public void shouldComputeIndexUpdatesForBetweenRangeSeekByStringWithExcludeLowerAndIncludeUpper() throws Exception
    {
        // GIVEN
        addNodesToIndex( indexOn_1_1 ).withStringProperties(asList(
            of( 42L, "Agatha" ), of( 43L, "Amy" ), of( 44L, "Andreas" ), of( 45L, "Aristotle" ),
            of( 47L, "Arthur" ), of( 48L, "Arwen" ), of( 49L, "Ashley" ) )
        );
        addNodesToIndex( indexOn_1_2 ).withStringProperties( singletonList ( of( 46L, "Andreas" ) ) );

        // WHEN
        ReadableDiffSets<Long> diffSets =
            state.indexUpdatesForRangeSeekByString( indexOn_1_1, "Amy", false, "Arwen", true );

        // THEN
        assertEquals( asSet( 44L, 45L, 47L, 48L ), diffSets.getAdded() );
    }

    @Test
    public void shouldComputeIndexUpdatesForBetweenRangeSeekByStringWithExcludeLowerAndExcludeUpper() throws Exception
    {
        // GIVEN
        addNodesToIndex( indexOn_1_1 ).withStringProperties(asList(
            of( 42L, "Agatha" ), of( 43L, "Amy" ), of( 44L, "Andreas" ), of( 45L, "Aristotle" ),
            of( 47L, "Arthur" ), of( 48L, "Arwen" ), of( 49L, "Ashley" ) )
        );
        addNodesToIndex( indexOn_1_2 ).withStringProperties( singletonList ( of( 46L, "Andreas" ) ) );

        // WHEN
        ReadableDiffSets<Long> diffSets =
            state.indexUpdatesForRangeSeekByString( indexOn_1_1, "Amy", false, "Arwen", false );

        // THEN
        assertEquals( asSet( 44L, 45L, 47L ), diffSets.getAdded() );
    }

    @Test
    public void shouldComputeIndexUpdatesForBetweenRangeSeekByStringWithUnboundedLowerExcludeLowerAndIncludeUpper()
            throws Exception
    {
        // GIVEN
        addNodesToIndex( indexOn_1_1 ).withBooleanProperties( asList(
                of( 39L, true ), of( 38L, false )
        ) );
        addNodesToIndex( indexOn_1_1 ).withStringProperties(asList(
                of( 42L, "Agatha" ), of( 43L, "Amy" ), of( 44L, "Andreas" ), of( 45L, "Aristotle" ),
                of( 47L, "Arthur" ), of( 48L, "Arwen" ), of( 49L, "Ashley" ) )
        );
        addNodesToIndex( indexOn_1_2 ).withStringProperties( singletonList( of( 46L, "Andreas" ) ) );

        // WHEN
        ReadableDiffSets<Long> diffSets =
            state.indexUpdatesForRangeSeekByString( indexOn_1_1, null, false, "Arwen", true );

        // THEN
        assertEquals( asSet( 42L, 43L, 44L, 45L, 47L, 48L ), diffSets.getAdded() );
    }

    @Test
    public void shouldComputeIndexUpdatesForBetweenRangeSeekByStringWithUnboundedLowerIncludeLowerAndIncludeUpper()
            throws Exception
    {
        // GIVEN
        addNodesToIndex( indexOn_1_1 ).withBooleanProperties( asList(
            of( 39L, true ), of( 38L, false )
        ) );
        addNodesToIndex( indexOn_1_1 ).withStringProperties(asList(
            of( 42L, "Agatha" ), of( 43L, "Amy" ), of( 44L, "Andreas" ), of( 45L, "Aristotle" ),
            of( 47L, "Arthur" ), of( 48L, "Arwen" ), of( 49L, "Ashley" ) )
        );
        addNodesToIndex( indexOn_1_2 ).withStringProperties( singletonList( of( 46L, "Andreas") ) );

        // WHEN
        ReadableDiffSets<Long> diffSets =
            state.indexUpdatesForRangeSeekByString( indexOn_1_1, null, true, "Arwen", true );

        // THEN
        assertEquals( asSet( 42L, 43L, 44L, 45L, 47L, 48L ), diffSets.getAdded() );
    }

    @Test
    public void shouldComputeIndexUpdatesForBetweenRangeSeekByStringWithUnboundedLowerExcludeLowerAndExcludeUpper()
            throws Exception
    {
        // GIVEN
        addNodesToIndex( indexOn_1_1 ).withBooleanProperties( asList(
                of( 39L, true ), of( 38L, false )
        ) );
        addNodesToIndex( indexOn_1_1 ).withStringProperties(asList(
                        of( 42L, "Agatha" ), of( 43L, "Amy" ), of( 44L, "Andreas" ), of( 45L, "Aristotle" ),
                        of( 47L, "Arthur" ), of( 48L, "Arwen" ), of( 49L, "Ashley" ) )
        );
        addNodesToIndex( indexOn_1_2 ).withStringProperties( singletonList( of( 46L, "Andreas" ) ) );

        // WHEN
        ReadableDiffSets<Long> diffSets =
            state.indexUpdatesForRangeSeekByString( indexOn_1_1, null, false, "Arwen", false );

        // THEN
        assertEquals( asSet( 42L, 43L, 44L, 45L, 47L ), diffSets.getAdded() );
    }

    @Test
    public void shouldComputeIndexUpdatesForBetweenRangeSeekByStringWithUnboundedLowerIncludeLowerAndExcludeUpper() throws Exception
    {
        // GIVEN
        addNodesToIndex( indexOn_1_1 ).withBooleanProperties( asList(
                of( 39L, true ), of( 38L, false )
        ) );
        addNodesToIndex( indexOn_1_1 ).withStringProperties(asList(
                        of( 42L, "Agatha" ), of( 43L, "Amy" ), of( 44L, "Andreas" ), of( 45L, "Aristotle" ),
                        of( 47L, "Arthur" ), of( 48L, "Arwen" ), of( 49L, "Ashley" ) )
        );
        addNodesToIndex( indexOn_1_2 ).withStringProperties( singletonList( of( 46L, "Andreas" ) ) );

        // WHEN
        ReadableDiffSets<Long> diffSets =
            state.indexUpdatesForRangeSeekByString( indexOn_1_1, null, true, "Arwen", false );

        // THEN
        assertEquals( asSet( 42L, 43L, 44L, 45L, 47L ), diffSets.getAdded() );
    }

    @Test
    public void shouldComputeIndexUpdatesForBetweenRangeSeekByStringWithUnboundedUpperIncludeLowerAndIncludeUpper()
            throws Exception
    {
        // GIVEN
        addNodesToIndex( indexOn_1_1 ).withBooleanProperties( asList(
            of( 39L, true ), of( 38L, false )
        ) );
        addNodesToIndex( indexOn_1_1 ).withStringProperties(asList(
            of( 42L, "Agatha" ), of( 43L, "Amy" ), of( 44L, "Andreas" ), of( 45L, "Aristotle" ),
            of( 47L, "Arthur" ), of( 48L, "Arwen" ), of( 49L, "Ashley" ) )
        );
        addNodesToIndex( indexOn_1_2 ).withStringProperties( singletonList(of( 46L, "Andreas" ) ) );

        // WHEN
        ReadableDiffSets<Long> diffSets =
            state.indexUpdatesForRangeSeekByString( indexOn_1_1, "Arthur", true, null, true );

        // THEN
        assertEquals( asSet( 47L, 48L, 49L ), diffSets.getAdded() );
    }

    @Test
    public void shouldComputeIndexUpdatesForBetweenRangeSeekByStringWithUnboundedUpperIncludeLowerAndExcludeUpper()
            throws Exception
    {
        // GIVEN
        addNodesToIndex( indexOn_1_1 ).withBooleanProperties( asList(
            of( 39L, true ), of( 38L, false )
        ) );
        addNodesToIndex( indexOn_1_1 ).withStringProperties(asList(
            of( 42L, "Agatha" ), of( 43L, "Amy" ), of( 44L, "Andreas" ), of( 45L, "Aristotle" ),
            of( 47L, "Arthur" ), of( 48L, "Arwen" ), of( 49L, "Ashley" ) )
        );
        addNodesToIndex( indexOn_1_2 ).withStringProperties( singletonList( of( 46L, "Andreas" ) ) );

        // WHEN
        ReadableDiffSets<Long> diffSets =
            state.indexUpdatesForRangeSeekByString(indexOn_1_1, "Arthur", true, null, false);

        // THEN
        assertEquals( asSet( 47L, 48L, 49L ), diffSets.getAdded() );
    }

    @Test
    public void shouldComputeIndexUpdatesForBetweenRangeSeekByStringWithUnboundedUpperExcludeLowerAndIncludeUpper()
            throws Exception
    {
        // GIVEN
        addNodesToIndex( indexOn_1_1 ).withBooleanProperties( asList(
            of( 39L, true ), of( 38L, false )
        ) );
        addNodesToIndex( indexOn_1_1 ).withStringProperties(asList(
            of( 42L, "Agatha" ), of( 43L, "Amy" ), of( 44L, "Andreas" ), of( 45L, "Aristotle" ),
            of( 47L, "Arthur" ), of( 48L, "Arwen" ), of( 49L, "Ashley" ) )
        );
        addNodesToIndex( indexOn_1_2 ).withStringProperties( singletonList( of( 46L, "Andreas" ) ) );

        // WHEN
        ReadableDiffSets<Long> diffSets =
            state.indexUpdatesForRangeSeekByString(indexOn_1_1, "Arthur", false, null, true);

        // THEN
        assertEquals( asSet( 48L, 49L ), diffSets.getAdded() );
    }

    @Test
    public void shouldComputeIndexUpdatesForBetweenRangeSeekByStringWithUnboundedUpperExcludeLowerAndExcludeUpper()
            throws Exception
    {
        // GIVEN
        addNodesToIndex( indexOn_1_1 ).withBooleanProperties( asList(
            of( 39L, true ), of( 38L, false )
        ) );
        addNodesToIndex( indexOn_1_1 ).withStringProperties(asList(
            of( 42L, "Agatha" ), of( 43L, "Amy" ), of( 44L, "Andreas" ), of( 45L, "Aristotle" ),
            of( 47L, "Arthur" ), of( 48L, "Arwen" ), of( 49L, "Ashley" ) )
        );
        addNodesToIndex( indexOn_1_2 ).withStringProperties( singletonList( of ( 46L, "Andreas" ) ) );

        // WHEN
        ReadableDiffSets<Long> diffSets =
                state.indexUpdatesForRangeSeekByString( indexOn_1_1, "Arthur", false, null, false );

        // THEN
        assertEquals( asSet( 48L, 49L ), diffSets.getAdded() );
    }

    @Test
    public void shouldComputeIndexUpdatesForBetweenRangeSeekByStringWithNoBounds() throws Exception
    {
        // GIVEN
        addNodesToIndex( indexOn_1_1 ).withBooleanProperties( asList(
            of( 39L, true ), of( 38L, false )
        ) );
        addNodesToIndex( indexOn_1_1 ).withStringProperties( asList(
            of( 42L, "Agatha" ), of( 43L, "Amy" ), of( 44L, "Andreas" ) )
        );
        addNodesToIndex( indexOn_1_2 ).withStringProperties( singletonList( of( 46L, "Andreas" ) ) );

        // WHEN
        ReadableDiffSets<Long> diffSets = state.indexUpdatesForRangeSeekByString( indexOn_1_1, null, true, null, true );

        // THEN
        assertEquals( asSet( 42L, 43L, 44L ), diffSets.getAdded() );
    }

    //endregion

    //region range seek by prefix index update tests

    @Test
    public void shouldComputeIndexUpdatesForRangeSeekByPrefixWhenThereAreNoMatchingNodes() throws Exception
    {
        // GIVEN
        addNodesToIndex( indexOn_1_1 ).withDefaultStringProperties( 42L, 43L );
        addNodesToIndex( indexOn_1_2 ).withDefaultStringProperties( 44L );

        // WHEN
        ReadableDiffSets<Long> diffSets = state.indexUpdatesForRangeSeekByPrefix( indexOn_1_1, "eulav" );

        // THEN
        assertEquals( emptySet(), diffSets.getAdded() );
    }

    @Test
    public void shouldComputeIndexUpdatesForRangeSeekByPrefixWhenThereAreNewNodesCreatedInOneBatch() throws Exception
    {
        // GIVEN
        addNodesToIndex( indexOn_1_1 ).withDefaultStringProperties( 42L, 43L );
        addNodesToIndex( indexOn_1_2 ).withDefaultStringProperties( 44L );

        // WHEN
        ReadableDiffSets<Long> diffSets = state.indexUpdatesForRangeSeekByPrefix( indexOn_1_1, "value" );

        // THEN
        assertEquals( asSet( 42L, 43L ), diffSets.getAdded() );
    }

    @Test
    public void shouldComputeIndexUpdatesForRangeSeekByPrefixWhenThereArePartiallyMatchingNewNodes1() throws Exception
    {
        // GIVEN
        addNodesToIndex( indexOn_1_1 ).withStringProperties( asList(
            of( 40L, "Aaron" ), of( 41L, "Agatha" ), of( 42L, "Andreas" ), of( 43L, "Andrea" ), of( 44L, "Aristotle" ),
            of( 45L, "Barbara" ), of( 46L, "Barbarella" ),
            of( 47L, "Cinderella" )
        ) );
        addNodesToIndex( indexOn_1_2 ).withDefaultStringProperties( 44L );

        // WHEN
        ReadableDiffSets<Long> diffSets = state.indexUpdatesForRangeSeekByPrefix( indexOn_1_1, "And" );

        // THEN
        assertEquals( asSet( 42L, 43L ), diffSets.getAdded() );
    }

    @Test
    public void shouldComputeIndexUpdatesForRangeSeekByPrefixWhenThereArePartiallyMatchingNewNodes2() throws Exception
    {
        // GIVEN
        addNodesToIndex( indexOn_1_1 ).withStringProperties( asList(
            of( 40L, "Aaron" ), of( 41L, "Agatha" ), of( 42L, "Andreas" ), of( 43L, "Andrea" ), of( 44L, "Aristotle" ),
            of( 45L, "Barbara" ), of( 46L, "Barbarella" ),
            of( 47L, "Cinderella" )
        ) );
        addNodesToIndex( indexOn_1_2 ).withDefaultStringProperties( 44L );

        // WHEN
        ReadableDiffSets<Long> diffSets = state.indexUpdatesForRangeSeekByPrefix( indexOn_1_1, "Bar" );

        // THEN
        assertEquals( asSet( 45L, 46L ), diffSets.getAdded() );
    }

    @Test
    public void shouldComputeIndexUpdatesForRangeSeekByPrefixWhenThereArePartiallyMatchingLeadingNewNodes()
            throws Exception
    {
        // GIVEN
        addNodesToIndex( indexOn_1_1 ).withStringProperties( asList(
            of( 40L, "Aaron" ), of( 41L, "Agatha" ), of( 42L, "Andreas" ), of( 43L, "Andrea" ), of( 44L, "Aristotle" ),
            of( 45L, "Barbara" ), of( 46L, "Barbarella" ),
            of( 47L, "Cinderella" )
        ) );
        addNodesToIndex( indexOn_1_2 ).withDefaultStringProperties( 44L );

        // WHEN
        ReadableDiffSets<Long> diffSets = state.indexUpdatesForRangeSeekByPrefix( indexOn_1_1, "Aa" );

        // THEN
        assertEquals( asSet( 40L ), diffSets.getAdded() );
    }

    @Test
    public void shouldComputeIndexUpdatesForRangeSeekByPrefixWhenThereArePartiallyMatchingTrailingNewNodes()
            throws Exception
    {
        // GIVEN
        addNodesToIndex( indexOn_1_1 ).withStringProperties( asList(
            of( 40L, "Aaron" ), of( 41L, "Agatha" ), of( 42L, "Andreas" ), of( 43L, "Andrea" ), of( 44L, "Aristotle" ),
            of( 45L, "Barbara" ), of( 46L, "Barbarella" ),
            of( 47L, "Cinderella" )
        ) );
        addNodesToIndex( indexOn_1_2 ).withDefaultStringProperties( 44L );

        // WHEN
        ReadableDiffSets<Long> diffSets = state.indexUpdatesForRangeSeekByPrefix( indexOn_1_1, "Ci" );

        // THEN
        assertEquals( asSet( 47L ), diffSets.getAdded() );
    }

    @Test
    public void shouldComputeIndexUpdatesForRangeSeekByPrefixWhenThereAreNewNodesCreatedInTwoBatches() throws Exception
    {
        // GIVEN
        addNodesToIndex( indexOn_1_1 ).withDefaultStringProperties( 42L );
        addNodesToIndex( indexOn_1_2 ).withDefaultStringProperties( 44L );
        addNodesToIndex( indexOn_1_1 ).withDefaultStringProperties( 43L );

        // WHEN
        ReadableDiffSets<Long> diffSets = state.indexUpdatesForRangeSeekByPrefix( indexOn_1_1, "value" );

        // THEN
        assertEquals( asSet( 42L, 43L ), diffSets.getAdded() );
    }

    //endregion

    //region miscellaneous

    @Test
    public void shouldListNodeAsDeletedIfItIsDeleted() throws Exception
    {
        // Given

        // When
        long nodeId = 1337l;
        state.nodeDoDelete( nodeId );

        // Then
        assertThat( asSet( state.addedAndRemovedNodes().getRemoved() ), equalTo( asSet( nodeId ) ) );
    }

    @Test
    public void shouldAddUniquenessConstraint() throws Exception
    {
        // when
        UniquenessConstraint constraint = new UniquenessConstraint( 1, 17 );
        state.constraintDoAdd( constraint, 7 );

        // then
        ReadableDiffSets<NodePropertyConstraint> diff = state.constraintsChangesForLabel( 1 );

        assertEquals( singleton( constraint ), diff.getAdded() );
        assertTrue( diff.getRemoved().isEmpty() );
    }

    @Test
    public void addingUniquenessConstraintShouldBeIdempotent() throws Exception
    {
        // given
        UniquenessConstraint constraint1 = new UniquenessConstraint( 1, 17 );
        state.constraintDoAdd( constraint1, 7 );

        // when
        UniquenessConstraint constraint2 = new UniquenessConstraint( 1, 17 );
        state.constraintDoAdd( constraint2, 19 );

        // then
        assertEquals( constraint1, constraint2 );
        assertEquals( singleton( constraint1 ), state.constraintsChangesForLabel( 1 ).getAdded() );
    }

    @Test
    public void shouldDifferentiateBetweenUniquenessConstraintsForDifferentLabels() throws Exception
    {
        // when
        UniquenessConstraint constraint1 = new UniquenessConstraint( 1, 17 );
        state.constraintDoAdd( constraint1, 7 );
        UniquenessConstraint constraint2 = new UniquenessConstraint( 2, 17 );
        state.constraintDoAdd( constraint2, 19 );

        // then
        assertEquals( singleton( constraint1 ), state.constraintsChangesForLabel( 1 ).getAdded() );
        assertEquals( singleton( constraint2 ), state.constraintsChangesForLabel( 2 ).getAdded() );
    }

    @Test
    public void shouldAddRelationshipPropertyExistenceConstraint()
    {
        // Given
        RelationshipPropertyExistenceConstraint constraint = new RelationshipPropertyExistenceConstraint( 1, 42 );

        // When
        state.constraintDoAdd( constraint );

        // Then
        assertEquals( singleton( constraint ), state.constraintsChangesForRelationshipType( 1 ).getAdded() );
    }

    @Test
    public void addingRelationshipPropertyExistenceConstraintConstraintShouldBeIdempotent()
    {
        // Given
        RelationshipPropertyExistenceConstraint constraint1 = new RelationshipPropertyExistenceConstraint( 1, 42 );
        RelationshipPropertyExistenceConstraint constraint2 = new RelationshipPropertyExistenceConstraint( 1, 42 );

        // When
        state.constraintDoAdd( constraint1 );
        state.constraintDoAdd( constraint2 );

        // Then
        assertEquals( constraint1, constraint2 );
        assertEquals( singleton( constraint1 ), state.constraintsChangesForRelationshipType( 1 ).getAdded() );
    }

    @Test
    public void shouldDropRelationshipPropertyExistenceConstraint()
    {
        // Given
        RelationshipPropertyExistenceConstraint constraint = new RelationshipPropertyExistenceConstraint( 1, 42 );
        state.constraintDoAdd( constraint );

        // When
        state.constraintDoDrop( constraint );

        // Then
        assertTrue( state.constraintsChangesForRelationshipType( 1 ).isEmpty() );
    }

    @Test
    public void shouldDifferentiateRelationshipPropertyExistenceConstraints() throws Exception
    {
        // Given
        RelationshipPropertyExistenceConstraint constraint1 = new RelationshipPropertyExistenceConstraint( 1, 11 );
        RelationshipPropertyExistenceConstraint constraint2 = new RelationshipPropertyExistenceConstraint( 1, 22 );
        RelationshipPropertyExistenceConstraint constraint3 = new RelationshipPropertyExistenceConstraint( 3, 33 );

        // When
        state.constraintDoAdd( constraint1 );
        state.constraintDoAdd( constraint2 );
        state.constraintDoAdd( constraint3 );

        // Then
        assertEquals( asSet( constraint1, constraint2 ), state.constraintsChangesForRelationshipType( 1 ).getAdded() );
        assertEquals( singleton( constraint1 ),
                state.constraintsChangesForRelationshipTypeAndProperty( 1, 11 ).getAdded() );
        assertEquals( singleton( constraint2 ),
                state.constraintsChangesForRelationshipTypeAndProperty( 1, 22 ).getAdded() );
        assertEquals( singleton( constraint3 ), state.constraintsChangesForRelationshipType( 3 ).getAdded() );
        assertEquals( singleton( constraint3 ),
                state.constraintsChangesForRelationshipTypeAndProperty( 3, 33 ).getAdded() );
    }

    @Test
    public void shouldListRelationshipsAsCreatedIfCreated() throws Exception
    {
        // When
        long relId = 10;
        state.relationshipDoCreate( relId, 0, 1, 2 );

        // Then
        assertTrue( state.hasChanges() );
        assertTrue( state.relationshipIsAddedInThisTx( relId ) );
    }

    @Test
    public void shouldGiveCorrectDegreeWhenAddingAndRemovingRelationships() throws Exception
    {
        // Given
        int startNode = 1, endNode = 2, relType = 0;

        // When
        state.relationshipDoCreate( 10, relType, startNode, endNode );
        state.relationshipDoCreate( 11, relType, startNode, endNode );
        state.relationshipDoCreate( 12, relType + 1, startNode, endNode );
        state.relationshipDoCreate( 13, relType + 1, endNode, startNode );

        state.relationshipDoDelete( 1337, relType, startNode, endNode );
        state.relationshipDoDelete( 1338, relType + 1, startNode, startNode );

        // Then
        assertEquals( 12, state.augmentNodeDegree( startNode, 10, Direction.BOTH ) );
        assertEquals( 10, state.augmentNodeDegree( startNode, 10, Direction.INCOMING ) );
        assertEquals( 11, state.augmentNodeDegree( startNode, 10, Direction.BOTH, relType ) );
    }

    @Test
    public void shouldGiveCorrectRelationshipTypesForNode() throws Exception
    {
        // Given
        int startNode = 1, endNode = 2, relType = 0;

        // When
        long relA = 10, relB = 11, relC = 12;
        state.relationshipDoCreate( relA, relType, startNode, endNode );
        state.relationshipDoCreate( relB, relType, startNode, endNode );
        state.relationshipDoCreate( relC, relType + 1, startNode, endNode );

        state.relationshipDoDelete( relB, relType, startNode, endNode );
        state.relationshipDoDelete( relC, relType + 1, startNode, endNode );

        // Then
        assertThat( IteratorUtil.asList( state.nodeRelationshipTypes( startNode ) ),
                    equalTo( asList( relType ) ) );
    }

    @Test
    public void shouldNotChangeRecordForCreatedAndDeletedNode() throws Exception
    {
        // GIVEN
        state.nodeDoCreate( 0 );
        state.nodeDoDelete( 0 );
        state.nodeDoCreate( 1 );

        // WHEN
        state.accept( new TxStateVisitor.Adapter()
        {
            @Override
            public void visitCreatedNode( long id )
            {
                assertEquals( "Should not create any other node than 1", 1, id );
            }

            @Override
            public void visitDeletedNode( long id )
            {
                fail( "Should not delete any node" );
            }
        } );
    }

    @Test
    public void shouldVisitDeletedNode() throws Exception
    {
        // Given
        state.nodeDoDelete( 42 );

        // When
        state.accept( new TxStateVisitor.Adapter()
        {
            @Override
            public void visitDeletedNode( long id )
            {
                // Then
                assertEquals( "Wrong deleted node id", 42, id );
            }
        } );
    }

    @Test
    public void shouldReportDeletedNodeIfItWasCreatedAndDeletedInSameTx()
    {
        // Given
        long nodeId = 42;

        // When
        state.nodeDoCreate( nodeId );
        state.nodeDoDelete( nodeId );

        // Then
        assertTrue( state.nodeIsDeletedInThisTx( nodeId ) );
    }

    @Test
    public void shouldNotReportDeletedNodeIfItIsNotDeleted()
    {
        // Given
        long nodeId = 42;

        // When
        state.nodeDoCreate( nodeId );

        // Then
        assertFalse( state.nodeIsDeletedInThisTx( nodeId ) );
    }

    @Test
    public void shouldNotChangeRecordForCreatedAndDeletedRelationship() throws Exception
    {
        // GIVEN
        state.relationshipDoCreate( 0, 0, 1, 2 );
        state.relationshipDoDelete( 0, 0, 1, 2 );
        state.relationshipDoCreate( 1, 0, 2, 3 );

        // WHEN
        state.accept( new TxStateVisitor.Adapter()
        {
            @Override
            public void visitCreatedRelationship( long id, int type, long startNode, long endNode )
            {
                assertEquals( "Should not create any other relationship than 1", 1, id );
            }

            @Override
            public void visitDeletedRelationship( long id )
            {
                fail( "Should not delete any relationship" );
            }
        } );
    }

    @Test
    public void shouldVisitDeletedRelationship() throws Exception
    {
        // Given
        state.relationshipDoDelete( 42, 2, 3, 4 );

        // When
        state.accept( new TxStateVisitor.Adapter()
        {
            @Override
            public void visitDeletedRelationship( long id )
            {
                // Then
                assertEquals( "Wrong deleted relationship id", 42, id );
            }
        } );
    }

    @Test
    public void shouldReportDeletedRelationshipIfItWasCreatedAndDeletedInSameTx()
    {
        // Given
        long startNodeId = 1;
        long relationshipId = 2;
        int relationshipType = 3;
        long endNodeId = 4;

        // When
        state.relationshipDoCreate( relationshipId, relationshipType, startNodeId, endNodeId );
        state.relationshipDoDelete( relationshipId, relationshipType, startNodeId, endNodeId );

        // Then
        assertTrue( state.relationshipIsDeletedInThisTx( relationshipId ) );
    }

    @Test
    public void shouldNotReportDeletedRelationshipIfItIsNotDeleted()
    {
        // Given
        long startNodeId = 1;
        long relationshipId = 2;
        int relationshipType = 3;
        long endNodeId = 4;

        // When
        state.relationshipDoCreate( relationshipId, relationshipType, startNodeId, endNodeId );

        // Then
        assertFalse( state.relationshipIsDeletedInThisTx( relationshipId ) );
    }

    @Test
    @RepeatRule.Repeat(times = 100)
    public void shouldVisitCreatedNodesBeforeDeletedNodes() throws Exception
    {
        // when
        state.accept( new VisitationOrder( random.nextInt( 100 ) )
        {
            // given

            @Override
            void createEarlyState()
            {
                state.nodeDoCreate( /*id=*/random.nextInt( 1 << 20 ) );
            }

            @Override
            void createLateState()
            {
                state.nodeDoDelete( /*id=*/random.nextInt( 1 << 20 ) );
            }

            // then

            @Override
            public void visitCreatedNode( long id )
            {
                visitEarly();
            }

            @Override
            public void visitDeletedNode( long id )
            {
                visitLate();
            }
        } );
    }

    @Test
    @RepeatRule.Repeat(times = 100)
    public void shouldVisitCreatedNodesBeforeCreatedRelationships() throws Exception
    {
        // when
        state.accept( new VisitationOrder( random.nextInt( 100 ) )
        {
            // given

            @Override
            void createEarlyState()
            {
                state.nodeDoCreate( /*id=*/random.nextInt( 1 << 20 ) );
            }

            @Override
            void createLateState()
            {
                state.relationshipDoCreate( /*id=*/random.nextInt( 1 << 20 ),
                                            /*type=*/random.nextInt( 128 ),
                                            /*startNode=*/random.nextInt( 1 << 20 ),
                                            /*endNode=*/random.nextInt( 1 << 20 ) );
            }

            // then

            @Override
            public void visitCreatedNode( long id )
            {
                visitEarly();
            }

            @Override
            public void visitCreatedRelationship( long id, int type, long startNode, long endNode )
            {
                visitLate();
            }
        } );
    }

    @Test
    @RepeatRule.Repeat(times = 100)
    public void shouldVisitCreatedRelationshipsBeforeDeletedRelationships() throws Exception
    {
        // when
        state.accept( new VisitationOrder( random.nextInt( 100 ) )
        {
            // given

            @Override
            void createEarlyState()
            {
                state.relationshipDoCreate( /*id=*/random.nextInt( 1 << 20 ),
                                            /*type=*/random.nextInt( 128 ),
                                            /*startNode=*/random.nextInt( 1 << 20 ),
                                            /*endNode=*/random.nextInt( 1 << 20 ) );
            }

            @Override
            void createLateState()
            {
                state.relationshipDoDelete( /*id=*/random.nextInt( 1 << 20 ),
                                            /*type=*/random.nextInt( 128 ),
                                            /*startNode=*/random.nextInt( 1 << 20 ),
                                            /*endNode=*/random.nextInt( 1 << 20 ) );
            }

            // then
            @Override
            public void visitCreatedRelationship( long id, int type, long startNode, long endNode )
            {
                visitEarly();
            }

            @Override
            public void visitDeletedRelationship( long id )
            {
                visitLate();
            }
        } );
    }

    @Test
    @RepeatRule.Repeat(times = 100)
    public void shouldVisitDeletedNodesAfterDeletedRelationships() throws Exception
    {
        // when
        state.accept( new VisitationOrder( random.nextInt( 100 ) )
        {
            // given

            @Override
            void createEarlyState()
            {
                state.relationshipDoCreate( /*id=*/random.nextInt( 1 << 20 ),
                                            /*type=*/random.nextInt( 128 ),
                                            /*startNode=*/random.nextInt( 1 << 20 ),
                                            /*endNode=*/random.nextInt( 1 << 20 ) );
            }

            @Override
            void createLateState()
            {
                state.nodeDoDelete( /*id=*/random.nextInt( 1 << 20 ) );
            }

            // then

            @Override
            public void visitDeletedRelationship( long id )
            {
                visitEarly();
            }

            @Override
            public void visitDeletedNode( long id )
            {
                visitLate();
            }
        } );
    }

    //endregion

    abstract class VisitationOrder extends TxStateVisitor.Adapter
    {
        private final Set<String> visitMethods = new HashSet<>();

        VisitationOrder( int size )
        {
            for ( Method method : getClass().getDeclaredMethods() )
            {
                if ( method.getName().startsWith( "visit" ) )
                {
                    visitMethods.add( method.getName() );
                }
            }
            assertEquals( "should implement exactly two visit*(...) methods", 2, visitMethods.size() );
            do
            {
                if ( random.nextBoolean() )
                {
                    createEarlyState();
                }
                else
                {
                    createLateState();
                }
            } while ( size-- > 0 );
        }

        abstract void createEarlyState();

        abstract void createLateState();

        private boolean late;

        final void visitEarly()
        {
            if ( late )
            {
                String early = "the early visit*-method", late = "the late visit*-method";
                for ( StackTraceElement trace : Thread.currentThread().getStackTrace() )
                {
                    if ( visitMethods.contains( trace.getMethodName() ) )
                    {
                        early = trace.getMethodName();
                        for ( String method : visitMethods )
                        {
                            if ( !method.equals( early ) )
                            {
                                late = method;
                            }
                        }
                        break;
                    }
                }
                fail( early + "(...) should not be invoked after " + late + "(...)" );
            }
        }

        final void visitLate()
        {
            late = true;
        }
    }

    public static RelationshipIterator wrapInRelationshipIterator( final PrimitiveLongIterator iterator )
    {
        return new RelationshipIterator.BaseIterator()
        {
            private int cursor;

            @Override
            public <EXCEPTION extends Exception> boolean relationshipVisit( long relationshipId,
                    RelationshipVisitor<EXCEPTION> visitor ) throws EXCEPTION
            {
                throw new UnsupportedOperationException( "Shouldn't be required" );
            }

            @Override
            protected boolean fetchNext()
            {
                return iterator.hasNext() && next( iterator.next() );
            }
        };
    }

    private final int labelId1 = 2;
    private final int labelId2 = 5;

    private final int propertyKeyId1 = 3;
    private final int propertyKeyId2 = 4;

    private final IndexDescriptor indexOn_1_1 = new IndexDescriptor( labelId1, propertyKeyId1 );
    private final IndexDescriptor indexOn_1_2 = new IndexDescriptor( labelId1, propertyKeyId2 );
    private final IndexDescriptor indexOn_2_1 = new IndexDescriptor( labelId2, propertyKeyId1 );

    private TransactionState state;

    @Before
    public void before() throws Exception
    {
        state = new TxState();
    }

    private interface IndexUpdater
    {
        void withDefaultStringProperties( long... nodeIds );
        void withStringProperties( Collection<Pair<Long, String>> nodesWithValues );
        <T extends Number> void withNumberProperties( Collection<Pair<Long, T>> nodesWithValues );
        void withBooleanProperties( Collection<Pair<Long, Boolean>> nodesWithValues );
    }

    private IndexUpdater addNodesToIndex( final IndexDescriptor descriptor )
    {
        return new IndexUpdater()
        {
            public void withDefaultStringProperties( long... nodeIds )
            {
                Collection<Pair<Long, String>> entries = new ArrayList<>( nodeIds.length );
                for ( long nodeId : nodeIds )
                {
                    entries.add( of( nodeId, "value" + nodeId ) );
                }
                withStringProperties( entries );
            }

            public void withStringProperties( Collection<Pair<Long, String>> nodesWithValues )
            {
                final int labelId = descriptor.getLabelId();
                final int propertyKeyId = descriptor.getPropertyKeyId();
                for ( Pair<Long, String> entry : nodesWithValues )
                {
                    long nodeId = entry.first();
                    state.nodeDoCreate( nodeId );
                    state.nodeDoAddLabel( labelId, nodeId );
                    Property propertyBefore = noNodeProperty( nodeId, propertyKeyId );
                    DefinedProperty propertyAfter = stringProperty( propertyKeyId, entry.other() );
                    state.nodeDoReplaceProperty( nodeId, propertyBefore, propertyAfter );
                    state.indexDoUpdateProperty( descriptor, nodeId, null, propertyAfter );
                }
            }

            public <T extends Number> void withNumberProperties( Collection<Pair<Long, T>> nodesWithValues )
            {
                final int labelId = descriptor.getLabelId();
                final int propertyKeyId = descriptor.getPropertyKeyId();
                for ( Pair<Long, T> entry : nodesWithValues )
                {
                    long nodeId = entry.first();
                    state.nodeDoCreate( nodeId );
                    state.nodeDoAddLabel( labelId, nodeId );
                    Property propertyBefore = noNodeProperty( nodeId, propertyKeyId );
                    DefinedProperty propertyAfter = numberProperty( propertyKeyId, entry.other() );
                    state.nodeDoReplaceProperty( nodeId, propertyBefore, propertyAfter );
                    state.indexDoUpdateProperty( descriptor, nodeId, null, propertyAfter );
                }
            }

            public void withBooleanProperties( Collection<Pair<Long, Boolean>> nodesWithValues )
            {
                final int labelId = descriptor.getLabelId();
                final int propertyKeyId = descriptor.getPropertyKeyId();
                for ( Pair<Long, Boolean> entry : nodesWithValues )
                {
                    long nodeId = entry.first();
                    state.nodeDoCreate( nodeId );
                    state.nodeDoAddLabel( labelId, nodeId );
                    Property propertyBefore = noNodeProperty( nodeId, propertyKeyId );
                    DefinedProperty propertyAfter = booleanProperty( propertyKeyId, entry.other() );
                    state.nodeDoReplaceProperty( nodeId, propertyBefore, propertyAfter );
                    state.indexDoUpdateProperty( descriptor, nodeId, null, propertyAfter );
                }
            }
        };
    }
}
