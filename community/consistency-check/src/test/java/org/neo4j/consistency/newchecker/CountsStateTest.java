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
package org.neo4j.consistency.newchecker;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.function.Consumer;

import org.neo4j.consistency.checking.cache.CacheAccess;
import org.neo4j.consistency.checking.cache.DefaultCacheAccess;
import org.neo4j.consistency.report.ConsistencyReport;
import org.neo4j.consistency.report.ConsistencyReporter;
import org.neo4j.consistency.statistics.Counts;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.internal.recordstorage.RelationshipCounter;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.test.Race;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.consistency.checking.ByteArrayBitsManipulator.MAX_SLOT_BITS;
import static org.neo4j.consistency.checking.cache.CacheSlots.NodeLink.SLOT_LABELS;
import static org.neo4j.consistency.newchecker.RecordStorageConsistencyChecker.DEFAULT_SLOT_SIZES;
import static org.neo4j.internal.batchimport.cache.NumberArrayFactory.HEAP;
import static org.neo4j.kernel.impl.store.record.Record.NULL_REFERENCE;
import static org.neo4j.token.api.TokenConstants.ANY_LABEL;
import static org.neo4j.token.api.TokenConstants.ANY_RELATIONSHIP_TYPE;

class CountsStateTest
{
    private static final int HIGH_NODE_ID = 100;
    private static final int HIGH_TOKEN_ID = 10;
    private static final int NUMBER_OF_RACE_THREADS = 10;
    private static final int NUMBER_OF_RACE_ITERATIONS = 100;
    private static final int TOTAL_COUNT = NUMBER_OF_RACE_THREADS * NUMBER_OF_RACE_ITERATIONS;

    private CountsState countsState;
    private Race race;
    private ConsistencyReporter noConsistencyReporter;
    private ConsistencyReporter inconsistencyReporter;
    private CacheAccess cacheAccess;

    @BeforeEach
    void setUp()
    {
        cacheAccess = new DefaultCacheAccess( HEAP.newByteArray( HIGH_NODE_ID, new byte[MAX_SLOT_BITS] ), Counts.NONE, 1 );
        cacheAccess.setCacheSlotSizes( DEFAULT_SLOT_SIZES );
        countsState = new CountsState( HIGH_TOKEN_ID, HIGH_TOKEN_ID, HIGH_NODE_ID, cacheAccess );
        noConsistencyReporter = mock( ConsistencyReporter.class );
        when( noConsistencyReporter.forCounts( any() ) ).thenReturn( mock( ConsistencyReport.CountsConsistencyReport.class ) );
        inconsistencyReporter = mock( ConsistencyReporter.class );
        when( inconsistencyReporter.forCounts( any() ) ).thenReturn( mock( ConsistencyReport.CountsConsistencyReport.class ) );
        race = new Race().withEndCondition( () -> false );
    }

    @AfterEach
    void tearDown()
    {
        verifyZeroInteractions( noConsistencyReporter );
        countsState.close();
    }

    @Test
    void shouldAddNumberOfUsedNodes()
    {
        // when
        race.addContestants( NUMBER_OF_RACE_THREADS, () -> countsState.incrementNodeLabel( ANY_LABEL, 5 ), NUMBER_OF_RACE_ITERATIONS );
        race.goUnchecked();

        // then
        try ( CountsState.CountsChecker checker = countsState.checker( noConsistencyReporter ) )
        {
            checker.visitNodeCount( ANY_LABEL, TOTAL_COUNT * 5 );
        }
    }

    @Test
    void shouldIncrementNodeCount()
    {
        // when
        race.addContestants( NUMBER_OF_RACE_THREADS, () -> countsState.incrementNodeLabel( 7, 1 ), NUMBER_OF_RACE_ITERATIONS );
        race.goUnchecked();

        // then
        try ( CountsState.CountsChecker checker = countsState.checker( noConsistencyReporter ) )
        {
            checker.visitNodeCount( 7, TOTAL_COUNT );
        }
    }

    @Test
    void shouldIncrementNodeCountAboveHighLabelId()
    {
        // when
        race.addContestants( NUMBER_OF_RACE_THREADS, () -> countsState.incrementNodeLabel( 70, 1 ), NUMBER_OF_RACE_ITERATIONS );
        race.goUnchecked();

        // then
        try ( CountsState.CountsChecker checker = countsState.checker( noConsistencyReporter ) )
        {
            checker.visitNodeCount( 70, TOTAL_COUNT );
        }
    }

    @Test
    void shouldIncrementNodeCountForNegativeLabelId()
    {
        // when
        race.addContestants( NUMBER_OF_RACE_THREADS, () -> countsState.incrementNodeLabel( -10, 1 ), NUMBER_OF_RACE_ITERATIONS );
        race.goUnchecked();

        // then
        try ( CountsState.CountsChecker checker = countsState.checker( noConsistencyReporter ) )
        {
            checker.visitNodeCount( -10, TOTAL_COUNT );
        }
    }

    @Test
    void shouldReportNodeCountMismatches()
    {
        // when
        countsState.incrementNodeLabel( 7, 1 );
        countsState.incrementNodeLabel( 70, 1 );
        countsState.incrementNodeLabel( 6, 1 );
        countsState.incrementNodeLabel( 60, 1 );

        // then
        try ( CountsState.CountsChecker checker = countsState.checker( inconsistencyReporter ) )
        {
            // visiting node count for unseen label ids
            checker.visitNodeCount( 5, 1 );
            checker.visitNodeCount( 50, 1 );

            // visiting node count with wrong counts
            checker.visitNodeCount( 6, 2 );
            checker.visitNodeCount( 60, 2 );

            // not visiting label ids 7 and 70
        }
        verify( inconsistencyReporter, times( 6 ) ).forCounts( any() );
    }

    @Test
    void shouldIncrementRelationshipCount()
    {
        // given some labels on our nodes
        putLabelsOnNodes( nodeLabels( 10, 1 ), nodeLabels( 20, 3 ) );

        // when
        race.addContestants( NUMBER_OF_RACE_THREADS, id -> new RelationshipIncrementer(
                counter -> incrementCounts( counter, relationship( 10, 2, 20 ) ) ),
                NUMBER_OF_RACE_ITERATIONS );
        race.goUnchecked();

        // then
        try ( CountsState.CountsChecker checker = countsState.checker( noConsistencyReporter ) )
        {
            visitRelationshipCountForAllPermutations( checker, 1, 2, 3, TOTAL_COUNT );
        }
    }

    private void incrementCounts( RelationshipCounter counter, RelationshipRecord relationship )
    {
        countsState.incrementRelationshipTypeCounts( counter, relationship );
        countsState.incrementRelationshipNodeCounts( counter, relationship, true, true );
    }

    @Test
    void shouldIncrementRelationshipCountAboveHighLabelId()
    {
        // given
        putLabelsOnNodes( nodeLabels( 1, 50 ), nodeLabels( 3, 60 ) );

        // when
        race.addContestants( NUMBER_OF_RACE_THREADS, id -> new RelationshipIncrementer(
                counter -> incrementCounts( counter, relationship( 1, 2, 3 ) ) ), NUMBER_OF_RACE_ITERATIONS );
        race.goUnchecked();

        // then
        try ( CountsState.CountsChecker checker = countsState.checker( noConsistencyReporter ) )
        {
            visitRelationshipCountForAllPermutations( checker, 50, 2, 60, TOTAL_COUNT );
        }
    }

    @Test
    void shouldIncrementRelationshipCountForNegativeLabelId()
    {
        // given
        putLabelsOnNodes( nodeLabels( 1, -50 ), nodeLabels( 3, -60 ) );

        // when
        race.addContestants( NUMBER_OF_RACE_THREADS, id -> new RelationshipIncrementer(
                counter -> incrementCounts( counter, relationship( 1, 2, 3 ) ) ), NUMBER_OF_RACE_ITERATIONS );
        race.goUnchecked();

        // then
        try ( CountsState.CountsChecker checker = countsState.checker( noConsistencyReporter ) )
        {
            visitRelationshipCountForAllPermutations( checker, -50, 2, -60, TOTAL_COUNT );
        }
    }

    @Test
    void shouldIncrementRelationshipCountAboveHighRelationshipTypeId()
    {
        // given
        putLabelsOnNodes( nodeLabels( 1, 5 ), nodeLabels( 3, 7 ) );

        // when
        race.addContestants( NUMBER_OF_RACE_THREADS, id -> new RelationshipIncrementer(
                counter -> incrementCounts( counter, relationship( 1, 27, 3 ) ) ), NUMBER_OF_RACE_ITERATIONS );
        race.goUnchecked();

        // then
        try ( CountsState.CountsChecker checker = countsState.checker( noConsistencyReporter ) )
        {
            visitRelationshipCountForAllPermutations( checker, 5, 27, 7, TOTAL_COUNT );
        }
    }

    @Test
    void shouldIncrementRelationshipCountForNegativeHighRelationshipTypeId()
    {
        // given
        putLabelsOnNodes( nodeLabels( 1, 5 ), nodeLabels( 3, 7 ) );

        // when
        race.addContestants( NUMBER_OF_RACE_THREADS, id -> new RelationshipIncrementer(
                counter -> incrementCounts( counter, relationship( 1, -27, 3 ) ) ), NUMBER_OF_RACE_ITERATIONS );
        race.goUnchecked();

        // then
        try ( CountsState.CountsChecker checker = countsState.checker( noConsistencyReporter ) )
        {
            visitRelationshipCountForAllPermutations( checker, 5, -27, 7, TOTAL_COUNT );
        }
    }

    @Test
    void shouldReportRelationshipCountMismatches()
    {
        // when
        RelationshipCounter counter = countsState.instantiateRelationshipCounter();
        long node1 = 1;
        long node2 = 2;
        long node3 = 3;
        putLabelsOnNodes(
                nodeLabels( node1, 7 ),
                nodeLabels( node2, 6 ),
                nodeLabels( node3, 70 ) );
        incrementCounts( counter, relationship( node1, 1, node2 ) );
        incrementCounts( counter, relationship( node1, 1, node3 ) );
        incrementCounts( counter, relationship( node1, 100, node2 ) );
        incrementCounts( counter, relationship( node1, 100, node3 ) );

        // The increments above results in the following actual increments:
        // Combination      Count
        // ----------------|-----
        // ANY,1,6          1 // visited with wrong count
        // ANY,1,70         1
        // ANY,100,6        1
        // ANY,100,70       1
        // 7,100,ANY        2 // visited with wrong count
        // 7,1,ANY          2
        // ANY,1,ANY        2
        // ANY,100,ANY      2
        // ANY,ANY,ANY      4
        // ANY,ANY,6        2
        // 7,ANY,ANY        4
        // ANY,ANY,70       2

        // then
        try ( CountsState.CountsChecker checker = countsState.checker( inconsistencyReporter ) )
        {
            // visiting unseen relationship counts
            checker.visitRelationshipCount( 2, 1, ANY_LABEL, 1 );
            checker.visitRelationshipCount( 6, 2, ANY_LABEL, 1 );
            checker.visitRelationshipCount( ANY_LABEL, 1, 8, 1 );
            checker.visitRelationshipCount( ANY_LABEL, 100, 71, 1 );
            checker.visitRelationshipCount( 7, 99, ANY_LABEL, 1 );
            checker.visitRelationshipCount( ANY_LABEL, 100, 7, 1 );

            // visiting wrong counts
            checker.visitRelationshipCount( ANY_LABEL, 1, 6, 999 );
            checker.visitRelationshipCount( 7, 100, ANY_LABEL, 999 );

            // not visiting 10 counts
        }
        verify( inconsistencyReporter, times( 18 ) ).forCounts( any() );
    }

    private void visitRelationshipCountForAllPermutations( CountsState.CountsChecker checker, int startLabel, int relationshipType, int endLabel, long count )
    {
        checker.visitRelationshipCount( startLabel, relationshipType, ANY_LABEL, count );
        checker.visitRelationshipCount( startLabel, ANY_RELATIONSHIP_TYPE, ANY_LABEL, count );
        checker.visitRelationshipCount( ANY_LABEL, relationshipType, endLabel, count );
        checker.visitRelationshipCount( ANY_LABEL, relationshipType, ANY_LABEL, count );
        checker.visitRelationshipCount( ANY_LABEL, ANY_RELATIONSHIP_TYPE, endLabel, count );
        checker.visitRelationshipCount( ANY_LABEL, ANY_RELATIONSHIP_TYPE, ANY_LABEL, count );
    }

    @SafeVarargs
    private void putLabelsOnNodes( Pair<Long,long[]>... labelDefinitions )
    {
        CacheAccess.Client client = cacheAccess.client();
        for ( Pair<Long,long[]> labelDefinition : labelDefinitions )
        {
            long index = countsState.cacheDynamicNodeLabels( labelDefinition.other() );
            client.putToCacheSingle( labelDefinition.first(), SLOT_LABELS, index );
        }
    }

    private Pair<Long,long[]> nodeLabels( long nodeId, long... labelIds )
    {
        return Pair.of( nodeId, labelIds );
    }

    private RelationshipRecord relationship( long startNodeId, int relationshipType, long endNodeId )
    {
        return new RelationshipRecord( 0 ).initialize( true, NULL_REFERENCE.longValue(), startNodeId, endNodeId, relationshipType,
                NULL_REFERENCE.longValue(), NULL_REFERENCE.longValue(), NULL_REFERENCE.longValue(), NULL_REFERENCE.longValue(), true, true );
    }

    private class RelationshipIncrementer implements Runnable
    {
        private final RelationshipCounter counter = countsState.instantiateRelationshipCounter();
        private final Consumer<RelationshipCounter> increment;

        RelationshipIncrementer( Consumer<RelationshipCounter> increment )
        {
            this.increment = increment;
        }

        @Override
        public void run()
        {
            increment.accept( counter );
        }
    }
}
