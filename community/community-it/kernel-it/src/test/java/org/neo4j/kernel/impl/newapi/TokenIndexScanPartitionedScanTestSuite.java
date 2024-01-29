/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.newapi;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.common.EntityType;
import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.kernel.api.Cursor;
import org.neo4j.internal.kernel.api.PartitionedScan;
import org.neo4j.internal.kernel.api.TokenPredicate;
import org.neo4j.internal.kernel.api.TokenReadSession;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.newapi.PartitionedScanFactories.TokenIndex;
import org.neo4j.kernel.impl.newapi.PartitionedScanTestSuite.Query;
import org.neo4j.kernel.impl.newapi.TokenIndexScanPartitionedScanTestSuite.TokenScanQuery;
import org.neo4j.test.Tokens;

public abstract class TokenIndexScanPartitionedScanTestSuite<CURSER extends Cursor>
        implements PartitionedScanTestSuite.TestSuite<TokenScanQuery, TokenReadSession, CURSER> {
    abstract static class WithoutData<CURSER extends Cursor>
            extends PartitionedScanTestSuite.WithoutData<TokenScanQuery, TokenReadSession, CURSER> {
        WithoutData(TokenIndexScanPartitionedScanTestSuite<CURSER> testSuite) {
            super(testSuite);
        }

        Queries<TokenScanQuery> emptyQueries(EntityType entityType, int[] tokenIds) {
            final var tokenIndexName = getTokenIndexName(entityType);
            final var empty = Arrays.stream(tokenIds)
                    .mapToObj(TokenPredicate::new)
                    .map(pred -> new TokenScanQuery(tokenIndexName, pred))
                    .collect(EntityIdsMatchingQuery.collector());
            return new Queries<>(empty);
        }
    }

    abstract static class WithData<CURSER extends Cursor>
            extends PartitionedScanTestSuite.WithData<TokenScanQuery, TokenReadSession, CURSER> {
        WithData(TokenIndexScanPartitionedScanTestSuite<CURSER> testSuite) {
            super(testSuite);
        }

        abstract Queries<TokenScanQuery> createData(long numberOfEntities, SortedMap<Integer, List<Range>> tokenRanges);

        /*
            sections    | before  | former  |   gap   | latter  |  after  |    notes:
            entities    ...................................................

             token 0              [         )         [         )              former and latter leading ranges

             token 1       [   )                                               before former leading range
             token 2         [         )                                       partially before, and partially within, former leading range
             token 3                 [   )                                     within former leading range
             token 4                           [   )                           within the gap between leading ranges
             token 5                   [                   )                   spanning between the former, and the latter, leading ranges
             token 6                                       [         )         partially within, and partially after, latter leading range
             token 7                                               [   )       after latter leading range
             token 8       [   )                                   [   )       split, before former leading range, and after latter leading range
        */
        protected List<List<Range>> createTokenRanges(long numberOfEntities) {
            final var range = new Range(0, numberOfEntities);

            // leading token range between [40, 60)% of all entities
            final var leadingTokenRangeSize = range.randomBetweenQuantiles(random.random(), 40, 60, 100) - range.min();
            // centered around, between [40, 60)% of all entities
            final var leadingTokenCenter = range.randomBetweenQuantiles(random.random(), 40, 60, 100);
            final var leadingTokenRange = new Range(
                    leadingTokenCenter - leadingTokenRangeSize / 2, leadingTokenCenter + leadingTokenRangeSize / 2);
            final var leadingTokenRangeAssert = assertThat(leadingTokenRange).as("leading range is within total range");
            leadingTokenRangeAssert
                    .extracting(Range::min, InstanceOfAssertFactories.LONG)
                    .isGreaterThanOrEqualTo(range.min());
            leadingTokenRangeAssert
                    .extracting(Range::max, InstanceOfAssertFactories.LONG)
                    .isLessThanOrEqualTo(range.max());

            // gap within leading token range between [25, 35)% of leading token range
            final var gapSize =
                    leadingTokenRange.randomBetweenQuantiles(random.random(), 25, 35, 100) - leadingTokenRange.min();
            // centered around, between [40, 60)% of leading token range
            final var gapCenter = leadingTokenRange.randomBetweenQuantiles(random.random(), 40, 60, 100);
            final var gap = new Range(gapCenter - gapSize / 2, gapCenter + gapSize / 2);
            final var gapAssert = assertThat(gap).as("gap range is within leading range");
            gapAssert
                    .extracting(Range::min, InstanceOfAssertFactories.LONG)
                    .isGreaterThanOrEqualTo(leadingTokenRange.min());
            gapAssert
                    .extracting(Range::max, InstanceOfAssertFactories.LONG)
                    .isLessThanOrEqualTo(leadingTokenRange.max());

            final var former = new Range(leadingTokenRange.min(), gap.min());
            final var latter = new Range(gap.max(), leadingTokenRange.max());
            final var before = new Range(range.min(), former.min());
            final var after = new Range(latter.max(), range.max());

            return List.of(
                    List.of(former, latter),
                    List.of(saneSpanningRangeFromRanges(random.random(), before, before)),
                    List.of(saneSpanningRangeFromRanges(random.random(), before, former)),
                    List.of(saneSpanningRangeFromRanges(random.random(), former, former)),
                    List.of(saneSpanningRangeFromRanges(random.random(), gap, gap)),
                    List.of(saneSpanningRangeFromRanges(random.random(), former, latter)),
                    List.of(saneSpanningRangeFromRanges(random.random(), latter, after)),
                    List.of(saneSpanningRangeFromRanges(random.random(), after, after)),
                    List.of(
                            saneSpanningRangeFromRanges(random.random(), before, before),
                            saneSpanningRangeFromRanges(random.random(), after, after)));
        }

        protected final <TOKEN> SortedMap<Integer, List<Range>> tokenRangesFromTokenId(
                Tokens.Suppliers.Supplier<TOKEN> token, List<List<Range>> ranges) {
            final var tokenIds = createTokens(ranges.size(), token);
            final var tokenRanges = new TreeMap<Integer, List<Range>>();
            for (int i = 0; i < ranges.size(); i++) {
                tokenRanges.put(tokenIds[i], ranges.get(i));
            }
            return tokenRanges;
        }

        @ParameterizedTest(name = "desiredNumberOfPartitions={0}")
        @MethodSource("rangeFromOneToMaxPartitions")
        final void shouldFollowTheLeadingScanSingles(int desiredNumberOfPartitions) throws KernelException {
            try (var tx = beginTx();
                    var entities = factory.getCursor(tx.cursors()).with(tx.cursorContext())) {
                final var validQueryEntries = queries.valid().iterator();
                final var leadingQueryEntry = validQueryEntries.next();
                assumeThat(validQueryEntries)
                        .as("there are queries to follow the partitioning of the leader")
                        .hasNext();

                final var leadingQuery = leadingQueryEntry.getKey();
                final var leadingExpectedMatches = leadingQueryEntry.getValue();

                // given  a database with entries
                // when   partitioning the scan
                final var leadingScan = factory.partitionedScan(tx, desiredNumberOfPartitions, leadingQuery);
                final var estimatedLeadingRanges = assertLeadingScan(
                        tx, entities, desiredNumberOfPartitions, leadingScan, leadingQuery, leadingExpectedMatches);

                final var tokenIndexFactory = (TokenIndex<CURSER>) factory;
                while (validQueryEntries.hasNext()) {
                    // when   partitioning the following scans
                    final var followingQueryEntry = validQueryEntries.next();
                    final var followingQuery = followingQueryEntry.getKey();
                    final var followingExpectedMatched = followingQueryEntry.getValue();

                    final var followingScan = tokenIndexFactory.partitionedScan(tx, leadingScan, followingQuery);
                    assertFollowingScan(
                            tx,
                            entities,
                            leadingScan.getNumberOfPartitions(),
                            estimatedLeadingRanges,
                            followingScan,
                            followingQuery,
                            followingExpectedMatched);
                }
            }
        }

        @ParameterizedTest(name = "desiredNumberOfPartitions={0}")
        @MethodSource("rangeFromOneToMaxPartitions")
        final void shouldFollowTheLeadingScanList(int desiredNumberOfPartitions) throws KernelException {
            try (var tx = beginTx();
                    var entities = factory.getCursor(tx.cursors()).with(tx.cursorContext())) {
                final var validQueries = new ArrayList<TokenScanQuery>();
                final var expectedMatches = new ArrayList<Set<Long>>();
                for (final var entry : queries.valid()) {
                    validQueries.add(entry.getKey());
                    expectedMatches.add(entry.getValue());
                }
                assumeThat(validQueries)
                        .size()
                        .as("there are queries to follow the partitioning of the leader")
                        .isGreaterThan(1);

                final var tokenIndexFactory = (TokenIndex<CURSER>) factory;

                // given  a database with entries
                // when   partitioning the scan
                final var partitionScans =
                        tokenIndexFactory.partitionedScans(tx, desiredNumberOfPartitions, validQueries);
                softly.assertThat(partitionScans)
                        .size()
                        .as(
                                "returned number of %s is the same as the number of given queries",
                                PartitionedScan.class.getSimpleName())
                        .isEqualTo(validQueries.size());
                final var estimatedLeadingRanges = assertLeadingScan(
                        tx,
                        entities,
                        desiredNumberOfPartitions,
                        partitionScans.get(0),
                        validQueries.get(0),
                        expectedMatches.get(0));

                final var numberOfPartitions = partitionScans.get(0).getNumberOfPartitions();
                for (int i = 1; i < partitionScans.size(); i++) {
                    // when   partitioning the following scans
                    assertFollowingScan(
                            tx,
                            entities,
                            numberOfPartitions,
                            estimatedLeadingRanges,
                            partitionScans.get(i),
                            validQueries.get(i),
                            expectedMatches.get(i));
                }
            }
        }

        private List<Range> assertLeadingScan(
                KernelTransaction tx,
                CURSER entities,
                int desiredNumberOfPartitions,
                PartitionedScan<CURSER> leadingScan,
                TokenScanQuery leadingQuery,
                Set<Long> leadingExpectedMatches) {
            // then   the number of partitions can be less, but no more than the desired number of partitions
            final var numberOfPartitions = leadingScan.getNumberOfPartitions();
            softly.assertThat(numberOfPartitions)
                    .as("number of partitions")
                    .isGreaterThan(0)
                    .isLessThanOrEqualTo(desiredNumberOfPartitions)
                    .isLessThanOrEqualTo(maxNumberOfPartitions);

            // given  each partition
            final var foundInLeading = new HashSet<Long>();
            final var estimatedLeadingRanges = new ArrayList<Range>(numberOfPartitions);
            for (int i = 0; i < numberOfPartitions; i++) {
                // when   inspecting the found entities
                estimatedLeadingRanges.add(findAllAndGetRange(tx, entities, leadingScan, foundInLeading));
            }

            // then   all the entities with matching the query should be found
            if (!leadingExpectedMatches.equals(foundInLeading)) {
                // only use softly if we see that there's a mismatch because the call is absurdly ultra slow
                softly.assertThat(foundInLeading)
                        .as("only the expected data found matching %s", leadingQuery)
                        .containsExactlyInAnyOrderElementsOf(leadingExpectedMatches);
            }

            return estimatedLeadingRanges;
        }

        private void assertFollowingScan(
                KernelTransaction tx,
                CURSER entities,
                int numberOfPartitions,
                List<Range> estimatedLeadingRanges,
                PartitionedScan<CURSER> followingScan,
                TokenScanQuery followingQuery,
                Set<Long> followingExpectedMatched) {
            // then   the number of partitions has to be the same as leading partition's
            softly.assertThat(followingScan.getNumberOfPartitions())
                    .as("number of partitions")
                    .isEqualTo(numberOfPartitions);

            // given  each partition
            final var foundInFollowing = new HashSet<Long>();
            final var estimatedRanges = new ArrayList<Range>(numberOfPartitions);
            for (int i = 0; i < numberOfPartitions; i++) {
                // when   inspecting the found entities
                final var estimatedFollowingRange = findAllAndGetRange(tx, entities, followingScan, foundInFollowing);
                final var estimatedLeadingRange = estimatedLeadingRanges.get(i);
                final var estimatedRange = Range.union(estimatedLeadingRange, estimatedFollowingRange);
                if (estimatedRange != null) {
                    estimatedRanges.add(estimatedRange);
                }
            }

            // then   all the entities with matching the query should be found
            if (!followingExpectedMatched.equals(foundInFollowing)) {
                // only use softly if we see that there's a mismatch because the call is absurdly ultra slow
                softly.assertThat(foundInFollowing)
                        .as("only the expected data found matching %s", followingQuery)
                        .containsExactlyInAnyOrderElementsOf(followingExpectedMatched);
            }

            if (!storageEngine.indexingBehaviour().useNodeIdsInRelationshipTokenIndex()) {
                // then   all estimated ranges should be strictly less than the next
                for (int i = 1; i < estimatedRanges.size(); i++) {
                    final var prev = estimatedRanges.get(i - 1);
                    final var curr = estimatedRanges.get(i);
                    softly.assertThat(Range.strictlyLessThan(prev, curr))
                            .as("%s is strictly less than %s", prev, curr)
                            .isTrue();
                }
            }
        }

        protected final Range findAllAndGetRange(
                KernelTransaction tx, CURSER entities, PartitionedScan<CURSER> scan, Set<Long> found) {
            long min = Long.MAX_VALUE;
            long max = Long.MIN_VALUE;

            try (var statement = tx.acquireStatement();
                    var executionContext = tx.createExecutionContext()) {
                scan.reservePartition(entities, executionContext);
                while (entities.next()) {
                    // then   there should be no duplicates
                    final var entity = factory.getEntityReference(entities);
                    min = Math.min(min, entity);
                    max = Math.max(max, entity);
                    softly.assertThat(found.add(entity)).as("no duplicate").isTrue();
                }

                executionContext.complete();
            }

            return !(min == Long.MAX_VALUE || max == Long.MIN_VALUE) ? new Range(min, max) : null;
        }

        protected static Range saneSpanningRangeFromRanges(Random random, Range start, Range end) {
            return Range.createSane(start.random(random), end.random(random));
        }
    }

    protected record TokenScanQuery(String indexName, TokenPredicate predicate) implements Query<TokenPredicate> {
        @Override
        public TokenPredicate get() {
            return predicate;
        }

        @Override
        public String toString() {
            return String.format(
                    "%s[index='%s', pred='%s']", getClass().getSimpleName(), indexName, predicate.tokenId());
        }
    }
}
