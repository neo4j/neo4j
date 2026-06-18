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
package org.neo4j.collection;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.Stream;
import org.eclipse.collections.impl.set.mutable.primitive.LongHashSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class BloomFilterTest {

    Random random = new Random();
    long seed;

    @BeforeEach
    void setRandom() {
        seed = System.nanoTime();
        random.setSeed(seed);
    }

    @ParameterizedTest
    @MethodSource("provideBloomFilterImplementations")
    void bloomFilter(BloomFilter filter) {
        // Given
        List<Long> values = List.of(1L, 10L, 1000L, 10000000L, 1000000000000L, 1000000000000000L);

        // When
        values.forEach(filter::add);
        // Then
        for (long value : values) {
            assertThat(filter.mayContain(value - 1)).isFalse();
            assertThat(filter.mayContain(value)).isTrue();
            assertThat(filter.mayContain(value + 1)).isFalse();
        }
    }

    @ParameterizedTest
    @MethodSource("provideBloomFilterImplementations")
    void bloomFilterRandomNumbers(BloomFilter filter) {
        // Given
        List<Long> values = new ArrayList<>();
        // When
        for (int i = 0; i < 10; i++) {
            long value = random.nextLong();
            values.add(value);
            filter.add(value);
        }
        // Then
        for (long value : values) {
            assertThat(filter.mayContain(value)).as("random seed %d", seed).isTrue();
        }
    }

    private void probablisticOneSeedOneFilter(BloomFilter filter, long seed) {
        int entries = LARGE_ENTRIES;
        long range = LARGE_RANGE;
        LongHashSet added = new LongHashSet();
        random.setSeed(seed);
        random.longs(range).limit(entries).forEach(value -> {
            filter.add(value);
            added.add(value);
        });
        for (var it = added.longIterator(); it.hasNext(); ) {
            long in = it.next();
            assertThat(filter.mayContain(in)).isTrue();
        }
        long falsePositives = random.longs(range)
                .filter(value -> !added.contains(value))
                .limit(entries)
                .filter(filter::mayContain)
                .count();
        double falsePositiveRatio = ((double) falsePositives) / entries;
        assertThat(falsePositiveRatio).as("False positive from seed %d", seed).isLessThan(0.01);
        assertThat(falsePositiveRatio).as("False positive from seed %d", seed).isGreaterThan(0.001);
    }

    @ParameterizedTest
    @MethodSource("provideLargeBloomFilterImplementations")
    void probabilisticRangeIsExpected(FilterFactory filterFactory) {
        for (long seed : notRandomSeeds) {
            BloomFilter filter = filterFactory.make(LARGE_SIZE, LARGE_NUM_HASHES);
            probablisticOneSeedOneFilter(filter, seed);
        }
    }

    private static Stream<Arguments> provideBloomFilterImplementations() {
        return Stream.of(
                Arguments.of(new ConcurrentLongBloomFilter(100, 4)), Arguments.of(new LongBloomFilter(100, 4)));
    }

    private static final int LARGE_ENTRIES = 10000;
    private static final int LARGE_SIZE = LARGE_ENTRIES / 5;
    private static final long LARGE_RANGE = LARGE_ENTRIES * 4;
    private static final int LARGE_NUM_HASHES = 4;

    private static Stream<Arguments> provideLargeBloomFilterImplementations() {
        return Stream.of(
                Arguments.of(new FilterFactory() {
                    @Override
                    BloomFilter make(int filterSize, int numHashes) {
                        return new ConcurrentLongBloomFilter(filterSize, numHashes);
                    }
                }),
                Arguments.of(new FilterFactory() {
                    @Override
                    BloomFilter make(int filterSize, int numHashes) {
                        return new LongBloomFilter(filterSize, numHashes);
                    }
                }));
    }

    /// Run through the bloom filter with known seeds to confirm
    /// that we always have a small number of false positives.

    private static final long[] notRandomSeeds = {
        2466234495127034133L,
        -1398222438294726337L,
        6220812387435589971L,
        -4354970574556972974L,
        -6192450482595333094L,
        2203858256288181555L,
        7218798837133657392L,
        -4649499299831216120L,
        -9039420545886279687L,
        -3503567983512833304L,
        -4045647099278007681L,
        -386994946565382111L,
        -3118558681812790439L,
        -7787861585702037595L,
        -5149175543177479112L,
        3280898560653986891L,
        -5244964261539249294L,
        -6993339135729325902L,
        1986597642999705649L,
        -816002000159039738L,
        5773760117985148622L,
        7731011899412246742L,
        -1458353605752067021L,
        8089287116916492128L,
        -5174717681586803645L,
        3319387959597669012L,
        -4519072027480247825L,
        558425367478929996L,
        -8537874492959267176L,
        2836806833599361182L,
        1986640995608044408L,
        4761238025693267556L,
        2557161826898330034L,
        4194403176308244545L,
        -6208668763120839716L,
        -4448570777632145067L,
        -3349400742713774210L,
        5153432717872698406L,
        3327324309897229603L,
        -7307092547777545961L,
        -349404560880264891L,
        5872399826574532137L,
        1035524820565457449L,
        8093407123360029332L,
        -242864332212311675L,
        5909819504223481982L,
        -8489397225837593782L,
        -595712631999905018L,
        6884135596608630298L,
        -2703351281079700037L,
        6475065618500353902L,
        2960157791522163328L,
        -2602724233111204826L,
        3494951390449239014L,
        7873212267747174636L,
        -3048951160946709856L,
        2268670396494259880L,
        8816324214728704470L,
        2955033558657503697L,
        -9121174828107158768L,
        -6628973304928258688L,
        8207169687657378012L,
        -4950623484578961662L,
        -1685423643203805871L,
        6194328374101057794L,
        4551968713585031520L,
        -483100491780492815L,
        7617009775268100872L,
        -8427381730950471480L,
        -428472448518921905L,
        196972145325128827L,
        606971429571842537L,
        5875227372876552673L,
        6186909596691241988L,
        -8555123050274848100L,
        3281257998482372381L,
        7417402574677368256L,
        7579744944791455643L,
        -7412630723800974597L,
        8660508228640235967L,
        4307253170526855656L,
        6391156314166599125L,
        -4743564983411527945L,
        4376011452040122179L,
        59051837282758772L,
        2800380655558102082L,
        -1513636847249780534L,
        -7651319721263436972L,
        -6272556437332436871L,
        -2153778041530160847L,
        -9071292768393823311L,
        7889943983333100729L,
        -18079056353691473L,
        -4820520153178012192L,
        6294665584728120596L,
        -8430641744966954984L,
        -205903163537614954L,
        1535865492801463372L,
        -1367667618091491757L,
        8252680796652877787L
    };

    private abstract static class FilterFactory {
        abstract BloomFilter make(int filterSize, int numHashes);
    }
}
