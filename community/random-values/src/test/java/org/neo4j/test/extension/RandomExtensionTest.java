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
package org.neo4j.test.extension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.test.RandomSupport;

@ExtendWith(RandomExtension.class)
class RandomExtensionTest {
    @Inject
    RandomSupport random;

    @BeforeEach
    void injectedBeforeEach() {
        assertInjected(random);
    }

    @BeforeEach
    void initialisedBeforeEach() {
        assertInitialised(random);
    }

    @Test
    void injectedTest() {
        assertInjected(random);
    }

    @Test
    void initialisedTest() {
        assertInitialised(random);
    }

    @Test
    @RandomSupport.Seed(1337)
    void seedTest() {
        assertSeed(random, 1337);
    }

    private static void assertInjected(RandomSupport random) {
        assertNotNull(random);
    }

    private static void assertInitialised(RandomSupport random) {
        assertNotNull(random.nextAlphaNumericString());
    }

    private static void assertSeed(RandomSupport random, long seed) {
        assertThat(random.seed()).isEqualTo(seed);
    }

    @Nested
    class NestedRandomTest {
        @Inject
        RandomSupport nestedRandom;

        @BeforeEach
        void injectedNestedBeforeEach() {
            assertInjected(nestedRandom);
        }

        @BeforeEach
        void initialisedNestedBeforeEach() {
            assertInitialised(nestedRandom);
        }

        @Test
        void injectedNestedTest() {
            assertInjected(nestedRandom);
        }

        @Test
        void initialisedNestedTest() {
            assertInitialised(nestedRandom);
        }

        @Test
        @RandomSupport.Seed(15)
        void seedNestedTest() {
            assertSeed(nestedRandom, 15);
        }
    }

    @Nested
    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    class NestedPerClassRandomTest {
        @Inject
        RandomSupport lifetimeRandom;

        private long firstSeed;

        @BeforeAll
        void assertPerClassBeforeAll() {
            assertInjected(lifetimeRandom);
            assertInitialised(lifetimeRandom);
            firstSeed = lifetimeRandom.seed();
        }

        @BeforeEach
        void injectedPerClassBeforeEach() {
            assertNotNull(lifetimeRandom);
        }

        @BeforeEach
        void initialisedPerClassBeforeEach() {
            assertInitialised(lifetimeRandom);
        }

        @BeforeEach
        void seedPerClassBeforeEach() {
            assertSeed(lifetimeRandom, firstSeed);
        }

        @Test
        void injectedPerClassTest() {
            assertInjected(lifetimeRandom);
        }

        @Test
        void initialisedPerClassTest() {
            assertInitialised(lifetimeRandom);
        }

        @Test
        void seedPerClassTest() {
            assertSeed(lifetimeRandom, firstSeed);
        }
    }

    @Nested
    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    @RandomSupport.Seed(555)
    class NestedPerClassWithSeedRandomTest {
        @Inject
        RandomSupport lifetimeRandom;

        @BeforeAll
        void assertPerClassWithSeedBeforeAll() {
            assertInjected(lifetimeRandom);
            assertInitialised(lifetimeRandom);
            assertSeed(lifetimeRandom, 555);
        }

        @BeforeEach
        void seedPerClassWithSeedBeforeEach() {
            assertSeed(lifetimeRandom, 555);
        }

        @Test
        void seedPerClassWithSeedTest() {
            assertSeed(lifetimeRandom, 555);
        }
    }

    @Nested
    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    @TestMethodOrder(MethodOrderer.MethodName.class)
    class NestedPerClassWithOrderRandomTest {
        @Inject
        RandomSupport lifetimeRandom;

        private long firstSeed;
        private long testCounter;

        @BeforeAll
        void assertPerClassBeforeAll() {
            assertInjected(lifetimeRandom);
            assertInitialised(lifetimeRandom);
            firstSeed = lifetimeRandom.seed();
        }

        @BeforeEach
        void injectedPerClassBeforeEach() {
            assertNotNull(lifetimeRandom);
        }

        @BeforeEach
        void initialisedPerClassBeforeEach() {
            assertInitialised(lifetimeRandom);
        }

        @BeforeEach
        void seedPerClassBeforeEach() {
            assertSeed(lifetimeRandom, firstSeed);
        }

        @Test
        void injectedPerClassTest() {
            assertInjected(lifetimeRandom);
            assertThat(testCounter++).isEqualTo(1);
        }

        @Test
        void initialisedPerClassTest() {
            assertInitialised(lifetimeRandom);
            assertThat(testCounter++).isEqualTo(0);
        }

        @Test
        void seedPerClassTest() {
            assertSeed(lifetimeRandom, firstSeed);
            assertThat(testCounter++).isEqualTo(2);
        }
    }
}
