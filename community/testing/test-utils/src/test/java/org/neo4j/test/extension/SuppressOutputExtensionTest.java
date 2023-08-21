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
import static org.junit.jupiter.engine.descriptor.JupiterEngineDescriptor.ENGINE_ID;
import static org.junit.platform.engine.discovery.DiscoverySelectors.selectClass;
import static org.junit.platform.testkit.engine.EventConditions.event;
import static org.junit.platform.testkit.engine.EventConditions.finishedWithFailure;
import static org.junit.platform.testkit.engine.TestExecutionResultConditions.instanceOf;
import static org.junit.platform.testkit.engine.TestExecutionResultConditions.message;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;
import org.junit.platform.testkit.engine.EngineTestKit;
import org.junit.platform.testkit.engine.Events;

class SuppressOutputExtensionTest {
    @Test
    void shouldThrowExceptionOnMissingResourceLock() {
        Events testEvents = EngineTestKit.engine(ENGINE_ID)
                .selectors(selectClass(SuppressOutputExtensionIncorrectUsage.class))
                .enableImplicitConfigurationParameters(true)
                .execute()
                .testEvents();

        testEvents
                .assertThatEvents()
                .haveExactly(
                        1,
                        event(
                                finishedWithFailure(
                                        instanceOf(IllegalStateException.class),
                                        message(
                                                message -> message.contains(
                                                        "SuppressOutputExtension requires `@ResourceLock(Resources.SYSTEM_OUT)` annotation.")))));
    }

    @Nested
    @ExtendWith(SuppressOutputExtension.class)
    @ResourceLock(Resources.SYSTEM_OUT)
    class HasLock {
        @Inject
        private SuppressOutput output;

        @Test
        void shouldSucceedWithLockPresent() {
            verifySuppressOutputWorks(output);
        }

        @Nested
        class NestedLock {
            @Test
            void shouldSucceedWithNestedLock() {
                verifySuppressOutputWorks(output);
            }
        }
    }

    @Nested
    class InheritedLock extends HasLock {
        @Inject
        private SuppressOutput output;

        @Test
        void shouldFindInheritedLock() {
            verifySuppressOutputWorks(output);
        }
    }

    @Nested
    @ExtendWith(SuppressOutputExtension.class)
    @ResourceLock("SuppressOutputTest.foo")
    @ResourceLock("SuppressOutputTest.bar")
    @ResourceLock(Resources.SYSTEM_OUT)
    class RepeatedLock {
        @Inject
        private SuppressOutput output;

        @Test
        void shouldFindRepeatedLock() {
            verifySuppressOutputWorks(output);
        }
    }

    static void verifySuppressOutputWorks(SuppressOutput output) {
        System.out.println("foo");
        System.out.println("bar");
        assertThat(output.getOutputVoice().lines()).containsExactly("foo", "bar");
    }
}
