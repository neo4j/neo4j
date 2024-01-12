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
package org.neo4j.test.extension.timeout;

import static org.junit.jupiter.engine.descriptor.JupiterEngineDescriptor.ENGINE_ID;
import static org.junit.platform.engine.discovery.DiscoverySelectors.selectMethod;
import static org.junit.platform.testkit.engine.EventConditions.event;
import static org.junit.platform.testkit.engine.EventConditions.finishedWithFailure;
import static org.junit.platform.testkit.engine.TestExecutionResultConditions.instanceOf;
import static org.junit.platform.testkit.engine.TestExecutionResultConditions.message;

import java.util.Arrays;
import org.assertj.core.api.Condition;
import org.junit.jupiter.api.Test;
import org.junit.platform.testkit.engine.EngineTestKit;
import org.junit.platform.testkit.engine.Events;
import org.neo4j.test.extension.timeout.DumpThreadDumpOnTimeout.After;
import org.neo4j.test.extension.timeout.DumpThreadDumpOnTimeout.Before;
import org.neo4j.test.extension.timeout.DumpThreadDumpOnTimeout.IncludeThreadsCleanedOnAfter;
import org.neo4j.test.extension.timeout.DumpThreadDumpOnTimeout.ThreadDumpingDisabled;

class VerboseTimeoutExceptionExtensionTest {
    @Test
    void shouldDumpThreadsOnTimeout() {
        assertTestGetsThreadDump("dumpOnTimeoutException");
        assertTestGetsThreadDump("dumpOnAssertionFailedErrorWithMessage");
        assertTestGetsThreadDump("dumpOnTimeoutPreemptively");
        assertTestGetsThreadDump("dumpOnTimeoutAnnotation");
        assertTestGetsThreadDump("dumpOnCauseTimeout");
        assertTestGetsThreadDump("dumpOnSuppressedTimeout");
        assertTestGetsThreadDump("dumpOnDeepCauseTimeout");
        assertTestGetsThreadDump("dumpOnDeepSuppressedTimeout");
        assertTestGetsThreadDump("dumpOnAssertEventually");
    }

    @Test
    void shouldDumpOnSetupOrTeardown() {
        assertTestGetsThreadDump("testWithoutTimeout", After.class);
        assertTestGetsThreadDump("testWithoutTimeout", Before.class);
    }

    @Test
    void shouldContainDumpOnThreadsCleanedOnAfter() {
        assertTestGetsThreadDumpWithMessage(
                "shouldContainHangingThread",
                IncludeThreadsCleanedOnAfter.class,
                "HangingThread",
                "IncludeThreadsCleanedOnAfter.hangingMethod");
    }

    @Test
    void shouldNotDumpThreadsOnNormalFailure() {
        assertTestGetsNoThreadDump("doNotDumpOnAssume");
        assertTestGetsNoThreadDump("doNotDumpOnAssert");
        assertTestGetsNoThreadDump("doNotDumpOnException");
        assertTestGetsNoThreadDump("doNotDumpOnDeepException");
    }

    @Test
    void shouldNotDumpThreadsIfDisabled() {
        assertTestGetsNoThreadDump("testWithTimeout", ThreadDumpingDisabled.class);
    }

    static void assertTestGetsThreadDump(String test) {
        assertTestGetsThreadDump(test, DumpThreadDumpOnTimeout.class);
    }

    static void assertTestGetsNoThreadDump(String test) {
        assertTestGetsNoThreadDump(test, DumpThreadDumpOnTimeout.class);
    }

    static void assertTestGetsThreadDump(String test, Class<?> cls) {
        assertThreadDumpEvent(executeTest(test, cls), true);
    }

    static void assertTestGetsNoThreadDump(String test, Class<?> cls) {
        assertThreadDumpEvent(executeTest(test, cls), false);
    }

    static void assertTestGetsThreadDumpWithMessage(String test, Class<?> cls, String... messages) {
        assertThreadDumpWithMessage(executeTest(test, cls), messages);
    }

    private static Events executeTest(String method, Class<?> cls) {
        Events events = EngineTestKit.engine(ENGINE_ID)
                .selectors(selectMethod(cls, method))
                .enableImplicitConfigurationParameters(true)
                .execute()
                .testEvents();
        events.assertStatistics(stats -> stats.finished(1));
        return events;
    }

    private static void assertThreadDumpEvent(Events events, boolean shouldHave) {
        events.assertThatEvents()
                .haveExactly(
                        shouldHave ? 1 : 0,
                        event(finishedWithFailure(
                                suppressed(instanceOf(VerboseTimeoutExceptionExtension.ThreadDump.class)))));
    }

    private static void assertThreadDumpWithMessage(Events events, String... expected) {
        events.assertThatEvents()
                .haveExactly(
                        1,
                        event(finishedWithFailure(
                                suppressed(instanceOf(VerboseTimeoutExceptionExtension.ThreadDump.class)),
                                suppressed(
                                        message(trace -> Arrays.stream(expected).allMatch(trace::contains))))));
    }

    private static Condition<Throwable> suppressed(Condition<Throwable> condition) {
        return new Condition<>(
                throwable -> Arrays.stream(throwable.getSuppressed()).anyMatch(condition::matches),
                "Suppressed throwable matches %s",
                condition);
    }
}
