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
package org.neo4j.scheduler;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.engine.descriptor.JupiterEngineDescriptor.ENGINE_ID;
import static org.junit.platform.engine.discovery.DiscoverySelectors.selectClass;
import static org.junit.platform.testkit.engine.EventConditions.event;
import static org.junit.platform.testkit.engine.EventConditions.finishedWithFailure;
import static org.junit.platform.testkit.engine.TestExecutionResultConditions.instanceOf;
import static org.junit.platform.testkit.engine.TestExecutionResultConditions.message;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.platform.testkit.engine.EngineTestKit;
import org.junit.platform.testkit.engine.Events;
import org.neo4j.test.extension.Inject;

@ExtendWith(JobSchedulerExtension.class)
class JobSchedulerExtensionTest {
    @Inject
    private JobScheduler jobScheduler;

    @Test
    void injectStartedJobScheduler() throws InterruptedException {
        CountDownLatch countDownLatch = new CountDownLatch(1);
        jobScheduler.schedule(Group.TESTING, countDownLatch::countDown);
        assertTrue(countDownLatch.await(1, TimeUnit.MINUTES));
    }

    @Nested
    class ShutdownScheduler {

        @Test
        void componentShutdownAfterTest() {
            Events testEvents = EngineTestKit.engine(ENGINE_ID)
                    .selectors(selectClass(JobSchedulerExtensionShutdown.class))
                    .enableImplicitConfigurationParameters(true)
                    .execute()
                    .testEvents();

            testEvents
                    .assertThatEvents()
                    .haveExactly(
                            1,
                            event(finishedWithFailure(
                                    instanceOf(RuntimeException.class),
                                    message(message -> message.contains("Shutdown called.")))));
        }
    }
}
