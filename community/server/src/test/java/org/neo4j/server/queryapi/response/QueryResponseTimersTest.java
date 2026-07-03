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
package org.neo4j.server.queryapi.response;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class QueryResponseTimersTest {

    @Test
    void shouldReturnTimesIfNothingIsNotified() {
        var timers = QueryResponseTimers.start();

        Assertions.assertNull(timers.resultAvailableAfter());
        Assertions.assertNull(timers.resultConsumedAfter());
    }

    @Test
    void shouldNotifyResultAvailableAfter() throws InterruptedException {
        var timers = QueryResponseTimers.start();
        Thread.sleep(10);

        timers.notifyResultAvailable();

        Assertions.assertNotNull(timers.resultAvailableAfter());
        Assertions.assertNull(timers.resultConsumedAfter());
    }

    @Test
    void shouldOnlyCountsOnceWhenNotifyResultAvailableAfter() throws InterruptedException {
        var timers = QueryResponseTimers.start();
        Thread.sleep(10);

        timers.notifyResultAvailable();

        Assertions.assertNotNull(timers.resultAvailableAfter());
        var resultAvailableAfter = timers.resultAvailableAfter();

        Thread.sleep(10);
        timers.notifyResultAvailable();

        Assertions.assertEquals(resultAvailableAfter, timers.resultAvailableAfter());

        Assertions.assertNull(timers.resultConsumedAfter());
    }

    @Test
    void shouldNotifyResultConsumedAfter() throws InterruptedException {
        var timers = QueryResponseTimers.start();
        Thread.sleep(10);

        timers.notifyResultConsumed();

        Assertions.assertNotNull(timers.resultConsumedAfter());
        Assertions.assertNull(timers.resultAvailableAfter());
    }

    @Test
    void shouldOnlyCountsOnceWhenNotifyResultConsumedAfter() throws InterruptedException {
        var timers = QueryResponseTimers.start();
        Thread.sleep(10);

        timers.notifyResultConsumed();

        Assertions.assertNotNull(timers.resultConsumedAfter());
        var resultConsumedAfter = timers.resultConsumedAfter();

        Thread.sleep(10);
        timers.notifyResultConsumed();

        Assertions.assertEquals(resultConsumedAfter, timers.resultConsumedAfter());

        Assertions.assertNull(timers.resultAvailableAfter());
    }
}
