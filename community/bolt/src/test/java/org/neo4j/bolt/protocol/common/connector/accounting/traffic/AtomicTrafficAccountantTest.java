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
package org.neo4j.bolt.protocol.common.connector.accounting.traffic;

import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.AssertableLogProvider.Level;
import org.neo4j.logging.LogAssertions;
import org.neo4j.logging.internal.SimpleLogService;
import org.neo4j.time.FakeClock;

class AtomicTrafficAccountantTest {

    private FakeClock clock;
    private AssertableLogProvider logProvider;

    @BeforeEach
    void prepare() {
        this.clock = new FakeClock();
        this.logProvider = new AssertableLogProvider();
    }

    @Test
    void shouldWarnWhenReadBandwidthIsExceeded() {
        var accountant = new AtomicTrafficAccountant(500, 1, 1, 500, clock, new SimpleLogService(this.logProvider));

        accountant.notifyRead(100_000);

        LogAssertions.assertThat(this.logProvider).doesNotHaveAnyLogs();

        accountant.notifyRead(20_000);

        LogAssertions.assertThat(this.logProvider).doesNotHaveAnyLogs();

        accountant.notifyWrite(8_000);

        LogAssertions.assertThat(this.logProvider).doesNotHaveAnyLogs();

        accountant.notifyRead(20_625);

        LogAssertions.assertThat(this.logProvider).doesNotHaveAnyLogs();

        accountant.tryCheck();

        LogAssertions.assertThat(this.logProvider).doesNotHaveAnyLogs();

        this.clock.forward(500, TimeUnit.MILLISECONDS);

        accountant.tryCheck();

        LogAssertions.assertThat(this.logProvider)
                .forLevel(Level.WARN)
                .containsMessageWithArguments(
                        "Inbound bandwidth threshold has been exceeded (%.2f Mb/s exceeds configured threshold of %.2f Mb/s)",
                        2.25, 1.0f);
    }

    @Test
    void shouldNotWarnWhenReadBandwidthIsNeverExceeded() {
        var accountant = new AtomicTrafficAccountant(500, 1, 1, 500, clock, new SimpleLogService(this.logProvider));

        accountant.notifyRead(5_000);

        LogAssertions.assertThat(this.logProvider).doesNotHaveAnyLogs();

        this.clock.forward(500, TimeUnit.MILLISECONDS);

        accountant.tryCheck();

        LogAssertions.assertThat(this.logProvider).doesNotHaveAnyLogs();
    }

    @Test
    void shouldClearWarningWhenReadBandwidthFallsBelowThreshold() {
        var accountant = new AtomicTrafficAccountant(500, 1, 1, 500, clock, new SimpleLogService(this.logProvider));

        accountant.notifyRead(140_625);

        this.clock.forward(500, TimeUnit.MILLISECONDS);

        accountant.tryCheck();

        LogAssertions.assertThat(this.logProvider)
                .forLevel(Level.WARN)
                .containsMessageWithArguments(
                        "Inbound bandwidth threshold has been exceeded (%.2f Mb/s exceeds configured threshold of %.2f Mb/s)",
                        2.25, 1.0f);

        this.clock.forward(500, TimeUnit.MILLISECONDS);

        accountant.tryCheck();

        LogAssertions.assertThat(this.logProvider)
                .forLevel(Level.INFO)
                .containsMessageWithArguments(
                        "Inbound bandwidth has normalized (traffic has dropped below %.2f Mb/s for at least %d ms)",
                        1.0f, 500L);
    }

    @Test
    void shouldWarnWhenWriteBandwidthIsExceeded() {
        var accountant = new AtomicTrafficAccountant(500, 1, 1, 500, clock, new SimpleLogService(this.logProvider));

        accountant.notifyWrite(100_000);

        LogAssertions.assertThat(this.logProvider).doesNotHaveAnyLogs();

        accountant.notifyWrite(20_000);

        LogAssertions.assertThat(this.logProvider).doesNotHaveAnyLogs();

        accountant.notifyRead(5_000);

        LogAssertions.assertThat(this.logProvider).doesNotHaveAnyLogs();

        accountant.notifyWrite(20_625);

        LogAssertions.assertThat(this.logProvider).doesNotHaveAnyLogs();

        accountant.tryCheck();

        LogAssertions.assertThat(this.logProvider).doesNotHaveAnyLogs();

        this.clock.forward(500, TimeUnit.MILLISECONDS);

        accountant.tryCheck();

        LogAssertions.assertThat(this.logProvider)
                .forLevel(Level.WARN)
                .containsMessageWithArguments(
                        "Outbound bandwidth threshold has been exceeded (%.2f Mb/s exceeds configured threshold of %.2f Mb/s)",
                        2.25, 1.0f);
    }

    @Test
    void shouldNotWarnWhenWriteBandwidthIsNeverExceeded() {
        var accountant = new AtomicTrafficAccountant(500, 1, 1, 500, clock, new SimpleLogService(this.logProvider));

        accountant.notifyWrite(5_000);

        LogAssertions.assertThat(this.logProvider).doesNotHaveAnyLogs();

        this.clock.forward(500, TimeUnit.MILLISECONDS);

        accountant.tryCheck();

        LogAssertions.assertThat(this.logProvider).doesNotHaveAnyLogs();
    }

    @Test
    void shouldClearWarningWhenWriteBandwidthFallsBelowThreshold() {
        var accountant = new AtomicTrafficAccountant(500, 1, 1, 500, clock, new SimpleLogService(this.logProvider));

        accountant.notifyWrite(140_625);

        this.clock.forward(500, TimeUnit.MILLISECONDS);

        accountant.tryCheck();

        LogAssertions.assertThat(this.logProvider)
                .forLevel(Level.WARN)
                .containsMessageWithArguments(
                        "Outbound bandwidth threshold has been exceeded (%.2f Mb/s exceeds configured threshold of %.2f Mb/s)",
                        2.25, 1.0f);

        this.clock.forward(500, TimeUnit.MILLISECONDS);

        accountant.tryCheck();

        LogAssertions.assertThat(this.logProvider)
                .forLevel(Level.INFO)
                .containsMessageWithArguments(
                        "Outbound bandwidth has normalized (traffic has dropped below %.2f Mb/s for at least %d ms)",
                        1.0f, 500L);
    }
}
