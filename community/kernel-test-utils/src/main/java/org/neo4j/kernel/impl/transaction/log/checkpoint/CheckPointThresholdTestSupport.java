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
package org.neo4j.kernel.impl.transaction.log.checkpoint;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.SettingImpl;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.pruning.LogPruning;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.time.Clocks;
import org.neo4j.time.FakeClock;

public class CheckPointThresholdTestSupport {
    public static final long ARBITRARY_LOG_VERSION = 5;
    public static final long ARBITRARY_LOG_OFFSET = 128;
    public static final LogPosition ARBITRARY_LOG_POSITION =
            new LogPosition(ARBITRARY_LOG_VERSION, ARBITRARY_LOG_OFFSET);

    protected Config config;
    protected FakeClock clock;
    protected LogPruning logPruning;
    protected InternalLogProvider logProvider;
    protected Integer intervalTx;
    protected Duration intervalTime;
    protected Consumer<String> notTriggered;
    protected BlockingQueue<String> triggerConsumer;
    protected Consumer<String> triggered;

    @BeforeEach
    public void setUp() {
        config = Config.defaults();
        clock = Clocks.fakeClock();
        logPruning = LogPruning.NO_PRUNING;
        logProvider = NullLogProvider.getInstance();
        intervalTx = config.get(GraphDatabaseSettings.check_point_interval_tx);
        intervalTime = config.get(GraphDatabaseSettings.check_point_interval_time);
        triggerConsumer = new LinkedBlockingQueue<>();
        triggered = triggerConsumer::offer;
        notTriggered = s -> Assertions.fail("Should not have triggered: " + s);
    }

    protected void withPolicy(String policy) {
        config.set(
                GraphDatabaseSettings.check_point_policy,
                ((SettingImpl<GraphDatabaseSettings.CheckpointPolicy>) GraphDatabaseSettings.check_point_policy)
                        .parse(policy));
    }

    protected void withIntervalTime(String time) {
        config.set(
                GraphDatabaseSettings.check_point_interval_time,
                ((SettingImpl<Duration>) GraphDatabaseSettings.check_point_interval_time).parse(time));
    }

    protected void withIntervalTx(int count) {
        config.set(GraphDatabaseSettings.check_point_interval_tx, count);
    }

    protected CheckPointThreshold createThreshold() {
        return CheckPointThreshold.createThreshold(config, clock, logPruning, logProvider);
    }

    protected void verifyTriggered(String... reason) {
        assertThat(triggerConsumer.poll()).contains(reason);
    }

    protected void verifyNoMoreTriggers() {
        assertTrue(triggerConsumer.isEmpty());
    }
}
