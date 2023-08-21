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
package org.neo4j.dbms.diagnostics.profile;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.test.assertion.Assert.assertEventually;
import static org.neo4j.test.conditions.Conditions.TRUE;
import static org.neo4j.test.conditions.Conditions.equalityCondition;
import static org.neo4j.test.conditions.Conditions.instanceOf;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.time.FakeClock;
import org.neo4j.time.SystemNanoClock;

class ProfileToolTest {
    private ProfileTool tool;
    private static final Runnable NOOP = () -> {};
    private static final Runnable THROW = () -> {
        throw new RuntimeException("Fail");
    };

    @BeforeEach
    void setup() {
        this.tool = new ProfileTool();
    }

    @AfterEach
    void cleanup() {
        tool.close();
    }

    @Test
    void shouldStartAndStopBasicProfiler() {
        TestProfiler profiler = new TestProfiler(NOOP);
        tool.add(profiler);

        assertThat(profiler.started).isFalse();
        assertThat(profiler.stopped).isFalse();

        tool.start();
        assertThat(profiler.started).isTrue();
        assertThat(profiler.stopped).isFalse();

        tool.stop();
        assertThat(profiler.started).isTrue();
        assertThat(profiler.stopped).isTrue();
    }

    @Test
    void shouldStartAndStopContinuousProfiler() {
        TestContinuousProfiler profiler = new TestContinuousProfiler(NOOP);
        tool.add(profiler);
        assertThat(profiler.started).isFalse();
        tool.start();
        assertEventually(() -> profiler.started.get(), TRUE, 1, MINUTES);
        tool.stop();
        assertEventually(() -> profiler.stopped.get(), TRUE, 1, MINUTES);
    }

    @Test
    void shouldStartAndStopPeriodicProfiler() {
        FakeClock clock = new FakeClock();
        Duration tick = Duration.ofSeconds(1);
        TestPeriodicProfiler profiler = new TestPeriodicProfiler(tick, clock, NOOP);
        tool.add(profiler);
        tool.start();
        assertEventually(() -> profiler.ticks.get(), equalityCondition(1), 1, MINUTES);
        clock.forward(tick);
        assertEventually(() -> profiler.ticks.get(), equalityCondition(2), 1, MINUTES);
        clock.forward(tick);
        assertEventually(() -> profiler.ticks.get(), equalityCondition(3), 1, MINUTES);

        // Profiler tick is "slow" won't yield more ticks
        clock.forward(tick.plus(tick).plus(tick));
        assertEventually(() -> profiler.ticks.get(), equalityCondition(4), 1, MINUTES);
    }

    @Test
    void shouldFailBasicProfiler() {
        TestProfiler profiler = new TestProfiler(THROW);
        tool.add(profiler);

        assertThat(profiler.failure()).isNull();
        tool.start();
        assertThat(profiler.failure()).isNotNull();
    }

    @Test
    void shouldFailContinuousProfiler() {
        TestContinuousProfiler profiler = new TestContinuousProfiler(THROW);
        tool.add(profiler);
        assertThat(profiler.failure()).isNull();
        tool.start();
        assertEventually(profiler::failure, instanceOf(RuntimeException.class), 1, MINUTES);
    }

    @Test
    void shouldFailPeriodicProfiler() {
        TestPeriodicProfiler profiler = new TestPeriodicProfiler(Duration.ofSeconds(1), new FakeClock(), THROW);
        tool.add(profiler);
        assertThat(profiler.failure()).isNull();
        tool.start();
        assertEventually(profiler::failure, instanceOf(RuntimeException.class), 1, MINUTES);
    }

    @Test
    void shouldReportRunningToolWithSomeFailedProfiler() {
        tool.add(new TestProfiler(THROW));
        tool.add(new TestProfiler(NOOP));
        tool.start();

        assertThat(tool.hasRunningProfilers()).isTrue();
        tool.stop();
        assertThat(tool.hasRunningProfilers()).isFalse();
    }

    @Test
    void shouldNotReportRunningToolWithAllFailedProfilers() {
        tool.add(new TestProfiler(THROW));
        tool.add(new TestProfiler(THROW));
        tool.start();

        assertThat(tool.hasRunningProfilers()).isFalse();
    }

    private static class TestProfiler extends Profiler {

        Runnable onStart;

        TestProfiler(Runnable onStart) {
            this.onStart = onStart;
        }

        AtomicBoolean started = new AtomicBoolean();
        AtomicBoolean stopped = new AtomicBoolean();

        @Override
        protected boolean available() {
            return true;
        }

        @Override
        protected void start() {
            started.set(true);
            onStart.run();
        }

        @Override
        protected void stop() {
            stopped.set(true);
        }
    }

    private static class TestContinuousProfiler extends ContinuousProfiler {
        AtomicBoolean started = new AtomicBoolean();
        AtomicBoolean stopped = new AtomicBoolean();

        Runnable onRun;

        TestContinuousProfiler(Runnable onRun) {
            this.onRun = onRun;
        }

        @Override
        protected void run(BooleanSupplier stopCondition) {
            started.set(true);
            onRun.run();
            while (!stopCondition.getAsBoolean()) {
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            stopped.set(true);
        }

        @Override
        protected boolean available() {
            return true;
        }
    }

    private static class TestPeriodicProfiler extends PeriodicProfiler {
        AtomicInteger ticks = new AtomicInteger();
        Runnable onTick;

        TestPeriodicProfiler(Duration interval, SystemNanoClock clock, Runnable onTick) {
            super(interval, clock);
            this.onTick = onTick;
        }

        @Override
        protected void tick() {
            ticks.incrementAndGet();
            onTick.run();
        }

        @Override
        protected boolean available() {
            return true;
        }
    }
}
