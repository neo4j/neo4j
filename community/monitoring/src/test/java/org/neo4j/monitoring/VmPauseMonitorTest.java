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
package org.neo4j.monitoring;

import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.neo4j.monitoring.VmPauseMonitor.Monitor.EMPTY;
import static org.neo4j.scheduler.JobMonitoringParams.NOT_MONITORED;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.neo4j.monitoring.VmPauseMonitor.VmPauseInfo;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobHandle;
import org.neo4j.scheduler.JobScheduler;

class VmPauseMonitorTest {
    private final VmPauseMonitor.Monitor monitor = Mockito.mock(VmPauseMonitor.Monitor.class);
    private final JobHandle<?> jobHandle = Mockito.mock(JobHandle.class);
    private final JobScheduler jobScheduler = Mockito.mock(JobScheduler.class);
    private final VmPauseMonitor vmPauseMonitor =
            Mockito.spy(new VmPauseMonitor(ofMillis(1), ofMillis(0), monitor, jobScheduler));

    @BeforeEach
    void setUp() {
        Mockito.doReturn(jobHandle)
                .when(jobScheduler)
                .schedule(any(Group.class), eq(NOT_MONITORED), any(Runnable.class));
    }

    @Test
    void testCtorParametersValidation() {
        assertThrows(
                NullPointerException.class, () -> new VmPauseMonitor(ofSeconds(1), ofSeconds(1), null, jobScheduler));
        assertThrows(NullPointerException.class, () -> new VmPauseMonitor(ofSeconds(1), ofSeconds(1), EMPTY, null));
        assertThrows(
                IllegalArgumentException.class,
                () -> new VmPauseMonitor(ofSeconds(0), ofSeconds(1), EMPTY, jobScheduler));
        assertThrows(
                IllegalArgumentException.class,
                () -> new VmPauseMonitor(ofSeconds(1), ofSeconds(-1), EMPTY, jobScheduler));
        assertThrows(
                IllegalArgumentException.class,
                () -> new VmPauseMonitor(ofSeconds(-1), ofSeconds(1), EMPTY, jobScheduler));
    }

    @Test
    void testStartAndStop() {
        vmPauseMonitor.start();
        vmPauseMonitor.stop();

        verify(jobScheduler).schedule(any(Group.class), eq(NOT_MONITORED), any(Runnable.class));
        verify(jobHandle).cancel();
    }

    @Test
    void testRestart() {
        vmPauseMonitor.start();
        vmPauseMonitor.stop();
        vmPauseMonitor.start();

        verify(jobScheduler, Mockito.times(2)).schedule(any(Group.class), eq(NOT_MONITORED), any(Runnable.class));
        verify(jobHandle).cancel();
    }

    @Test
    void testFailStopWithoutStart() {
        assertThrows(IllegalStateException.class, vmPauseMonitor::stop);
    }

    @Test
    void testFailOnDoubleStart() {
        assertThrows(IllegalStateException.class, () -> {
            vmPauseMonitor.start();
            vmPauseMonitor.start();
        });
    }

    @Test
    void testFailOnDoubleStop() {
        assertThrows(IllegalStateException.class, () -> {
            vmPauseMonitor.start();
            vmPauseMonitor.stop();
            vmPauseMonitor.stop();
        });
    }

    @Test
    void testNotifyListener() throws Exception {
        Mockito.doReturn(false, true).when(vmPauseMonitor).isStopped();
        vmPauseMonitor.monitor();
        Mockito.verify(monitor).pauseDetected(any(VmPauseInfo.class));
    }
}
