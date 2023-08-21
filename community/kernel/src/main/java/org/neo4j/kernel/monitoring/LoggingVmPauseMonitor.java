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
package org.neo4j.kernel.monitoring;

import static java.util.Objects.requireNonNull;

import org.neo4j.logging.InternalLog;
import org.neo4j.monitoring.VmPauseMonitor;

public class LoggingVmPauseMonitor implements VmPauseMonitor.Monitor {
    private final InternalLog log;

    public LoggingVmPauseMonitor(InternalLog log) {
        this.log = requireNonNull(log);
    }

    @Override
    public void started() {
        log.debug("Starting VM pause monitor");
    }

    @Override
    public void stopped() {
        log.debug("Stopping VM pause monitor");
    }

    @Override
    public void interrupted() {
        log.debug("VM pause monitor stopped");
    }

    @Override
    public void failed(Exception e) {
        log.debug("VM pause monitor failed", e);
    }

    @Override
    public void pauseDetected(VmPauseMonitor.VmPauseInfo info) {
        log.warn("Detected VM stop-the-world pause: %s", info);
    }
}
