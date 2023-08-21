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
package org.neo4j.storemigration;

import java.util.HashMap;
import org.neo4j.internal.kernel.api.IndexMonitor;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.monitoring.Monitors;

/**
 * Record the {@link InternalIndexState initial state} for each index in target database.
 */
public class InitialIndexStateMonitor extends IndexMonitor.MonitorAdapter {
    private final String targetDatabase;
    public HashMap<IndexDescriptor, InternalIndexState> allIndexStates = new HashMap<>();

    public InitialIndexStateMonitor(String targetDatabase) {
        this.targetDatabase = targetDatabase;
    }

    @Override
    public void initialState(String databaseName, IndexDescriptor descriptor, InternalIndexState state) {
        if (databaseName.equals(targetDatabase)) {
            allIndexStates.put(descriptor, state);
        }
    }

    /**
     * Convenience method.
     * @return {@link Monitors} instance, initialised with this monitor.
     */
    public Monitors monitors() {
        var monitors = new Monitors();
        monitors.addMonitorListener(this);
        return monitors;
    }
}
