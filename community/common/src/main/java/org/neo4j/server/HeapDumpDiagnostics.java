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
package org.neo4j.server;

import static org.neo4j.util.FeatureToggles.flag;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * An object with information useful for debugging a heap dump.
 * This object is updated from the place where information is available.
 */
public class HeapDumpDiagnostics {
    public static final HeapDumpDiagnostics INSTANCE = new HeapDumpDiagnostics();
    private static final boolean STORE_DIAGNOSTICS = flag(HeapDumpDiagnostics.class, "ENABLED", true);
    public volatile String START_TIME;
    public volatile String NEO4J_VERSION;
    public final ConcurrentHashMap<String, String> DIAGNOSTICS = new ConcurrentHashMap<>();
    public volatile String SYSTEM_DIAGNOSTICS;

    public static void addDiagnostics(String database, String diagnostics) {
        if (STORE_DIAGNOSTICS) {
            if (Objects.equals(database, "")) {
                // Empty string is System diagnostics
                INSTANCE.SYSTEM_DIAGNOSTICS = cleanupDiagnostics(diagnostics);
            } else if (INSTANCE.DIAGNOSTICS.containsKey(database) || INSTANCE.DIAGNOSTICS.size() < 10) {
                INSTANCE.DIAGNOSTICS.put(database, cleanupDiagnostics(diagnostics));
            }
        }
    }

    private static String cleanupDiagnostics(String diagnostics) {
        // Save some memory by removing most of the redundant whitespace
        return diagnostics.replace("  ", "");
    }

    private HeapDumpDiagnostics() {}
}
