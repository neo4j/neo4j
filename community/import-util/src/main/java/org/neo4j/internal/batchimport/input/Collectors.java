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
package org.neo4j.internal.batchimport.input;

import static java.io.OutputStream.nullOutputStream;
import static org.neo4j.internal.batchimport.input.BadCollector.BAD_NODES;
import static org.neo4j.internal.batchimport.input.BadCollector.BAD_RELATIONSHIPS;
import static org.neo4j.internal.batchimport.input.BadCollector.COLLECT_ALL;
import static org.neo4j.internal.batchimport.input.BadCollector.DEFAULT_BACK_PRESSURE_THRESHOLD;
import static org.neo4j.internal.batchimport.input.BadCollector.EXTRA_COLUMNS;
import static org.neo4j.internal.batchimport.input.BadCollector.NO_MONITOR;

import java.io.OutputStream;
import java.util.function.Function;

/**
 * Common implementations of {@link Collector}
 */
public class Collectors {
    private Collectors() {}

    public static Collector silentBadCollector(long tolerance) {
        return silentBadCollector(tolerance, COLLECT_ALL);
    }

    public static Collector silentBadCollector(long tolerance, int collect) {
        return badCollector(nullOutputStream(), tolerance, collect);
    }

    public static Collector badCollector(OutputStream out, long tolerance) {
        return badCollector(out, tolerance, COLLECT_ALL, false);
    }

    public static Collector badCollector(OutputStream out, long tolerance, int collect) {
        return new BadCollector(out, tolerance, collect, DEFAULT_BACK_PRESSURE_THRESHOLD, false, NO_MONITOR);
    }

    public static Collector badCollector(OutputStream out, long tolerance, int collect, boolean skipBadEntriesLogging) {
        return new BadCollector(
                out, tolerance, collect, DEFAULT_BACK_PRESSURE_THRESHOLD, skipBadEntriesLogging, NO_MONITOR);
    }

    public static Function<OutputStream, Collector> badCollector(final int tolerance) {
        return badCollector(tolerance, COLLECT_ALL);
    }

    public static Function<OutputStream, Collector> badCollector(final int tolerance, final int collect) {
        return out -> badCollector(out, tolerance, collect, false);
    }

    public static int collect(boolean skipBadRelationships, boolean skipDuplicateNodes, boolean ignoreExtraColumns) {
        return (skipBadRelationships ? BAD_RELATIONSHIPS : 0)
                // for now, we use the skipDuplicateNodes for both duplicate and violating nodes
                // We probably need to split this into multiple ones
                | (skipDuplicateNodes ? BAD_NODES : 0)
                | (ignoreExtraColumns ? EXTRA_COLUMNS : 0);
    }
}
