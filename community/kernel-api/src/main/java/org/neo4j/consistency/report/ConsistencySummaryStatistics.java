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
package org.neo4j.consistency.report;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class ConsistencySummaryStatistics {
    private final Map<String, AtomicInteger> inconsistenciesPerType = new ConcurrentHashMap<>();
    private final AtomicLong errorCount = new AtomicLong();
    private final AtomicLong warningCount = new AtomicLong();
    private final List<String> genericErrors = new CopyOnWriteArrayList<>();

    @Override
    public String toString() {
        final var result = new StringBuilder(getClass().getSimpleName())
                .append(" {")
                .append("%n\tNumber of errors: ")
                .append(errorCount)
                .append("%n\tNumber of warnings: ")
                .append(warningCount);
        for (final var entry : inconsistenciesPerType.entrySet()) {
            if (entry.getValue().get() != 0) {
                result.append("%n\tNumber of inconsistent ")
                        .append(entry.getKey())
                        .append(" records: ")
                        .append(entry.getValue());
            }
        }
        if (!genericErrors.isEmpty()) {
            result.append("%n\tGeneric errors: ");
            genericErrors.forEach(message -> result.append("%n\t\t").append(message));
        }
        return result.append("%n}").toString().formatted();
    }

    public boolean isConsistent() {
        return getTotalInconsistencyCount() == 0;
    }

    public int getInconsistencyCountForRecordType(String type) {
        final var count = inconsistenciesPerType.get(type);
        return count != null ? count.get() : 0;
    }

    public long getTotalInconsistencyCount() {
        return errorCount.get() - genericErrors.size();
    }

    public long getTotalWarningCount() {
        return warningCount.get();
    }

    public List<String> getGenericErrors() {
        return Collections.unmodifiableList(genericErrors);
    }

    public void update(String type, int errors, int warnings) {
        if (errors > 0) {
            inconsistenciesPerType
                    .computeIfAbsent(type, t -> new AtomicInteger())
                    .addAndGet(errors);
            errorCount.addAndGet(errors);
        }
        if (warnings > 0) {
            warningCount.addAndGet(warnings);
        }
    }

    public void genericError(String message) {
        errorCount.incrementAndGet();
        genericErrors.add(message);
    }
}
