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
package org.neo4j.consistency;

import static org.neo4j.internal.helpers.MathUtil.clamp;
import static picocli.CommandLine.Help.Visibility.ALWAYS;
import static picocli.CommandLine.Help.Visibility.NEVER;

import java.nio.file.Path;
import org.neo4j.cli.Converters.MaxOffHeapMemoryConverter;
import org.neo4j.consistency.checking.ConsistencyFlags;
import picocli.CommandLine;
import picocli.CommandLine.Option;

public class ConsistencyCheckOptions {
    @Option(
            names = "--check-indexes",
            arity = "0..1",
            showDefaultValue = ALWAYS,
            paramLabel = "true|false",
            fallbackValue = "true",
            description = "Perform consistency checks on indexes.")
    private boolean checkIndexes = ConsistencyFlags.DEFAULT.checkIndexes();

    @Option(
            names = "--check-graph",
            arity = "0..1",
            showDefaultValue = NEVER, // manually handled,
            paramLabel = "true|false",
            fallbackValue = "true",
            description = "Perform consistency checks between nodes, relationships, properties, types, and tokens."
                    + "%n  Default: true")
    private Boolean checkGraph;

    private boolean checkGraph() {
        // if not explicitly enabled, then set via default or related checks
        return checkGraph == null
                ? ConsistencyFlags.DEFAULT.checkGraph() || checkCounts || checkPropertyOwners
                : checkGraph;
    }

    @Option(
            names = "--check-counts",
            arity = "0..1",
            showDefaultValue = NEVER, // manually handled
            paramLabel = "true|false",
            fallbackValue = "true",
            description = "Perform consistency checks on the counts. Requires <check-graph>, and may implicitly "
                    + "enable <check-graph> if it were not explicitly disabled."
                    + "%n  Default: <check-graph>")
    private boolean checkCounts = ConsistencyFlags.DEFAULT.checkCounts();

    @Option(
            names = "--check-property-owners",
            arity = "0..1",
            showDefaultValue = ALWAYS,
            paramLabel = "true|false",
            fallbackValue = "true",
            description = "Perform consistency checks on the ownership of properties. Requires <check-graph>, and "
                    + "may implicitly enable <check-graph> if it were not explicitly disabled.")
    private boolean checkPropertyOwners = ConsistencyFlags.DEFAULT.checkPropertyOwners();

    @Option(
            names = "--report-path",
            paramLabel = "<path>",
            showDefaultValue = ALWAYS,
            description = "Path to where a consistency report will be written. "
                    + "Interpreted as a directory, unless it has an extension of '.report'.")
    private Path reportPath = Path.of(".");

    @Option(
            names = "--max-off-heap-memory",
            paramLabel = "<size>",
            showDefaultValue = ALWAYS,
            defaultValue = "90%",
            converter = MaxOffHeapMemoryConverter.class,
            description = "Maximum memory that neo4j-admin can use for page cache and various caching data structures "
                    + "to improve performance. Value can be plain numbers, "
                    + "like 10000000 or e.g. 20G for 20 gigabytes, or even e.g. 70%%, which will amount to 70%%"
                    + " of currently free memory on the machine.")
    private long maxOffHeapMemory;

    @Option(
            names = "--threads",
            paramLabel = "<number of threads>",
            defaultValue = "all",
            showDefaultValue = NEVER,
            converter = NumberOfThreadsConverter.class,
            description =
                    "Number of threads used to check the consistency. Default: The number of CPUs on the machine.")
    private int numberOfThreads;

    public Path reportPath() {
        return reportPath.normalize();
    }

    private IllegalArgumentException validateGraphOptions() {
        if (checkGraph == null || checkGraph) {
            return null;
        }

        final var sb = new StringBuilder();
        final var error = "<%%s> cannot be %%s if <%s> is explicitly set to %s".formatted("check-graph", false);

        if (checkCounts) {
            sb.append(error.formatted("check-counts", true));
        }
        if (checkPropertyOwners) {
            sb.append(error.formatted("check-property-owners", true));
        }

        return !sb.isEmpty() ? new IllegalArgumentException(sb.toString()) : null;
    }

    public long maxOffHeapMemory() {
        return maxOffHeapMemory;
    }

    public int numberOfThreads() {
        return numberOfThreads;
    }

    public ConsistencyFlags toFlags() {
        return toFlags(false);
    }

    public ConsistencyFlags toFlags(boolean force) {
        final var invalidGraphOptions = validateGraphOptions();
        if (!force && invalidGraphOptions != null) {
            throw invalidGraphOptions;
        }

        final var checkGraphForce = force || checkGraph();
        final var checkCountsForce = checkGraphForce && checkCounts;
        final var checkPropertyOwnersForce = checkGraphForce && checkPropertyOwners;
        return new ConsistencyFlags(checkIndexes, checkGraphForce, checkCountsForce, checkPropertyOwnersForce);
    }

    private static class NumberOfThreadsConverter implements CommandLine.ITypeConverter<Integer> {
        @Override
        public Integer convert(String value) throws Exception {
            if ("all".equals(value)) {
                return Runtime.getRuntime().availableProcessors();
            }
            return clamp(Integer.parseInt(value), 1, Runtime.getRuntime().availableProcessors());
        }
    }
}
