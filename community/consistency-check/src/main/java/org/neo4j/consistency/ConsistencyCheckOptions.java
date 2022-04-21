/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.consistency;

import static picocli.CommandLine.Help.Visibility.ALWAYS;

import java.nio.file.Path;
import picocli.CommandLine.Option;

@SuppressWarnings("FieldMayBeFinal")
public class ConsistencyCheckOptions {
    @Option(
            names = "--report-dir",
            paramLabel = "<path>",
            description = "Directory where consistency report will be written.",
            defaultValue = ".")
    private Path reportDir;

    @Option(
            names = "--check-graph",
            arity = "1",
            showDefaultValue = ALWAYS,
            paramLabel = "<true/false>",
            description = "Perform consistency checks between nodes, relationships, properties, types and tokens.")
    private boolean checkGraph = true;

    @Option(
            names = "--check-indexes",
            arity = "1",
            showDefaultValue = ALWAYS,
            paramLabel = "<true/false>",
            description = "Perform consistency checks on indexes.")
    private boolean checkIndexes = true;

    @Option(
            names = "--check-index-structure",
            arity = "1",
            showDefaultValue = ALWAYS,
            paramLabel = "<true/false>",
            description = "Perform structure checks on indexes.")
    private boolean checkIndexStructure = true;

    public Path getReportDir() {
        return reportDir;
    }

    public boolean isCheckGraph() {
        return checkGraph;
    }

    public boolean isCheckIndexes() {
        return checkIndexes;
    }

    public boolean isCheckIndexStructure() {
        return checkIndexStructure;
    }
}
