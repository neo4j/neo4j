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
package org.neo4j.commandline.dbms;

import static org.neo4j.kernel.diagnostics.DiagnosticsReportSources.newDiagnosticsString;
import static org.neo4j.kernel.diagnostics.DiagnosticsReportSources.newFailedDiagnosticsSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.Config;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.diagnostics.DiagnosticsAuthenticatedReportProvider;
import org.neo4j.kernel.diagnostics.DiagnosticsLiveConnection;
import org.neo4j.kernel.diagnostics.DiagnosticsReportSource;

/**
 * Authenticated classifier that captures the graph counts of each reported database by running
 * {@code CALL db.stats.retrieve('GRAPH COUNTS')} against the running DBMS, writing one JSON file per database into
 * the report.
 */
@ServiceProvider
public class GraphCountsAuthenticatedReportProvider extends DiagnosticsAuthenticatedReportProvider {
    static final String CLASSIFIER = "graphcounts";
    private static final String QUERY = "CALL db.stats.retrieve('GRAPH COUNTS')";

    private Set<String> databaseNames;

    public GraphCountsAuthenticatedReportProvider() {
        super(CLASSIFIER);
    }

    @Override
    public void init(FileSystemAbstraction fs, Config config, Set<String> databaseNames) {
        this.databaseNames = databaseNames;
    }

    @Override
    protected String describe(String classifier) {
        return "include the output of '" + QUERY + "' for each database (requires --username/--password)";
    }

    @Override
    protected Map<String, List<DiagnosticsReportSource>> provideSources(
            Set<String> classifiers, DiagnosticsLiveConnection connection) {
        if (!classifiers.contains(CLASSIFIER)) {
            return Map.of();
        }

        List<DiagnosticsReportSource> sources = new ArrayList<>();
        for (String databaseName : databaseNames) {
            if (databaseName.equals("system")) {
                continue;
            }
            var destination = String.format("databases/%s/%s.json", databaseName, CLASSIFIER);
            // Run eagerly while the connection is open and capture the result, so the source can be written later.
            final String content;
            try {
                content = JSON.writeValueAsString(
                        connection.execute(databaseName, QUERY).rows());
            } catch (Exception e) {
                String message = "ERROR: Failed to run '" + QUERY + "' against database '" + databaseName + "': "
                        + e.getMessage();
                sources.add(newFailedDiagnosticsSource(destination, message));
                continue;
            }
            sources.add(newDiagnosticsString(destination, () -> content));
        }
        return Map.of(CLASSIFIER, sources);
    }
}
