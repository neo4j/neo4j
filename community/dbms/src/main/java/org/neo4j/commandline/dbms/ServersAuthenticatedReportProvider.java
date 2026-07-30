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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
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
 * Authenticated classifier that captures the servers of the DBMS by running {@code SHOW SERVERS YIELD *} against the running
 * instance, writing the result as a single JSON file into the report. {@code SHOW SERVERS} is a DBMS-level procedure,
 * so it is run once against the {@code system} database rather than per database.
 */
@ServiceProvider
public class ServersAuthenticatedReportProvider extends DiagnosticsAuthenticatedReportProvider {
    static final String CLASSIFIER = "servers";
    private static final String QUERY = "SHOW SERVERS YIELD *";
    private static final String SYSTEM_DATABASE = "system";
    private static final ObjectWriter JSON = new ObjectMapper().writerWithDefaultPrettyPrinter();

    public ServersAuthenticatedReportProvider() {
        super(CLASSIFIER);
    }

    @Override
    public void init(FileSystemAbstraction fs, Config config, Set<String> databaseNames) {}

    @Override
    protected String describe(String classifier) {
        return "include the output of '" + QUERY + "' (requires --username/--password)";
    }

    @Override
    protected Map<String, List<DiagnosticsReportSource>> provideSources(
            Set<String> classifiers, DiagnosticsLiveConnection connection) {
        if (!classifiers.contains(CLASSIFIER)) {
            return Map.of();
        }

        String destination = CLASSIFIER + ".json";
        // Run eagerly while the connection is open and capture the result, so the source can be written later.
        final String content;
        try {
            content = JSON.writeValueAsString(
                    connection.execute(SYSTEM_DATABASE, QUERY).rows());
        } catch (Exception e) {
            String message = "Failed to run '" + QUERY + "': " + e.getMessage();
            return Map.of(CLASSIFIER, List.of(newDiagnosticsString(destination, () -> message)));
        }
        return Map.of(CLASSIFIER, List.of(newDiagnosticsString(destination, () -> content)));
    }
}
