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
package org.neo4j.fleetmanagement.diagnostics;

import java.nio.file.Path;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.commandline.dbms.DiagnosticsReportGenerator;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.dbms.database.DatabaseContextProvider;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.diagnostics.DiagnosticsLiveConnection;
import org.neo4j.kernel.diagnostics.DiagnosticsReporterProgress;
import org.neo4j.kernel.diagnostics.EmbeddedDiagnosticsLiveConnection;
import org.neo4j.logging.internal.LogService;

/**
 * Fleet management service that produces a diagnostics report about the local instance. Authenticated classifiers
 * (indexes, graph counts, databases, servers) are collected through an {@link EmbeddedDiagnosticsLiveConnection}, which
 * runs the queries directly against the embedded databases - so, unlike the command-line {@code report} tool, no Bolt
 * connection or credentials are needed.
 */
public class DiagnosticsService {
    private static final Set<String> REPORT_CLASSIFIERS = Set.of(
            "logs",
            "config",
            "plugins",
            "tree",
            "metrics",
            "ps",
            "version",
            "databases",
            "servers",
            "indexes",
            "graphcounts");

    private final LogService logService;
    private final Config config;
    private final FileSystemAbstraction fs;
    private final DatabaseManagementService databaseManagementService;
    private final DatabaseContextProvider<DatabaseContext> databaseContextProvider;

    public DiagnosticsService(
            LogService logService,
            Config config,
            FileSystemAbstraction fs,
            DatabaseManagementService databaseManagementService,
            DatabaseContextProvider<DatabaseContext> databaseContextProvider) {
        this.logService = logService;
        this.config = config;
        this.fs = fs;
        this.databaseManagementService = databaseManagementService;
        this.databaseContextProvider = databaseContextProvider;
    }

    public void generateReport() {
        var log = logService.getUserLog(DiagnosticsReportGenerator.class);
        log.info("Generating diagnostics report...");
        try {
            Path homeDir = config.get(GraphDatabaseSettings.neo4j_home);
            Path confDir = config.get(GraphDatabaseSettings.configuration_directory);
            var ctx = new ExecutionContext(homeDir, confDir);
            DiagnosticsReportGenerator generator = new DiagnosticsReportGenerator(ctx, config, false);

            Path reportDir = config.get(GraphDatabaseSettings.logs_directory).resolve("reports");
            if (!fs.isDirectory(reportDir)) {
                fs.mkdirs(reportDir);
            }

            Set<String> dbNames = databaseContextProvider.registeredDatabases().keySet().stream()
                    .map(NamedDatabaseId::name)
                    .collect(Collectors.toCollection(TreeSet::new));
            String defaultDatabase = config.get(GraphDatabaseSettings.initial_default_database);

            try (DiagnosticsLiveConnection connection =
                    new EmbeddedDiagnosticsLiveConnection(databaseManagementService, defaultDatabase)) {
                generator.generate(
                        REPORT_CLASSIFIERS, reportDir, DiagnosticsReporterProgress.EMPTY, true, dbNames, connection);
            }

            log.info("Diagnostics report generation completed. Check the reports directory under logs.");
        } catch (Exception e) {
            log.error("Failed to generate diagnostics report", e);
        }
    }
}
