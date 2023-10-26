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
package org.neo4j.server.diagnostics;

import static org.neo4j.kernel.diagnostics.DiagnosticsReportSources.newDiagnosticsRotatingFile;
import static org.neo4j.logging.log4j.LogConfig.HTTP_LOG;

import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.diagnostics.DiagnosticsOfflineReportProvider;
import org.neo4j.kernel.diagnostics.DiagnosticsReportSource;

@ServiceProvider
public class ServerDiagnosticsOfflineReportProvider extends DiagnosticsOfflineReportProvider {
    private FileSystemAbstraction fs;
    private Config config;

    public ServerDiagnosticsOfflineReportProvider() {
        super("logs");
    }

    @Override
    public void init(FileSystemAbstraction fs, Config config, Set<String> databaseNames) {
        this.fs = fs;
        this.config = config;
    }

    @Override
    protected List<DiagnosticsReportSource> provideSources(Set<String> classifiers) {
        if (classifiers.contains("logs")) {
            Path httpLog = config.get(GraphDatabaseSettings.logs_directory).resolve(HTTP_LOG);
            if (fs.fileExists(httpLog)) {
                return newDiagnosticsRotatingFile("logs/", fs, httpLog);
            }
        }
        return Collections.emptyList();
    }
}
