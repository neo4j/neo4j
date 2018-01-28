/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.metrics.diagnostics;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.neo4j.diagnostics.DiagnosticsOfflineReportProvider;
import org.neo4j.diagnostics.DiagnosticsReportSource;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.metrics.MetricsSettings;

import static org.neo4j.diagnostics.DiagnosticsReportSources.newDiagnosticsFile;

public class MetricsDiagnosticsOfflineReportProvider extends DiagnosticsOfflineReportProvider
{
    private FileSystemAbstraction fs;
    private Config config;

    public MetricsDiagnosticsOfflineReportProvider()
    {
        super( "metrics", "metrics" );
    }

    @Override
    public void init( FileSystemAbstraction fs, Config config, File storeDirectory )
    {
        this.fs = fs;
        this.config = config;
    }

    @Override
    protected List<DiagnosticsReportSource> provideSources( Set<String> classifiers )
    {
        File metricsDirectory = config.get( MetricsSettings.csvPath );
        if ( fs.fileExists( metricsDirectory ) && fs.isDirectory( metricsDirectory ) )
        {
            List<DiagnosticsReportSource> files = new ArrayList<>();
            for ( File file : fs.listFiles( metricsDirectory ) )
            {
                files.add( newDiagnosticsFile( "metrics/" + file.getName(), fs, file ) );
            }
            return files;
        }
        return Collections.emptyList();
    }
}
