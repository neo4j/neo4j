/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.bolt.diagnostics;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.neo4j.diagnostics.DiagnosticsOfflineReportProvider;
import org.neo4j.diagnostics.DiagnosticsReportSource;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;

import static org.neo4j.diagnostics.DiagnosticsReportSources.newDiagnosticsRotatingFile;

public class BoltDiagnosticsOfflineReportProvider extends DiagnosticsOfflineReportProvider
{
    private FileSystemAbstraction fs;
    private Config config;

    public BoltDiagnosticsOfflineReportProvider()
    {
        super( "bolt", "logs" );
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
        if ( classifiers.contains( "logs" ) )
        {
            File boltLogFile = config.get( GraphDatabaseSettings.bolt_log_filename );
            if ( fs.fileExists( boltLogFile ) )
            {
                return newDiagnosticsRotatingFile( "logs/bolt.log", fs, boltLogFile );
            }
        }
        return Collections.emptyList();
    }
}
