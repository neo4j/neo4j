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
package org.neo4j.diagnostics;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.info.DiagnosticsPhase;
import org.neo4j.kernel.internal.KernelDiagnostics;
import org.neo4j.logging.BufferingLog;

import static org.neo4j.diagnostics.DiagnosticsReportSources.newDiagnosticsFile;
import static org.neo4j.diagnostics.DiagnosticsReportSources.newDiagnosticsRotatingFile;
import static org.neo4j.diagnostics.DiagnosticsReportSources.newDiagnosticsString;

public class KernelDiagnosticsOfflineReportProvider extends DiagnosticsOfflineReportProvider
{
    private FileSystemAbstraction fs;
    private Config config;
    private File storeDirectory;

    public KernelDiagnosticsOfflineReportProvider()
    {
        super( "kernel", "logs", "plugins", "tree", "tx" );
    }

    @Override
    public void init( FileSystemAbstraction fs, Config config, File storeDirectory )
    {
        this.fs = fs;
        this.config = config;
        this.storeDirectory = storeDirectory;
    }

    @Override
    protected List<DiagnosticsReportSource> provideSources( Set<String> classifiers )
    {
        List<DiagnosticsReportSource> sources = new ArrayList<>();
        if ( classifiers.contains( "logs" ) )
        {
            getLogFiles( sources );
        }
        if ( classifiers.contains( "plugins" ) )
        {
            listPlugins( sources );
        }
        if ( classifiers.contains( "tree" ) )
        {
            listDataDirectory( sources );
        }
        if ( classifiers.contains( "tx" ) )
        {
            getTransactionLogFiles( sources );
        }

        return sources;
    }

    /**
     * Collect a list of all the files in the plugins directory.
     *
     * @param sources destination of the sources.
     */
    private void listPlugins( List<DiagnosticsReportSource> sources )
    {
        File pluginDirectory = config.get( GraphDatabaseSettings.plugin_dir );
        if ( fs.fileExists( pluginDirectory ) )
        {
            StringBuilder sb = new StringBuilder();
            sb.append( "List of plugin directory:" ).append( System.lineSeparator() );
            listContentOfDirectory( pluginDirectory, "  ", sb );

            sources.add( newDiagnosticsString( "plugins.txt", sb::toString ) );
        }
    }

    private void listContentOfDirectory( File directory, String prefix, StringBuilder sb )
    {
        if ( !fs.isDirectory( directory ) )
        {
            return;
        }

        File[] files = fs.listFiles( directory );
        for ( File file : files )
        {
            if ( fs.isDirectory( file ) )
            {
                listContentOfDirectory( file, prefix + File.separator + file.getName(), sb );
            }
            else
            {
                sb.append( prefix ).append( file.getName() ).append( System.lineSeparator() );
            }
        }
    }

    /**
     * Print a tree view of all the files in the graph.db directory with files sizes.
     *
     * @param sources destination of the sources.
     */
    private void listDataDirectory( List<DiagnosticsReportSource> sources )
    {
        KernelDiagnostics.StoreFiles storeFiles = new KernelDiagnostics.StoreFiles( storeDirectory );

        BufferingLog logger = new BufferingLog();
        storeFiles.dump( DiagnosticsPhase.INITIALIZED, logger.debugLogger() );

        sources.add( newDiagnosticsString( "tree.txt", logger::toString ) );
    }

    /**
     * Add {@code debug.log}, {@code neo4j.log} and {@code gc.log}. All with all available rotated files.
     *
     * @param sources destination of the sources.
     */
    private void getLogFiles( List<DiagnosticsReportSource> sources )
    {
        // debug.log
        File debugLogFile = config.get( GraphDatabaseSettings.store_internal_log_path );
        if ( fs.fileExists( debugLogFile ) )
        {
            sources.addAll( newDiagnosticsRotatingFile( "logs/debug.log", fs, debugLogFile ) );
        }

        // neo4j.log
        File logDirectory = config.get( GraphDatabaseSettings.logs_directory );
        File neo4jLog = new File( logDirectory, "neo4j.log" );
        if ( fs.fileExists( neo4jLog ) )
        {
            sources.add( newDiagnosticsFile( "logs/neo4j.log", fs, neo4jLog ) );
        }

        // gc.log
        File gcLog = new File( logDirectory, "gc.log" );
        if ( fs.fileExists( gcLog ) )
        {
            sources.add( newDiagnosticsFile( "logs/gc.log", fs, gcLog ) );
        }
        // we might have rotation activated, check
        int i = 0;
        while ( true )
        {
            File gcRotationLog = new File( logDirectory, "gc.log." + i );
            if ( !fs.fileExists( gcRotationLog ) )
            {
                break;
            }
            sources.add( newDiagnosticsFile( "logs/gc.log." + i, fs, gcRotationLog ) );
            i++;
        }
        // there are other rotation schemas but nothing we can predict...
    }

    /**
     * Add all available log files as sources.
     *
     * @param sources destination of the sources.
     */
    private void getTransactionLogFiles( List<DiagnosticsReportSource> sources )
    {
        try
        {
            LogFiles logFiles = LogFilesBuilder.logFilesBasedOnlyBuilder( storeDirectory, fs ).build();
            for ( File file : logFiles.logFiles() )
            {
                sources.add( DiagnosticsReportSources.newDiagnosticsFile( "tx/" + file.getName(), fs, file ) );
            }
        }
        catch ( IOException e )
        {
            sources.add( DiagnosticsReportSources
                    .newDiagnosticsString( "tx.txt", () -> "Error getting tx logs: " + e.getMessage() ) );
        }
    }

}
