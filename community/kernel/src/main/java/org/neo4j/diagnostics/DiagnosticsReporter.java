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
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.neo4j.helpers.Format;
import org.neo4j.helpers.Service;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;

public class DiagnosticsReporter
{
    private final List<DiagnosticsOfflineReportProvider> providers = new ArrayList<>();
    private final Set<String> availableClassifiers = new TreeSet<>();
    private final Map<String,List<DiagnosticsReportSource>> additionalSources = new HashMap<>();

    public void registerOfflineProvider( DiagnosticsOfflineReportProvider provider )
    {
        providers.add( provider );
        availableClassifiers.addAll( provider.getFilterClassifiers() );
    }

    public void registerSource( String classifier, DiagnosticsReportSource source )
    {
        availableClassifiers.add( classifier );
        additionalSources.computeIfAbsent( classifier, c -> new ArrayList<>() ).add( source );
    }

    public void dump( Set<String> classifiers, Path destination, DiagnosticsReporterProgress progress, boolean force ) throws IOException
    {
        // Collect sources
        List<DiagnosticsReportSource> sources = new ArrayList<>();
        for ( DiagnosticsOfflineReportProvider provider : providers )
        {
            sources.addAll( provider.getDiagnosticsSources( classifiers ) );
        }

        // Add additional sources
        for ( Map.Entry<String,List<DiagnosticsReportSource>> classifier : additionalSources.entrySet() )
        {
            if ( classifiers.contains( "all" ) || classifiers.contains( classifier.getKey() ) )
            {
                sources.addAll( classifier.getValue() );
            }
        }

        // Make sure target directory exists
        Path destinationFolder = destination.getParent();
        Files.createDirectories( destinationFolder );

        // Estimate an upper bound of the final size and make sure it will fit, if not, end reporting
        estimateSizeAndCheckAvailableDiskSpace( destination, progress, sources, destinationFolder, force );

        // Compress all files to destination
        Map<String, Object> env = new HashMap<>();
        env.put( "create", "true" );
        env.put( "useTempFile", Boolean.TRUE );

        // NOTE: we need the toUri() in order to handle windows file paths
        URI uri = URI.create("jar:file:" + destination.toAbsolutePath().toUri().getRawPath() );

        try ( FileSystem fs = FileSystems.newFileSystem( uri, env ) )
        {
            progress.setTotalSteps( sources.size() );
            for ( int i = 0; i < sources.size(); i++ )
            {
                DiagnosticsReportSource source = sources.get( i );
                Path path = fs.getPath( source.destinationPath() );
                if ( path.getParent() != null )
                {
                    Files.createDirectories( path.getParent() );
                }

                progress.started( i + 1, path.toString() );
                try
                {
                    source.addToArchive( path, progress );
                }
                catch ( Throwable e )
                {
                    progress.error( "Step failed", e );
                    continue;
                }
                progress.finished();
            }
        }
    }

    private void estimateSizeAndCheckAvailableDiskSpace( Path destination,
            DiagnosticsReporterProgress progress, List<DiagnosticsReportSource> sources,
            Path destinationFolder, boolean force ) throws IOException
    {
        if ( force )
        {
            return;
        }

        long estimatedFinalSize = 0;
        for ( DiagnosticsReportSource source  : sources )
        {
            estimatedFinalSize += source.estimatedSize( progress );
        }

        long freeSpace = destinationFolder.toFile().getFreeSpace();
        if ( estimatedFinalSize > freeSpace )
        {
            String message = String.format(
                    "Free available disk space for %s is %s, worst case estimate is %s. To ignore add '--force' to the command.",
                    destination.getFileName(), Format.bytes( freeSpace ), Format.bytes( estimatedFinalSize ) );
            throw new RuntimeException( message );
        }
    }

    public Set<String> getAvailableClassifiers()
    {
        return availableClassifiers;
    }

    public void registerAllOfflineProviders( Config config, File storeDirectory, FileSystemAbstraction fs )
    {
        for ( DiagnosticsOfflineReportProvider provider : Service.load( DiagnosticsOfflineReportProvider.class ) )
        {
            provider.init( fs, config, storeDirectory );
            registerOfflineProvider( provider );
        }
    }
}
