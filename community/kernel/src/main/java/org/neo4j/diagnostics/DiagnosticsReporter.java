/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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

import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;


public class DiagnosticsReporter
{
    private final List<DiagnosticsOfflineReportProvider> providers = new ArrayList<>();
    private final Set<String> availableClassifiers = new TreeSet<>();
    private final Map<String,List<DiagnosticsReportSource>> additionalSources = new HashMap<>();
    private final PrintStream out;

    public DiagnosticsReporter( PrintStream out )
    {
        this.out = out;
    }

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

    public void dump( Set<String> classifiers, Path destination ) throws IOException
    {
        // Collect sources
        List<DiagnosticsReportSource> sources = new ArrayList<>();
        for ( DiagnosticsOfflineReportProvider provider : providers )
        {
            sources.addAll( provider.getDiagnosticsSources( classifiers ) );
        }

        // Add additional sources
        for ( String classifier : additionalSources.keySet() )
        {
            if ( classifiers.contains( "all" ) || classifiers.contains( classifier ) )
            {
                sources.addAll( additionalSources.get( classifier ) );
            }
        }

        // Make sure target directory exists
        Files.createDirectories( destination.getParent() );

        // Compress all files to destination
        Map<String,String> env = new HashMap<>();
        env.put( "create", "true" );

        URI uri = URI.create("jar:file:" + destination.toAbsolutePath());

        try ( FileSystem fs = FileSystems.newFileSystem( uri, env ) )
        {
            for ( int i = 0; i < sources.size(); i++ )
            {
                DiagnosticsReportSource source = sources.get( i );
                Path path = fs.getPath( source.destinationPath() );
                if ( path.getParent() != null )
                {
                    Files.createDirectories( path.getParent() );
                }

                Mon monitor = new Mon( (i + 1) + "/" + sources.size(), "  " + path, out );
                monitor.started();
                source.addToArchive( path, monitor );
                monitor.finished();
            }
        }
    }

    public Set<String> getAvailableClassifiers()
    {
        return availableClassifiers;
    }

    // TODO:
    private static class Mon implements DiagnosticsReporterProgressCallback
    {
        private final String prefix;
        private final String suffix;
        private final PrintStream out;
        private String info = "";
        private int longestInfo;

        private Mon( String prefix, String suffix, PrintStream out )
        {
            this.prefix = prefix;
            this.suffix = suffix;
            this.out = out;
        }

        @Override
        public void percentChanged( int percent )
        {
            out.print( String.format( "\r%8s [", prefix ) );
            int totalWidth = 20;

            int numBars = totalWidth * percent / 100;
            for ( int i = 0; i < totalWidth; i++ )
            {
                if ( i < numBars )
                {
                    out.print( '#' );
                }
                else
                {
                    out.print( ' ' );
                }
            }
            out.print( String.format( "] %3s%% %s %s", percent, suffix, info ) );
        }

        @Override
        public void started()
        {
            percentChanged( 0 );
        }

        @Override
        public void finished()
        {
            // Pad string to erase info string
            info = String.join( "", Collections.nCopies( longestInfo, " " ) );

            percentChanged( 100 );
            out.println();
        }

        @Override
        public void info( String info )
        {
            this.info = info;
            if ( info.length() > longestInfo )
            {
                longestInfo = info.length();
            }
        }
    }
}
