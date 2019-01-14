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
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.neo4j.helpers.Service;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;

/**
 * Base class for a provider of offline reports. Offline reports does not require a running instance of the database
 * and is intended to be use as a way to gather information even if the database cannot be started. All implementing
 * classes is service loaded and initialized through the
 * {@link DiagnosticsOfflineReportProvider#init(FileSystemAbstraction, Config, File)} method.
 */
public abstract class DiagnosticsOfflineReportProvider extends Service
{
    private final Set<String> filterClassifiers;

    /**
     * A provider needs to know all the available classifiers in advance. A classifier is a group in the context of
     * diagnostics reporting, e.g. 'logs', 'config' or 'threaddump'.
     *
     * @param identifier a unique identifier for tagging during service loading, used for useful debug information.
     * @param classifier one
     * @param classifiers or more classifiers have to be provided.
     */
    protected DiagnosticsOfflineReportProvider( String identifier, String classifier, String... classifiers )
    {
        super( identifier );
        filterClassifiers = new HashSet<>( Arrays.asList( classifiers ) );
        filterClassifiers.add( classifier );
    }

    /**
     * Called after service loading to initialize the class.
     *
     * @param fs filesystem to use for file access.
     * @param config configuration file in use.
     * @param storeDirectory directory of the database files.
     */
    public abstract void init( FileSystemAbstraction fs, Config config, File storeDirectory );

    /**
     * Returns a list of source that matches the given classifiers.
     *
     * @param classifiers a set of classifiers to filter on.
     * @return a list of sources, empty if nothing matches.
     */
    protected abstract List<DiagnosticsReportSource> provideSources( Set<String> classifiers );

    final Set<String> getFilterClassifiers()
    {
        return filterClassifiers;
    }

    final List<DiagnosticsReportSource> getDiagnosticsSources( Set<String> classifiers )
    {
        if ( classifiers.contains( "all" ) )
        {
            return provideSources( filterClassifiers );
        }
        else
        {
            return provideSources( classifiers );
        }
    }
}
