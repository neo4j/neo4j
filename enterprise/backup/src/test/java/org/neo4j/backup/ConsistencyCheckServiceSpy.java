/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.backup;

import java.io.File;
import java.io.IOException;

import org.neo4j.consistency.ConsistencyCheckService;
import org.neo4j.consistency.checking.full.ConsistencyCheckIncompleteException;
import org.neo4j.consistency.checking.full.ConsistencyFlags;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.LogProvider;

class ConsistencyCheckServiceSpy extends ConsistencyCheckService
{
    private boolean checked;

    @Override
    public Result runFullConsistencyCheck( File storeDir, Config tuningConfiguration, ProgressMonitorFactory progressFactory, LogProvider logProvider,
            boolean verbose ) throws ConsistencyCheckIncompleteException, IOException
    {
        markAsChecked();
        return super.runFullConsistencyCheck( storeDir, tuningConfiguration, progressFactory, logProvider, verbose );
    }

    public Result runFullConsistencyCheck( File storeDir, Config config, ProgressMonitorFactory progressFactory, LogProvider logProvider, boolean verbose,
            ConsistencyFlags consistencyFlags ) throws ConsistencyCheckIncompleteException, IOException
    {
        markAsChecked();
        return super.runFullConsistencyCheck( storeDir, config, progressFactory, logProvider, verbose, consistencyFlags );
    }

    @Deprecated
    public Result runFullConsistencyCheck( File storeDir, Config tuningConfiguration, ProgressMonitorFactory progressFactory, LogProvider logProvider,
            FileSystemAbstraction fileSystem, boolean verbose ) throws ConsistencyCheckIncompleteException, IOException
    {
        markAsChecked();
        return super.runFullConsistencyCheck( storeDir, tuningConfiguration, progressFactory, logProvider, fileSystem, verbose );
    }

    public Result runFullConsistencyCheck( File storeDir, Config config, ProgressMonitorFactory progressFactory, LogProvider logProvider,
            FileSystemAbstraction fileSystem, boolean verbose, ConsistencyFlags consistencyFlags ) throws ConsistencyCheckIncompleteException, IOException
    {
        markAsChecked();
        return super.runFullConsistencyCheck( storeDir, config, progressFactory, logProvider, fileSystem, verbose, consistencyFlags );
    }

    @Deprecated
    public Result runFullConsistencyCheck( File storeDir, Config tuningConfiguration, ProgressMonitorFactory progressFactory, LogProvider logProvider,
            FileSystemAbstraction fileSystem, boolean verbose, File reportDir ) throws ConsistencyCheckIncompleteException, IOException
    {
        markAsChecked();
        return super.runFullConsistencyCheck( storeDir, tuningConfiguration, progressFactory, logProvider, fileSystem, verbose, reportDir );
    }

    public Result runFullConsistencyCheck( File storeDir, Config config, ProgressMonitorFactory progressFactory, LogProvider logProvider,
            FileSystemAbstraction fileSystem, boolean verbose, File reportDir, ConsistencyFlags consistencyFlags )
            throws ConsistencyCheckIncompleteException, IOException
    {
        markAsChecked();
        return super.runFullConsistencyCheck( storeDir, config, progressFactory, logProvider, fileSystem, verbose, reportDir, consistencyFlags );
    }

    @Deprecated
    public Result runFullConsistencyCheck( final File storeDir, Config tuningConfiguration, ProgressMonitorFactory progressFactory,
            final LogProvider logProvider, final FileSystemAbstraction fileSystem, final PageCache pageCache, final boolean verbose )
            throws ConsistencyCheckIncompleteException
    {
        markAsChecked();
        return super.runFullConsistencyCheck( storeDir, tuningConfiguration, progressFactory, logProvider, fileSystem, pageCache, verbose );
    }

    public Result runFullConsistencyCheck( final File storeDir, Config config, ProgressMonitorFactory progressFactory, final LogProvider logProvider,
            final FileSystemAbstraction fileSystem, final PageCache pageCache, final boolean verbose, ConsistencyFlags consistencyFlags )
            throws ConsistencyCheckIncompleteException
    {
        markAsChecked();
        return super.runFullConsistencyCheck( storeDir, config, progressFactory, logProvider, fileSystem, pageCache, verbose, consistencyFlags );
    }

    @Deprecated
    public Result runFullConsistencyCheck( final File storeDir, Config tuningConfiguration, ProgressMonitorFactory progressFactory,
            final LogProvider logProvider, final FileSystemAbstraction fileSystem, final PageCache pageCache, final boolean verbose, File reportDir )
            throws ConsistencyCheckIncompleteException
    {
        markAsChecked();
        return super.runFullConsistencyCheck( storeDir, tuningConfiguration, progressFactory, logProvider, fileSystem, pageCache, verbose, reportDir );
    }

    public Result runFullConsistencyCheck( final File storeDir, Config config, ProgressMonitorFactory progressFactory, final LogProvider logProvider,
            final FileSystemAbstraction fileSystem, final PageCache pageCache, final boolean verbose, File reportDir, ConsistencyFlags consistencyFlags )
            throws ConsistencyCheckIncompleteException
    {
        markAsChecked();
        return super.runFullConsistencyCheck( storeDir, config, progressFactory, logProvider, fileSystem, pageCache, verbose, reportDir, consistencyFlags );
    }

    private void markAsChecked()
    {
        checked = true;
    }

    public void unmark()
    {
        if ( !checked )
        {
            throw new IllegalStateException( "No need to unmark" );
        }
        checked = false;
    }

    boolean isChecked()
    {
        return checked;
    }
}
