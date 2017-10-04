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

import org.neo4j.consistency.ConsistencyCheckService;
import org.neo4j.consistency.checking.full.ConsistencyFlags;
import org.neo4j.consistency.checking.full.ConsistencyCheckIncompleteException;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.LogProvider;

interface ConsistencyCheck
{
    ConsistencyCheck NONE =
            new ConsistencyCheck()
            {
                @Override
                public String name()
                {
                    return "none";
                }

                @Override
                public boolean runFull( File storeDir, Config tuningConfiguration,
                        ProgressMonitorFactory progressFactory, LogProvider logProvider,
                        FileSystemAbstraction fileSystem, PageCache pageCache, boolean verbose,
                        ConsistencyFlags consistencyFlags )
                        throws ConsistencyCheckFailedException
                {
                    return true;
                }
            };

    ConsistencyCheck FULL =
            new ConsistencyCheck()
            {
                @Override
                public String name()
                {
                    return "full";
                }

                @Override
                public boolean runFull( File storeDir, Config tuningConfiguration,
                        ProgressMonitorFactory progressFactory, LogProvider logProvider,
                        FileSystemAbstraction fileSystem, PageCache pageCache, boolean verbose,
                        ConsistencyFlags consistencyFlags ) throws ConsistencyCheckFailedException
                {
                    try
                    {
                        return new ConsistencyCheckService().runFullConsistencyCheck( storeDir, tuningConfiguration,
                                progressFactory, logProvider, fileSystem, pageCache, verbose, consistencyFlags )
                                .isSuccessful();
                    }
                    catch ( ConsistencyCheckIncompleteException e )
                    {
                        throw new ConsistencyCheckFailedException( e );
                    }
                }
            };

    String name();

    boolean runFull( File storeDir, Config tuningConfiguration, ProgressMonitorFactory progressFactory,
            LogProvider logProvider, FileSystemAbstraction fileSystem, PageCache pageCache, boolean verbose,
            ConsistencyFlags consistencyFlags )
            throws ConsistencyCheckFailedException;

    String toString();

    static ConsistencyCheck fromString( String name )
    {
        for ( ConsistencyCheck consistencyCheck : new ConsistencyCheck[]{NONE, FULL} )
        {
            if ( consistencyCheck.name().equalsIgnoreCase( name ) )
            {
                return consistencyCheck;
            }
        }
        throw new IllegalArgumentException( "Unknown consistency check name: " + name +
                ". Supported values: NONE, FULL" );
    }

    static ConsistencyCheck full( File reportDir, ConsistencyCheckService consistencyCheckService )
    {
        return new ConsistencyCheck()
        {
            @Override
            public String name()
            {
                return "full";
            }

            @Override
            public boolean runFull( File storeDir, Config tuningConfiguration, ProgressMonitorFactory progressFactory,
                    LogProvider logProvider, FileSystemAbstraction fileSystem, PageCache pageCache, boolean verbose,
                    ConsistencyFlags consistencyFlags )
                    throws ConsistencyCheckFailedException
            {
                try
                {
                    return consistencyCheckService
                            .runFullConsistencyCheck( storeDir, tuningConfiguration, progressFactory, logProvider,
                                    fileSystem, pageCache, verbose, reportDir, consistencyFlags ).isSuccessful();
                }
                catch ( ConsistencyCheckIncompleteException e )
                {
                    throw new ConsistencyCheckFailedException( e );
                }
            }
        };
    }
}
