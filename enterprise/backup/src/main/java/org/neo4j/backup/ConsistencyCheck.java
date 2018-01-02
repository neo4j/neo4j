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
package org.neo4j.backup;

import java.io.File;
import java.util.Arrays;

import org.neo4j.consistency.ConsistencyCheckTool;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.LogProvider;

enum ConsistencyCheck
{
    NONE
            {
                @Override
                public boolean runFull( File storeDir, Config tuningConfiguration,
                        ProgressMonitorFactory progressFactory, LogProvider logProvider,
                        FileSystemAbstraction fileSystem, PageCache pageCache, boolean verbose )
                {
                    return true;
                }
            },
    DEFAULT
            {
                @Override
                public boolean runFull( File storeDir, Config tuningConfiguration,
                        ProgressMonitorFactory progressFactory, LogProvider logProvider,
                        FileSystemAbstraction fileSystem, PageCache pageCache, boolean verbose )
                        throws ConsistencyCheckFailedException
                {
                    ConsistencyCheck checker = ConsistencyCheckTool.USE_LEGACY_BY_DEFAULT ? LEGACY : EXPERIMENTAL;
                    return checker.runFull( storeDir, tuningConfiguration, progressFactory, logProvider,
                            fileSystem, pageCache, verbose );
                }
            },
    EXPERIMENTAL
            {
                @Override
                public boolean runFull( File storeDir, Config tuningConfiguration,
                        ProgressMonitorFactory progressFactory, LogProvider logProvider,
                        FileSystemAbstraction fileSystem, PageCache pageCache, boolean verbose )
                        throws ConsistencyCheckFailedException
                {
                    try
                    {
                        return new org.neo4j.consistency.ConsistencyCheckService().runFullConsistencyCheck( storeDir,
                                tuningConfiguration, progressFactory, logProvider, fileSystem, pageCache, verbose )
                                .isSuccessful();
                    }
                    catch ( org.neo4j.consistency.checking.full.ConsistencyCheckIncompleteException e )
                    {
                        throw new ConsistencyCheckFailedException( e );
                    }
                }
            },
    LEGACY
            {
                @Override
                public boolean runFull( File storeDir, Config tuningConfiguration,
                        ProgressMonitorFactory progressFactory, LogProvider logProvider,
                        FileSystemAbstraction fileSystem, PageCache pageCache, boolean verbose )
                        throws ConsistencyCheckFailedException
                {
                    try
                    {
                        return new org.neo4j.legacy.consistency.ConsistencyCheckService().runFullConsistencyCheck(
                                storeDir, tuningConfiguration, progressFactory, logProvider, fileSystem, pageCache )
                                .isSuccessful();
                    }
                    catch ( org.neo4j.legacy.consistency.checking.full.ConsistencyCheckIncompleteException e )
                    {
                        throw new ConsistencyCheckFailedException( e );
                    }
                }
            };

    public abstract boolean runFull( File storeDir, Config tuningConfiguration, ProgressMonitorFactory progressFactory,
            LogProvider logProvider, FileSystemAbstraction fileSystem, PageCache pageCache, boolean verbose )
            throws ConsistencyCheckFailedException;

    public static ConsistencyCheck fromString( String name )
    {
        for ( ConsistencyCheck consistencyCheck : values() )
        {
            if ( consistencyCheck.toString().equalsIgnoreCase( name ) )
            {
                return consistencyCheck;
            }
        }
        throw new IllegalArgumentException( "Unknown consistency check name: " + name +
                ". Supported values: " + Arrays.toString( values() ) );
    }
}
