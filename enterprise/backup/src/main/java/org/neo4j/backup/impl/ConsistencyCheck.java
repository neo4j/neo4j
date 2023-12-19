/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.backup.impl;

import java.nio.file.Path;

import org.neo4j.consistency.ConsistencyCheckService;
import org.neo4j.consistency.checking.full.ConsistencyCheckIncompleteException;
import org.neo4j.consistency.checking.full.ConsistencyFlags;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.LogProvider;

public interface ConsistencyCheck
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
                public boolean runFull( Path storeDir, Config tuningConfiguration,
                        ProgressMonitorFactory progressFactory, LogProvider logProvider,
                        FileSystemAbstraction fileSystem, PageCache pageCache, boolean verbose,
                        ConsistencyFlags consistencyFlags )
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
                public boolean runFull( Path storeDir, Config tuningConfiguration,
                        ProgressMonitorFactory progressFactory, LogProvider logProvider,
                        FileSystemAbstraction fileSystem, PageCache pageCache, boolean verbose,
                        ConsistencyFlags consistencyFlags ) throws ConsistencyCheckFailedException
                {
                    try
                    {
                        return new ConsistencyCheckService().runFullConsistencyCheck(
                                storeDir.toFile(), tuningConfiguration, progressFactory, logProvider, fileSystem,
                                pageCache, verbose, consistencyFlags ).isSuccessful();
                    }
                    catch ( ConsistencyCheckIncompleteException e )
                    {
                        throw new ConsistencyCheckFailedException( e );
                    }
                }
            };

    String name();

    boolean runFull( Path storeDir, Config tuningConfiguration, ProgressMonitorFactory progressFactory,
                     LogProvider logProvider, FileSystemAbstraction fileSystem, PageCache pageCache, boolean verbose,
                     ConsistencyFlags consistencyFlags ) throws ConsistencyCheckFailedException;

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
}
