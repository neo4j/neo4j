/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.test;

import java.io.File;
import java.util.Map;

import org.neo4j.graphdb.mockfs.LimitedFilesystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.factory.CommunityFacadeFactory;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory;
import org.neo4j.kernel.impl.factory.PlatformModule;
import org.neo4j.udc.UsageDataKeys.OperationalMode;

public class LimitedFileSystemGraphDatabase extends ImpermanentGraphDatabase
{
    private LimitedFilesystemAbstraction fs;

    public LimitedFileSystemGraphDatabase( String storeDir )
    {
        super( new File( storeDir ) );
    }

    @Override
    protected void create( File storeDir, Map<String, String> params, GraphDatabaseFacadeFactory.Dependencies dependencies )
    {
        new CommunityFacadeFactory()
        {
            @Override
            protected PlatformModule createPlatform( File storeDir, Map<String, String> params,
                    Dependencies dependencies, GraphDatabaseFacade graphDatabaseFacade,
                    OperationalMode operationalMode )
            {
                return new ImpermanentPlatformModule( storeDir, params, dependencies, graphDatabaseFacade )
                {
                    @Override
                    protected FileSystemAbstraction createFileSystemAbstraction()
                    {
                        return fs = new LimitedFilesystemAbstraction( super.createFileSystemAbstraction() );
                    }
                };
            }
        }.newFacade( storeDir, params, dependencies, this );
    }


    public void runOutOfDiskSpaceNao()
    {
        this.fs.runOutOfDiskSpace( true );
    }

    public void somehowGainMoreDiskSpace()
    {
        this.fs.runOutOfDiskSpace( false );
    }

    public LimitedFilesystemAbstraction getFileSystem()
    {
        return fs;
    }
}
