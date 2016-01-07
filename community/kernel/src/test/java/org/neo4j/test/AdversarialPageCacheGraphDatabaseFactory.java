/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import org.neo4j.adversaries.Adversary;
import org.neo4j.adversaries.pagecache.AdversarialPageCache;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.factory.CommunityFacadeFactory;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.factory.PlatformModule;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.monitoring.tracing.Tracers;

import static org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory.Dependencies;

public class AdversarialPageCacheGraphDatabaseFactory
{
    private AdversarialPageCacheGraphDatabaseFactory()
    {
        throw new AssertionError( "Not for instantiation!" );
    }

    public static GraphDatabaseFactory create( FileSystemAbstraction fs, Adversary adversary )
    {
        return new GraphDatabaseFactory()
        {
            @Override
            protected GraphDatabaseService newDatabase( File dir, Map<String,String> config, Dependencies dependencies )
            {
                return new CommunityFacadeFactory()
                {
                    @Override
                    protected PlatformModule createPlatform( File storeDir, Map<String,String> params,
                            Dependencies dependencies, GraphDatabaseFacade facade )
                    {
                        return new PlatformModule( storeDir, params, databaseInfo(), dependencies, facade )
                        {
                            @Override
                            protected FileSystemAbstraction createFileSystemAbstraction()
                            {
                                return fs;
                            }

                            @Override
                            protected PageCache createPageCache( FileSystemAbstraction fileSystem, Config config,
                                    LogService logging, Tracers tracers )
                            {
                                PageCache pageCache = super.createPageCache( fileSystem, config, logging, tracers );
                                return new AdversarialPageCache( pageCache, adversary );
                            }
                        };
                    }
                }.newFacade( dir, config, dependencies );
            }
        };
    }
}
