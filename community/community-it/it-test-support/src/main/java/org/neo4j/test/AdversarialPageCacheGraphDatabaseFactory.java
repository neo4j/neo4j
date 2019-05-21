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
package org.neo4j.test;

import java.io.File;

import org.neo4j.adversaries.Adversary;
import org.neo4j.adversaries.pagecache.AdversarialPageCache;
import org.neo4j.configuration.Config;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.graphdb.facade.DatabaseManagementServiceFactory;
import org.neo4j.graphdb.facade.ExternalDependencies;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.graphdb.factory.module.edition.CommunityEditionModule;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.factory.DatabaseInfo;
import org.neo4j.kernel.monitoring.tracing.Tracers;
import org.neo4j.logging.internal.LogService;
import org.neo4j.scheduler.JobScheduler;

public class AdversarialPageCacheGraphDatabaseFactory
{
    private AdversarialPageCacheGraphDatabaseFactory()
    {
        throw new AssertionError( "Not for instantiation!" );
    }

    public static DatabaseManagementServiceBuilder create( File databaseRootDir, FileSystemAbstraction fs, Adversary adversary )
    {
        return new TestDatabaseManagementServiceBuilder( databaseRootDir )
        {
            @Override
            protected DatabaseManagementService newDatabaseManagementService( File dir, Config config, ExternalDependencies dependencies )
            {
                return new DatabaseManagementServiceFactory( DatabaseInfo.COMMUNITY, CommunityEditionModule::new )
                {

                    @Override
                    protected GlobalModule createGlobalModule( File storeDir, Config config, ExternalDependencies dependencies )
                    {
                        return new GlobalModule( storeDir, config, databaseInfo, dependencies )
                        {
                            @Override
                            protected FileSystemAbstraction createFileSystemAbstraction()
                            {
                                return fs;
                            }

                            @Override
                            protected PageCache createPageCache( FileSystemAbstraction fileSystem, Config config,
                                    LogService logging, Tracers tracers, JobScheduler jobScheduler )
                            {
                                PageCache pageCache = super.createPageCache( fileSystem, config, logging, tracers, jobScheduler );
                                return new AdversarialPageCache( pageCache, adversary );
                            }
                        };
                    }
                }.build( dir, config, dependencies );
            }
        };
    }
}
