/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.db;

import org.eclipse.collections.api.set.ImmutableSet;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.OpenOption;
import java.nio.file.Path;

import org.neo4j.configuration.Config;
import org.neo4j.dbms.DatabaseStateService;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.event.DatabaseEventContext;
import org.neo4j.graphdb.event.DatabaseEventListenerAdapter;
import org.neo4j.graphdb.facade.DatabaseManagementServiceFactory;
import org.neo4j.graphdb.facade.ExternalDependencies;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.graphdb.factory.module.edition.CommunityEditionModule;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.DelegatingPageCache;
import org.neo4j.io.pagecache.DelegatingPagedFile;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.tracing.cursor.context.VersionContextSupplier;
import org.neo4j.kernel.impl.factory.DbmsInfo;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.LifecycleStatus;
import org.neo4j.kernel.monitoring.tracing.Tracers;
import org.neo4j.logging.internal.LogService;
import org.neo4j.memory.MemoryPools;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.EphemeralNeo4jLayoutExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.time.SystemNanoClock;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

@EphemeralNeo4jLayoutExtension
class DatabaseShutdownTest
{
    @Inject
    private FileSystemAbstraction fs;
    @Inject
    private DatabaseLayout databaseLayout;

    @Test
    void shouldShutdownCorrectlyWhenCheckPointingOnShutdownFails()
    {
        TestDatabaseManagementServiceBuilderWithFailingPageCacheFlush factory =
                new TestDatabaseManagementServiceBuilderWithFailingPageCacheFlush( databaseLayout.databaseDirectory(), fs );
        DatabaseManagementService managementService = factory.build();
        GraphDatabaseAPI databaseService = (GraphDatabaseAPI) managementService.database( DEFAULT_DATABASE_NAME );
        DatabaseStateService dbStateService = databaseService.getDependencyResolver().resolveDependency( DatabaseStateService.class );
        factory.setFailFlush( true );
        managementService.shutdown();
        assertTrue( dbStateService.causeOfFailure( databaseService.databaseId() ).isPresent() );
        assertEquals( LifecycleStatus.SHUTDOWN, factory.getDatabaseStatus() );
    }

    @Test
    void invokeDatabaseShutdownListenersOnShutdown()
    {
        DatabaseManagementService managementService = new TestDatabaseManagementServiceBuilder( databaseLayout ).setFileSystem( fs ).build();
        ShutdownListenerDatabaseEventListener shutdownHandler = new ShutdownListenerDatabaseEventListener();
        managementService.registerDatabaseEventListener( shutdownHandler );
        managementService.shutdown();

        assertEquals( 2, shutdownHandler.shutdownCounter() );
    }

    private static class TestDatabaseManagementServiceBuilderWithFailingPageCacheFlush extends TestDatabaseManagementServiceBuilder
    {
        private final FileSystemAbstraction fs;
        private LifeSupport globalLife;
        private volatile boolean failFlush;

        TestDatabaseManagementServiceBuilderWithFailingPageCacheFlush( Path homeDirectory, FileSystemAbstraction fs )
        {
            super( homeDirectory );
            this.fs = fs;
        }

        @Override
        protected DatabaseManagementService newDatabaseManagementService( Config config, ExternalDependencies dependencies )
        {
            return new DatabaseManagementServiceFactory( DbmsInfo.COMMUNITY, CommunityEditionModule::new )
            {

                @Override
                protected GlobalModule createGlobalModule( Config config, ExternalDependencies dependencies )
                {
                    GlobalModule globalModule = new GlobalModule( config, dbmsInfo, dependencies )
                    {
                        @Override
                        protected PageCache createPageCache( FileSystemAbstraction fileSystem, Config config, LogService logging, Tracers tracers,
                                JobScheduler jobScheduler, SystemNanoClock clock, MemoryPools memoryPools )
                        {
                            PageCache pageCache = super.createPageCache( fileSystem, config, logging, tracers, jobScheduler, clock, memoryPools );
                            return new DelegatingPageCache( pageCache )
                            {
                                @Override
                                public PagedFile map( Path path, VersionContextSupplier versionContextSupplier, int pageSize,
                                        ImmutableSet<OpenOption> openOptions, String databaseName ) throws IOException
                                {
                                    PagedFile pagedFile = super.map( path, versionContextSupplier, pageSize, openOptions, databaseName );
                                    return new DelegatingPagedFile( pagedFile )
                                    {
                                        @Override
                                        public void flushAndForce( IOLimiter limiter ) throws IOException
                                        {
                                            if ( failFlush )
                                            {
                                                // this is simulating a failing check pointing on shutdown
                                                throw new IOException( "Boom!" );
                                            }
                                            super.flushAndForce( limiter );
                                        }
                                    };
                                }
                            };
                        }

                        @Override
                        protected FileSystemAbstraction createFileSystemAbstraction()
                        {
                            return fs;
                        }
                    };
                    globalLife = globalModule.getGlobalLife();
                    return globalModule;
                }
            }.build( config, dependencies );
        }

        LifecycleStatus getDatabaseStatus()
        {
            return globalLife.getStatus();
        }

        void setFailFlush( boolean failFlush )
        {
            this.failFlush = failFlush;
        }
    }

    private static class ShutdownListenerDatabaseEventListener extends DatabaseEventListenerAdapter
    {
        private int shutdownCounter;

        @Override
        public void databaseShutdown( DatabaseEventContext eventContext )
        {
            shutdownCounter++;
        }

        int shutdownCounter()
        {
            return shutdownCounter;
        }
    }
}
