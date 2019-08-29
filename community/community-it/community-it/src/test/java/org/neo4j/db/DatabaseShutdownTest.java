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
package org.neo4j.db;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.OpenOption;

import org.neo4j.configuration.Config;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.graphdb.GraphDatabaseService;
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
import org.neo4j.kernel.impl.factory.DatabaseInfo;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.LifecycleStatus;
import org.neo4j.kernel.monitoring.tracing.Tracers;
import org.neo4j.logging.internal.LogService;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

@EphemeralTestDirectoryExtension
class DatabaseShutdownTest
{
    @Inject
    private TestDirectory testDirectory;
    @Inject
    private FileSystemAbstraction fs;

    @Test
    void shouldShutdownCorrectlyWhenCheckPointingOnShutdownFails()
    {
        DatabaseLayout databaseLayout = testDirectory.databaseLayout();
        TestDatabaseManagementServiceBuilderWithFailingPageCacheFlush factory =
                new TestDatabaseManagementServiceBuilderWithFailingPageCacheFlush( databaseLayout.databaseDirectory(), fs );
        DatabaseManagementService managementService = factory.build();
        GraphDatabaseService databaseService = managementService.database( DEFAULT_DATABASE_NAME );
        DatabaseManager<?> databaseManager = ((GraphDatabaseAPI) databaseService).getDependencyResolver().resolveDependency( DatabaseManager.class );
        var databaseContext = databaseManager.getDatabaseContext( databaseLayout.getDatabaseName() );
        factory.setFailFlush( true );
        managementService.shutdown();
        DatabaseContext context = databaseContext.get();
        assertTrue( context.isFailed() );
        assertEquals( LifecycleStatus.SHUTDOWN, factory.getDatabaseStatus() );
    }

    @Test
    void invokeDatabaseShutdownListenersOnShutdown()
    {
        DatabaseManagementService managementService = new TestDatabaseManagementServiceBuilder( testDirectory.storeDir() ).setFileSystem( fs ).build();
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

        TestDatabaseManagementServiceBuilderWithFailingPageCacheFlush( File databaseRootDir, FileSystemAbstraction fs )
        {
            super( databaseRootDir );
            this.fs = fs;
        }

        @Override
        protected DatabaseManagementService newDatabaseManagementService( File storeDir, Config config, ExternalDependencies dependencies )
        {
            return new DatabaseManagementServiceFactory( DatabaseInfo.COMMUNITY, CommunityEditionModule::new )
            {

                @Override
                protected GlobalModule createGlobalModule( File storeDir, Config config, ExternalDependencies dependencies )
                {
                    GlobalModule globalModule = new GlobalModule( storeDir, config, databaseInfo, dependencies )
                    {
                        @Override
                        protected PageCache createPageCache( FileSystemAbstraction fileSystem, Config config, LogService logging, Tracers tracers,
                                JobScheduler jobScheduler )
                        {
                            PageCache pageCache = super.createPageCache( fileSystem, config, logging, tracers, jobScheduler );
                            return new DelegatingPageCache( pageCache )
                            {
                                @Override
                                public PagedFile map( File file, VersionContextSupplier versionContextSupplier, int pageSize, OpenOption... openOptions )
                                        throws IOException
                                {
                                    PagedFile pagedFile = super.map( file, versionContextSupplier, pageSize, openOptions );
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
            }.build( storeDir, config, dependencies );
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
