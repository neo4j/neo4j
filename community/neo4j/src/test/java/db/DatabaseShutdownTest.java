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
package db;

import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.function.Supplier;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.DelegatingPageCache;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.cursor.context.VersionContextSupplier;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.factory.CommunityEditionModule;
import org.neo4j.kernel.impl.factory.DataSourceModule;
import org.neo4j.kernel.impl.factory.DatabaseInfo;
import org.neo4j.kernel.impl.factory.EditionModule;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory;
import org.neo4j.kernel.impl.factory.PlatformModule;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.query.QueryExecutionEngine;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.LifecycleException;
import org.neo4j.kernel.lifecycle.LifecycleStatus;
import org.neo4j.kernel.monitoring.tracing.Tracers;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class DatabaseShutdownTest
{
    @Rule
    public final EphemeralFileSystemRule fs = new EphemeralFileSystemRule();
    @Rule
    public final TestDirectory testDirectory =
            TestDirectory.testDirectory( getClass(), fs.get() );

    @Test
    public void shouldShutdownCorrectlyWhenCheckPointingOnShutdownFails() throws Exception
    {
        TestGraphDatabaseFactoryWithFailingPageCacheFlush factory =
                new TestGraphDatabaseFactoryWithFailingPageCacheFlush();

        try
        {
            factory.newEmbeddedDatabase( testDirectory.graphDbDir() ).shutdown();
            fail( "Should have thrown" );
        }
        catch ( LifecycleException ex )
        {
            assertEquals( LifecycleStatus.SHUTDOWN, factory.getNeoStoreDataSourceStatus() );
        }
    }

    private static class TestGraphDatabaseFactoryWithFailingPageCacheFlush extends TestGraphDatabaseFactory
    {
        private NeoStoreDataSource neoStoreDataSource;

        @Override
        protected GraphDatabaseService newEmbeddedDatabase( File storeDir, Config config,
                GraphDatabaseFacadeFactory.Dependencies dependencies )
        {
            return new GraphDatabaseFacadeFactory( DatabaseInfo.COMMUNITY, CommunityEditionModule::new )
            {
                @Override
                protected DataSourceModule createDataSource(
                        PlatformModule platformModule,
                        EditionModule editionModule,
                        Supplier<QueryExecutionEngine> queryEngine )
                {
                    DataSourceModule dataSource = new DataSourceModule( platformModule, editionModule, queryEngine );
                    neoStoreDataSource = dataSource.neoStoreDataSource;
                    return dataSource;
                }

                @Override
                protected PlatformModule createPlatform( File storeDir, Config config,
                        Dependencies dependencies, GraphDatabaseFacade graphDatabaseFacade )
                {
                    return new PlatformModule( storeDir, config, databaseInfo, dependencies, graphDatabaseFacade )
                    {
                        @Override
                        protected PageCache createPageCache( FileSystemAbstraction fileSystem, Config config, LogService logging,
                                Tracers tracers, VersionContextSupplier versionContextSupplier )
                        {
                            PageCache pageCache = super.createPageCache( fileSystem, config, logging, tracers,
                                    versionContextSupplier );
                            return new DelegatingPageCache( pageCache )
                            {
                                @Override
                                public void flushAndForce( IOLimiter ioLimiter ) throws IOException
                                {
                                    // this is simulating a failing check pointing on shutdown
                                    throw new IOException( "Boom!" );
                                }
                            };
                        }
                    };
                }
            }.newFacade( storeDir, config, dependencies );
        }

        LifecycleStatus getNeoStoreDataSourceStatus() throws NoSuchFieldException, IllegalAccessException
        {
            Field f = neoStoreDataSource.getClass().getDeclaredField( "life" );
            f.setAccessible( true );
            LifeSupport life = (LifeSupport) f.get( neoStoreDataSource );
            return life.getStatus();
        }
    }
}
