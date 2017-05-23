/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.index;

import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ThreadFactory;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactoryState;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.NamedThreadFactory;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.kernel.api.impl.labelscan.LuceneLabelScanStore;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.api.labelscan.LabelScanWriter;
import org.neo4j.kernel.api.labelscan.NodeLabelUpdate;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.factory.CommunityEditionModule;
import org.neo4j.kernel.impl.factory.DatabaseInfo;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory;
import org.neo4j.kernel.impl.factory.PlatformModule;
import org.neo4j.kernel.impl.index.labelscan.NativeLabelScanStore;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.logging.NullLogService;
import org.neo4j.kernel.impl.logging.SimpleLogService;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.Lifecycles;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.LogProvider;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.test.OnDemandJobScheduler;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.rule.TestDirectory;

public class LabelScanStoreLoggingTest
{

    @Rule
    public TestDirectory testDirectory = TestDirectory.testDirectory();

    @Test
    public void noLuceneLabelScanStoreMonitorMessages() throws Throwable
    {
        AssertableLogProvider logProvider = new AssertableLogProvider( true );
        GraphDatabaseService database = newEmbeddedGraphDatabaseWithOnDemandScheduler( logProvider );
        try
        {
            NativeLabelScanStore labelScanStore = resolveDependency( database, NativeLabelScanStore.class);
            RecoveryCleanupWorkCollector recoveryCleanupWorkCollector =
                    resolveDependency( database, RecoveryCleanupWorkCollector.class );
            performSomeWrites( labelScanStore );
            restartAll( recoveryCleanupWorkCollector, labelScanStore );
            OnDemandJobScheduler jobScheduler = getOnDemandScheduler( database );
            jobScheduler.runJob();

            logProvider.assertNoLogCallContaining( LuceneLabelScanStore.class.getName() );
            logProvider.assertContainsLogCallContaining( NativeLabelScanStore.class.getName() );
            logProvider.assertContainsMessageContaining(
                    "Scan store recovery completed: Number of cleaned crashed pointers" );
        }
        finally
        {
            database.shutdown();
        }
    }

    @Test
    public void noNativeLabelScanStoreMonitorMessages() throws Throwable
    {
        AssertableLogProvider logProvider = new AssertableLogProvider( true );

        GraphDatabaseService database = new TestGraphDatabaseFactory()
                .setInternalLogProvider( logProvider )
                .newEmbeddedDatabaseBuilder( testDirectory.directory() )
                .setConfig( GraphDatabaseSettings.label_index.name(), GraphDatabaseSettings.LabelIndex.LUCENE.name() )
                .newGraphDatabase();
        try
        {
            LuceneLabelScanStore labelScanStore = resolveDependency( database, LuceneLabelScanStore.class);
            performSomeWrites( labelScanStore );
            restartAll( labelScanStore );
            logProvider.assertNoLogCallContaining( NativeLabelScanStore.class.getName() );
            logProvider.assertContainsLogCallContaining( LuceneLabelScanStore.class.getName() );
        }
        finally
        {
            database.shutdown();
        }
    }

    private OnDemandJobScheduler getOnDemandScheduler( GraphDatabaseService database )
    {
        return (OnDemandJobScheduler) resolveDependency( database, JobScheduler.class );
    }

    private static void restartAll( Lifecycle... lifecycles ) throws Throwable
    {
        Lifecycle combinedLifecycle = Lifecycles.multiple( lifecycles );
        combinedLifecycle.stop();
        combinedLifecycle.shutdown();

        combinedLifecycle.init();
        combinedLifecycle.start();
    }

    private void performSomeWrites( LabelScanStore labelScanStore ) throws IOException
    {
        try ( LabelScanWriter labelScanWriter = labelScanStore.newWriter() )
        {
            labelScanWriter.write( NodeLabelUpdate.labelChanges( 1, new long[]{}, new long[]{1} ) );
        }
    }

    private static <T> T resolveDependency( GraphDatabaseService database, Class<T> clazz )
    {
        DependencyResolver resolver = ((GraphDatabaseAPI) database).getDependencyResolver();
        return resolver.resolveDependency( clazz );
    }

    private GraphDatabaseService newEmbeddedGraphDatabaseWithOnDemandScheduler( LogProvider logProvider )
    {
        GraphDatabaseFactoryState graphDatabaseFactoryState = new GraphDatabaseFactoryState();
        graphDatabaseFactoryState.setUserLogProvider( NullLogService.getInstance().getUserLogProvider() );
        return new GraphDatabaseFacadeFactory( DatabaseInfo.COMMUNITY, CommunityEditionModule::new )
        {
            @Override
            protected PlatformModule createPlatform( File storeDir, Config config, Dependencies dependencies,
                    GraphDatabaseFacade graphDatabaseFacade )
            {
                return new PlatformModule( storeDir, config, databaseInfo, dependencies, graphDatabaseFacade )
                {
                    @Override
                    protected JobScheduler createJobScheduler()
                    {
                        return new TestScheduler();
                    }

                    @Override
                    protected LogService createLogService( LogProvider userLogProvider )
                    {
                        return new SimpleLogService( logProvider, logProvider );
                    }
                };
            }
        }.newFacade( testDirectory.graphDbDir(), Config.embeddedDefaults(),
                graphDatabaseFactoryState.databaseDependencies() );
    }

    private static class TestScheduler extends OnDemandJobScheduler
    {
        @Override
        public ThreadFactory threadFactory( Group group )
        {
            return new NamedThreadFactory( "test", true );
        }
    }
}
