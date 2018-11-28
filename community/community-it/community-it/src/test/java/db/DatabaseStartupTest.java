/*
 * Copyright (c) 2002-2018 "Neo4j,"
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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.facade.GraphDatabaseFacadeFactory;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.factory.module.PlatformModule;
import org.neo4j.graphdb.factory.module.edition.CommunityEditionModule;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.factory.DatabaseInfo;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.storemigration.StoreUpgrader;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.scheduler.ThreadPoolJobScheduler;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.io.pagecache.impl.muninn.StandalonePageCacheFactory.createPageCache;

@ExtendWith( TestDirectoryExtension.class )
class DatabaseStartupTest
{
    @Inject
    private TestDirectory testDirectory;

    @Test
    void startTheDatabaseWithWrongVersionShouldFailWithUpgradeNotAllowed() throws Throwable
    {
        // given
        // create a store
        File databaseDir = testDirectory.databaseDir();
        GraphDatabaseService db = new TestGraphDatabaseFactory().newEmbeddedDatabase( databaseDir );
        try ( Transaction tx = db.beginTx() )
        {
            db.createNode();
            tx.success();
        }
        db.shutdown();

        // mess up the version in the metadatastore
        try ( FileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction();
              ThreadPoolJobScheduler scheduler = new ThreadPoolJobScheduler();
              PageCache pageCache = createPageCache( fileSystem, scheduler ) )
        {
            MetaDataStore.setRecord( pageCache, testDirectory.databaseLayout().metadataStore(),
                    MetaDataStore.Position.STORE_VERSION, MetaDataStore.versionStringToLong( "bad" ));
        }

        RuntimeException exception = assertThrows( RuntimeException.class, () -> new TestGraphDatabaseFactory().newEmbeddedDatabase( databaseDir ) );
        Throwable throwable = exception.getCause().getCause();
        assertThat( throwable, instanceOf( IllegalArgumentException.class ) );
        assertEquals( "Unknown store version 'bad'", throwable.getMessage() );
    }

    @Test
    void startTheDatabaseWithWrongVersionShouldFailAlsoWhenUpgradeIsAllowed() throws Throwable
    {
        // given
        // create a store
        File databaseDirectory = testDirectory.databaseDir();
        GraphDatabaseService db = new TestGraphDatabaseFactory().newEmbeddedDatabase( databaseDirectory );
        try ( Transaction tx = db.beginTx() )
        {
            db.createNode();
            tx.success();
        }
        db.shutdown();

        // mess up the version in the metadatastore
        String badStoreVersion = "bad";
        try ( FileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction();
              ThreadPoolJobScheduler scheduler = new ThreadPoolJobScheduler();
              PageCache pageCache = createPageCache( fileSystem, scheduler ) )
        {
            MetaDataStore.setRecord( pageCache, testDirectory.databaseLayout().metadataStore(), MetaDataStore.Position.STORE_VERSION,
                    MetaDataStore.versionStringToLong( badStoreVersion ) );
        }

        RuntimeException exception = assertThrows( RuntimeException.class,
                () -> new TestGraphDatabaseFactory().newEmbeddedDatabaseBuilder( databaseDirectory ).setConfig( GraphDatabaseSettings.allow_upgrade,
                        "true" ).newGraphDatabase() );
        assertThat( exception.getCause().getCause(), instanceOf( StoreUpgrader.UnexpectedUpgradingStoreVersionException.class ) );
    }

    @Test
    void startTestDatabaseOnProvidedNonAbsoluteFile()
    {
        File directory = new File( "notAbsoluteDirectory" );
        new TestGraphDatabaseFactory().newImpermanentDatabase( directory ).shutdown();
    }

    @Test
    void startCommunityDatabaseOnProvidedNonAbsoluteFile()
    {
        File directory = new File( "notAbsoluteDirectory" );
        EphemeralCommunityFacadeFactory factory = new EphemeralCommunityFacadeFactory();
        GraphDatabaseFactory databaseFactory = new EphemeralGraphDatabaseFactory( factory );
        GraphDatabaseService service = databaseFactory.newEmbeddedDatabase( directory );
        service.shutdown();
    }

    @Test
    void dumpSystemDiagnosticLoggingOnStartup()
    {
        AssertableLogProvider logProvider = new AssertableLogProvider();
        GraphDatabaseService database = new TestGraphDatabaseFactory().setInternalLogProvider( logProvider ).newEmbeddedDatabase( testDirectory.databaseDir() );
        try
        {
            logProvider.assertContainsMessageContaining( "System diagnostics" );
            logProvider.assertContainsMessageContaining( "System memory information" );
            logProvider.assertContainsMessageContaining( "JVM memory information" );
            logProvider.assertContainsMessageContaining( "Operating system information" );
            logProvider.assertContainsMessageContaining( "JVM information" );
            logProvider.assertContainsMessageContaining( "Java classpath" );
            logProvider.assertContainsMessageContaining( "Library path" );
            logProvider.assertContainsMessageContaining( "System properties" );
            logProvider.assertContainsMessageContaining( "(IANA) TimeZone database version" );
            logProvider.assertContainsMessageContaining( "Network information" );
            logProvider.assertContainsMessageContaining( "DBMS config" );
        }
        finally
        {
            database.shutdown();
        }
    }

    private static class EphemeralCommunityFacadeFactory extends GraphDatabaseFacadeFactory
    {
        EphemeralCommunityFacadeFactory()
        {
            super( DatabaseInfo.COMMUNITY, CommunityEditionModule::new );
        }

        @Override
        protected PlatformModule createPlatform( File storeDir, Config config, Dependencies dependencies )
        {
            return new PlatformModule( storeDir, config, databaseInfo, dependencies )
            {
                @Override
                protected FileSystemAbstraction createFileSystemAbstraction()
                {
                    return new EphemeralFileSystemAbstraction();
                }
            };
        }
    }

    private static class EphemeralGraphDatabaseFactory extends GraphDatabaseFactory
    {
        private final EphemeralCommunityFacadeFactory factory;

        EphemeralGraphDatabaseFactory( EphemeralCommunityFacadeFactory factory )
        {
            this.factory = factory;
        }

        @Override
        protected GraphDatabaseFacadeFactory getGraphDatabaseFacadeFactory()
        {
            return factory;
        }
    }
}
