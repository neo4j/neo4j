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
package org.neo4j.upgrade;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.consistency.checking.full.ConsistencyCheckIncompleteException;
import org.neo4j.dbms.DatabaseStateService;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.store.format.standard.StandardV3_4;
import org.neo4j.kernel.impl.storemigration.RecordStoreVersionCheck;
import org.neo4j.kernel.impl.storemigration.StoreUpgrader;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.storageengine.api.StoreVersionCheck;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static org.apache.commons.lang3.exception.ExceptionUtils.getRootCause;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.consistency.store.StoreAssertions.assertConsistentStore;
import static org.neo4j.kernel.impl.storemigration.MigrationTestUtils.checkNeoStoreHasDefaultFormatVersion;
import static org.neo4j.kernel.impl.storemigration.MigrationTestUtils.prepareSampleLegacyDatabase;
import static org.neo4j.kernel.impl.storemigration.StoreUpgraderTest.removeCheckPointFromTxLog;

@RunWith( Parameterized.class )
public class StoreUpgradeOnStartupTest
{
    private final TestDirectory testDir = TestDirectory.testDirectory();
    private final PageCacheRule pageCacheRule = new PageCacheRule();
    private final DefaultFileSystemRule fileSystemRule = new DefaultFileSystemRule();

    @Rule
    public RuleChain ruleChain = RuleChain.outerRule( testDir )
            .around( fileSystemRule ).around( pageCacheRule );

    @Parameterized.Parameter( 0 )
    public String version;

    private FileSystemAbstraction fileSystem;
    private DatabaseLayout workingDatabaseLayout;
    private StoreVersionCheck check;
    private File workingHomeDir;
    private DatabaseManagementService managementService;

    @Parameterized.Parameters( name = "{0}" )
    public static Collection<String> versions()
    {
        return Collections.singletonList( StandardV3_4.STORE_VERSION );
    }

    @Before
    public void setup() throws IOException
    {
        fileSystem = fileSystemRule.get();
        PageCache pageCache = pageCacheRule.getPageCache( fileSystem );
        workingHomeDir = testDir.homeDir( "working_" + version );
        workingDatabaseLayout = Neo4jLayout.of( workingHomeDir ).databaseLayout( DEFAULT_DATABASE_NAME );
        check = new RecordStoreVersionCheck( fileSystem, pageCache, workingDatabaseLayout, NullLogProvider.getInstance(), Config.defaults() );
        File prepareDirectory = testDir.directory( "prepare_" + version );
        prepareSampleLegacyDatabase( version, fileSystem, workingDatabaseLayout.databaseDirectory(), prepareDirectory );
    }

    @Test
    public void shouldUpgradeAutomaticallyOnDatabaseStartup() throws ConsistencyCheckIncompleteException
    {
        // when
        createGraphDatabaseService();
        managementService.shutdown();

        // then
        assertTrue( "Some store files did not have the correct version",
                checkNeoStoreHasDefaultFormatVersion( check ) );
        assertConsistentStore( workingDatabaseLayout );
    }

    @Test
    public void shouldAbortOnNonCleanlyShutdown() throws Throwable
    {
        // given
        removeCheckPointFromTxLog( fileSystem, workingDatabaseLayout.databaseDirectory() );
        GraphDatabaseAPI database = createGraphDatabaseService();
        try
        {
            DatabaseStateService databaseStateService = database.getDependencyResolver().resolveDependency( DatabaseStateService.class );
            assertTrue( databaseStateService.causeOfFailure( database.databaseId() ).isPresent() );
            assertThat( getRootCause( databaseStateService.causeOfFailure( database.databaseId() ).get() ) ).isInstanceOf(
                    StoreUpgrader.UnableToUpgradeException.class );
        }
        finally
        {
            managementService.shutdown();
        }
    }

    private GraphDatabaseAPI createGraphDatabaseService()
    {
        managementService = new TestDatabaseManagementServiceBuilder( workingHomeDir )
                .setConfig( GraphDatabaseSettings.allow_upgrade, true )
                .build();
        return (GraphDatabaseAPI) managementService.database( DEFAULT_DATABASE_NAME );
    }
}
