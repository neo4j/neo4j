/*
 * Copyright (c) "Neo4j"
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

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.nio.file.Path;
import java.util.stream.Stream;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.consistency.checking.full.ConsistencyCheckIncompleteException;
import org.neo4j.dbms.DatabaseStateService;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.io.layout.recordstorage.RecordDatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.store.format.RecordFormatSelector;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.format.standard.StandardV3_4;
import org.neo4j.kernel.impl.storemigration.RecordStoreVersionCheck;
import org.neo4j.kernel.impl.storemigration.StoreUpgrader;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.storageengine.api.StoreVersionCheck;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.apache.commons.lang3.exception.ExceptionUtils.getRootCause;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.consistency.store.StoreAssertions.assertConsistentStore;
import static org.neo4j.io.pagecache.tracing.PageCacheTracer.NULL;
import static org.neo4j.kernel.impl.storemigration.MigrationTestUtils.checkNeoStoreHasFormatVersion;
import static org.neo4j.kernel.impl.storemigration.MigrationTestUtils.prepareSampleLegacyDatabase;
import static org.neo4j.kernel.impl.storemigration.StoreUpgraderTest.removeCheckPointFromTxLog;

@PageCacheExtension
public class StoreUpgradeOnStartupTest
{
    @Inject
    private TestDirectory testDir;
    @Inject
    private PageCache pageCache;
    @Inject
    private FileSystemAbstraction fileSystem;

    private RecordDatabaseLayout workingDatabaseLayout;
    private StoreVersionCheck check;
    private Path workingHomeDir;
    private DatabaseManagementService managementService;
    private RecordFormats successorFormat;

    private static Stream<Arguments> versions()
    {
        return Stream.of( Arguments.of( StandardV3_4.STORE_VERSION ) );
    }

    private void init( String version ) throws IOException
    {
        workingHomeDir = testDir.homePath( "working_" + version );
        workingDatabaseLayout = RecordDatabaseLayout.of( Neo4jLayout.of( workingHomeDir ), DEFAULT_DATABASE_NAME );
        check = new RecordStoreVersionCheck( fileSystem, pageCache, workingDatabaseLayout, NullLogProvider.getInstance(),
                Config.defaults(), NULL );
        Path prepareDirectory = testDir.directory( "prepare_" + version );
        prepareSampleLegacyDatabase( version, fileSystem, workingDatabaseLayout.databaseDirectory(), prepareDirectory );
        RecordFormats baselineFormat = RecordFormatSelector.selectForVersion( version );
        successorFormat = RecordFormatSelector.findLatestFormatInFamily( baselineFormat ).orElse( baselineFormat );
    }

    @ParameterizedTest
    @MethodSource( "versions" )
    public void shouldUpgradeAutomaticallyOnDatabaseStartup( String version ) throws ConsistencyCheckIncompleteException, IOException
    {
        init( version );

        // when
        createGraphDatabaseService();
        managementService.shutdown();

        // then
        assertTrue( checkNeoStoreHasFormatVersion( check, successorFormat ), "Some store files did not have the correct version" );
        assertConsistentStore( workingDatabaseLayout );
    }

    @ParameterizedTest
    @MethodSource( "versions" )
    public void shouldAbortOnNonCleanlyShutdown( String version ) throws Throwable
    {
        init( version );

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
