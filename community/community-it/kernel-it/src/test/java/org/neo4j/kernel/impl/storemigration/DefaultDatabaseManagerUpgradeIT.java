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
package org.neo4j.kernel.impl.storemigration;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.file.Path;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementException;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.dbms.database.DefaultDatabaseManager;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.io.layout.recordstorage.RecordDatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.impl.store.format.aligned.PageAligned;
import org.neo4j.kernel.impl.store.format.standard.StandardV4_3;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.utils.TestDirectory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.io.pagecache.context.EmptyVersionContextSupplier.EMPTY;

@Neo4jLayoutExtension
class DefaultDatabaseManagerUpgradeIT
{
    private static final CursorContextFactory CONTEXT_FACTORY = new CursorContextFactory( PageCacheTracer.NULL, EMPTY );

    @Inject
    private TestDirectory testDirectory;
    @Inject
    private FileSystemAbstraction fs;
    @Inject
    private Neo4jLayout neo4jLayout;
    @Inject
    private RecordDatabaseLayout databaseLayout;
    private DatabaseManagementService dbms;
    private InternalLogProvider userLogProvider;

    @BeforeEach
    void setUp() throws IOException
    {
        userLogProvider = mock( InternalLogProvider.class, RETURNS_MOCKS );
        Path prepareDirectory = testDirectory.directory( "prepare" );
        MigrationTestUtils.prepareSampleLegacyDatabase( StandardV4_3.STORE_VERSION, fs, databaseLayout, prepareDirectory );
    }

    @AfterEach
    void tearDown()
    {
        dbms.shutdown();
    }

    @Test
    void upgradeDatabase()
    {
        // Given
        createDbms();
        GraphDatabaseAPI db = (GraphDatabaseAPI) dbms.database( DEFAULT_DATABASE_NAME );
        DefaultDatabaseManager databaseManager = getDatabaseManager( db );
        RecordStoreVersionCheck check =
                new RecordStoreVersionCheck( fs, getPageCache( db ), databaseLayout, NullLogProvider.getInstance(), Config.defaults(), CONTEXT_FACTORY );
        assertEquals( StandardV4_3.STORE_VERSION, check.storeVersion( CursorContext.NULL_CONTEXT ).orElseThrow() );

        // When
        databaseManager.upgradeDatabase( db.databaseId() );

        // Then
        assertTrue( db.isAvailable( 100 ), "Expected database to be available after upgrade" );
        assertTrue( MigrationTestUtils.checkNeoStoreHasFormatVersion( check, PageAligned.LATEST_RECORD_FORMATS ), "Expected store version to be default." );
    }

    @Test
    void upgradeDatabaseMustThrowOnFailure()
    {
        // Given
        RuntimeException expectedException = new RuntimeException( "Unexpected upgrade exception." );
        useThrowingMigrationLogProvider( expectedException );
        createDbms();
        GraphDatabaseAPI db = (GraphDatabaseAPI) dbms.database( DEFAULT_DATABASE_NAME );
        DefaultDatabaseManager databaseManager = getDatabaseManager( db );
        RecordStoreVersionCheck check =
                new RecordStoreVersionCheck( fs, getPageCache( db ), databaseLayout, NullLogProvider.getInstance(), Config.defaults(), CONTEXT_FACTORY );
        assertEquals( StandardV4_3.STORE_VERSION, check.storeVersion( CursorContext.NULL_CONTEXT ).orElseThrow() );

        // When
        NamedDatabaseId namedDatabaseId = db.databaseId();
        DatabaseManagementException e = assertThrows( DatabaseManagementException.class, () -> databaseManager.upgradeDatabase( namedDatabaseId ) );

        // Then
        assertThat( e ).hasMessage( "Failed to upgrade " + namedDatabaseId );
        assertThat( e ).hasRootCause( expectedException );
        assertFalse( db.isAvailable( 100 ), "Expected database to be available after upgrade" );
        assertTrue( MigrationTestUtils.checkNeoStoreHasFormatVersion( check, StandardV4_3.RECORD_FORMATS ), "Expected store not upgraded." );
    }

    private void createDbms()
    {
        TestDatabaseManagementServiceBuilder builder = new TestDatabaseManagementServiceBuilder( neo4jLayout );
        dbms = builder
                .setConfig( GraphDatabaseSettings.allow_upgrade, false )
                .setConfig( GraphDatabaseSettings.record_format, PageAligned.LATEST_NAME )
                .setUserLogProvider( userLogProvider )
                .build();
    }

    private static PageCache getPageCache( GraphDatabaseAPI db )
    {
        return db.getDependencyResolver().resolveDependency( PageCache.class );
    }

    private static DefaultDatabaseManager getDatabaseManager( GraphDatabaseAPI db )
    {
        return (DefaultDatabaseManager) db.getDependencyResolver().resolveDependency( DatabaseManager.class );
    }

    /**
     * Create a mocked user log provider that will throw provided exception
     * when trying to log on info level on LogsMigrator class.
     * This is designed to hook in on MigrationProgressMonitor.started in
     * StoreUpgrader.migrate.
     */
    private void useThrowingMigrationLogProvider( Exception e )
    {
        InternalLog mockedLog = mock( InternalLog.class, RETURNS_MOCKS );
        when( userLogProvider.getLog( LegacyDatabaseMigrator.class ) ).thenReturn( mockedLog );
        Mockito.doThrow( e ).when( mockedLog ).info( anyString() );
    }
}
