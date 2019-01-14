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
package org.neo4j.kernel.impl.storemigration.participant;

import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.impl.api.ExplicitIndexProvider;
import org.neo4j.kernel.impl.store.format.standard.StandardV2_3;
import org.neo4j.kernel.impl.store.format.standard.StandardV3_0;
import org.neo4j.kernel.impl.storemigration.StoreUpgrader;
import org.neo4j.kernel.impl.util.monitoring.ProgressReporter;
import org.neo4j.kernel.spi.explicitindex.IndexImplementation;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.upgrade.lucene.ExplicitIndexMigrationException;
import org.neo4j.upgrade.lucene.LuceneExplicitIndexUpgrader;

import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ExplicitIndexMigratorTest
{
    private final FileSystemAbstraction fs = mock( FileSystemAbstraction.class );
    private final LogProvider logProvider = mock( LogProvider.class );
    private final ProgressReporter progressMonitor = mock( ProgressReporter.class );
    private final DatabaseLayout storeLayout = DatabaseLayout.of( new File( GraphDatabaseSettings.DEFAULT_DATABASE_NAME ) );
    private final DatabaseLayout migrationLayout = DatabaseLayout.of( new File( StoreUpgrader.MIGRATION_DIRECTORY ) );
    private final File originalIndexStore = mock( File.class );
    private final File migratedIndexStore = new File( "." );

    @Before
    public void setUp()
    {
        when( originalIndexStore.getParentFile() ).thenReturn( storeLayout.databaseDirectory() );
        when( fs.isDirectory( originalIndexStore ) ).thenReturn( true );
        when( fs.listFiles( originalIndexStore ) ).thenReturn( new File[]{mock( File.class )} );
    }

    @Test
    public void skipEmptyIndexStorageMigration() throws IOException
    {
        when( fs.listFiles( originalIndexStore ) ).thenReturn( null );

        ExplicitIndexProvider indexProviders = getExplicitIndexProvider();
        ExplicitIndexMigrator indexMigrator = new TestExplicitIndexMigrator( fs, indexProviders, logProvider, true );

        indexMigrator.migrate( storeLayout, migrationLayout, progressMonitor, StandardV2_3.STORE_VERSION,
                StandardV3_0.STORE_VERSION );

        verify( fs, never() ).deleteRecursively( originalIndexStore );
        verify( fs, never() ).moveToDirectory( migratedIndexStore, storeLayout.databaseDirectory() );
    }

    @Test
    public void transferOriginalDataToMigrationDirectory() throws IOException
    {
        ExplicitIndexProvider indexProviders = getExplicitIndexProvider();
        ExplicitIndexMigrator indexMigrator = new TestExplicitIndexMigrator( fs, indexProviders, logProvider, true );

        indexMigrator.migrate( storeLayout, migrationLayout, progressMonitor, StandardV2_3.STORE_VERSION,
                StandardV3_0.STORE_VERSION );

        verify( fs ).copyRecursively( originalIndexStore, migratedIndexStore );
    }

    @Test
    public void transferMigratedIndexesToStoreDirectory() throws IOException
    {
        ExplicitIndexProvider indexProviders = getExplicitIndexProvider();
        ExplicitIndexMigrator indexMigrator = new TestExplicitIndexMigrator( fs, indexProviders, logProvider, true );

        indexMigrator.migrate( storeLayout, migrationLayout, progressMonitor, StandardV2_3.STORE_VERSION,
                StandardV3_0.STORE_VERSION );
        reset( fs );

        indexMigrator.moveMigratedFiles( migrationLayout, storeLayout, "any", "any" );

        verify( fs ).deleteRecursively( originalIndexStore );
        verify( fs ).moveToDirectory( migratedIndexStore, storeLayout.databaseDirectory() );
    }

    @Test
    public void logErrorWithIndexNameOnIndexMigrationException()
    {
        Log log = mock( Log.class );
        when( logProvider.getLog( TestExplicitIndexMigrator.class ) ).thenReturn( log );

        ExplicitIndexProvider indexProviders = getExplicitIndexProvider();
        try
        {
            ExplicitIndexMigrator indexMigrator = new TestExplicitIndexMigrator( fs, indexProviders, logProvider, false );
            indexMigrator.migrate( storeLayout, migrationLayout, progressMonitor, StandardV2_3.STORE_VERSION,
                    StandardV3_0.STORE_VERSION );

            fail( "Index migration should fail" );
        }
        catch ( IOException e )
        {
            // ignored
        }

        verify( log ).error( eq( "Migration of explicit indexes failed. Index: testIndex can't be migrated." ),
                any( Throwable.class ) );
    }

    @Test
    public void cleanupMigrationDirectory() throws IOException
    {
        when( fs.fileExists( migratedIndexStore ) ).thenReturn( true );

        ExplicitIndexProvider indexProviders = getExplicitIndexProvider();
        ExplicitIndexMigrator indexMigrator = new TestExplicitIndexMigrator( fs, indexProviders, logProvider, true );
        indexMigrator.migrate( storeLayout, migrationLayout, progressMonitor, StandardV2_3.STORE_VERSION,
                StandardV3_0.STORE_VERSION );
        indexMigrator.cleanup( migrationLayout );

        verify( fs ).deleteRecursively( migratedIndexStore );
    }

    private ExplicitIndexProvider getExplicitIndexProvider()
    {
        IndexImplementation indexImplementation = mock( IndexImplementation.class );

        when( indexImplementation.getIndexImplementationDirectory( storeLayout ) ).thenReturn( originalIndexStore );
        when( indexImplementation.getIndexImplementationDirectory( migrationLayout ) ).thenReturn( migratedIndexStore );

        ExplicitIndexProvider explicitIndexProvider = mock( ExplicitIndexProvider.class );
        when( explicitIndexProvider.getProviderByName( "lucene" ) ).thenReturn( indexImplementation );
        return explicitIndexProvider;
    }

    private class TestExplicitIndexMigrator extends ExplicitIndexMigrator
    {

        private final boolean successfulMigration;

        TestExplicitIndexMigrator( FileSystemAbstraction fileSystem, ExplicitIndexProvider explicitIndexProvider,
                LogProvider logProvider, boolean successfulMigration )
        {
            super( fileSystem, explicitIndexProvider, logProvider );
            this.successfulMigration = successfulMigration;
        }

        @Override
        LuceneExplicitIndexUpgrader createLuceneExplicitIndexUpgrader( Path indexRootPath,
                ProgressReporter progressReporter )
        {
            return new HumbleExplicitIndexUpgrader( indexRootPath, successfulMigration );
        }
    }

    private class HumbleExplicitIndexUpgrader extends LuceneExplicitIndexUpgrader
    {
        private final boolean successfulMigration;

        HumbleExplicitIndexUpgrader( Path indexRootPath, boolean successfulMigration )
        {
            super( indexRootPath, NO_MONITOR );
            this.successfulMigration = successfulMigration;
        }

        @Override
        public void upgradeIndexes() throws ExplicitIndexMigrationException
        {
            if ( !successfulMigration )
            {
                throw new ExplicitIndexMigrationException( "testIndex", "Index migration failed", null );
            }
        }
    }
}
