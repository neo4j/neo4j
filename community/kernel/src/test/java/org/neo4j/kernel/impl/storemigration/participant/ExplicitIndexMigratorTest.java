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
import java.util.HashMap;
import java.util.Map;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.store.format.standard.StandardV2_3;
import org.neo4j.kernel.impl.store.format.standard.StandardV3_0;
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
    private final File storeDir = mock( File.class );
    private final File migrationDir = mock( File.class );
    private final File originalIndexStore = mock( File.class );
    private final File migratedIndexStore = new File( "." );

    @Before
    public void setUp()
    {
        when( originalIndexStore.getParentFile() ).thenReturn( storeDir );
        when( fs.isDirectory( originalIndexStore ) ).thenReturn( true );
        when( fs.listFiles( originalIndexStore ) ).thenReturn( new File[]{mock( File.class )} );
    }

    @Test
    public void skipEmptyIndexStorageMigration() throws IOException
    {
        when( fs.listFiles( originalIndexStore ) ).thenReturn( null );

        HashMap<String,IndexImplementation> indexProviders = getIndexProviders();
        ExplicitIndexMigrator indexMigrator = new TestExplicitIndexMigrator( fs, indexProviders, logProvider, true );

        indexMigrator.migrate( storeDir, migrationDir, progressMonitor, StandardV2_3.STORE_VERSION,
                StandardV3_0.STORE_VERSION );

        verify( fs, never() ).deleteRecursively( originalIndexStore );
        verify( fs, never() ).moveToDirectory( migratedIndexStore, storeDir );
    }

    @Test
    public void transferOriginalDataToMigrationDirectory() throws IOException
    {
        HashMap<String,IndexImplementation> indexProviders = getIndexProviders();
        ExplicitIndexMigrator indexMigrator = new TestExplicitIndexMigrator( fs, indexProviders, logProvider, true );

        indexMigrator.migrate( storeDir, migrationDir, progressMonitor, StandardV2_3.STORE_VERSION,
                StandardV3_0.STORE_VERSION );

        verify( fs ).copyRecursively( originalIndexStore, migratedIndexStore );
    }

    @Test
    public void transferMigratedIndexesToStoreDirectory() throws IOException
    {
        HashMap<String,IndexImplementation> indexProviders = getIndexProviders();
        ExplicitIndexMigrator indexMigrator = new TestExplicitIndexMigrator( fs, indexProviders, logProvider, true );

        indexMigrator.migrate( storeDir, migrationDir, progressMonitor, StandardV2_3.STORE_VERSION,
                StandardV3_0.STORE_VERSION );
        reset( fs );

        indexMigrator.moveMigratedFiles( migrationDir, storeDir, "any", "any" );

        verify( fs ).deleteRecursively( originalIndexStore );
        verify( fs ).moveToDirectory( migratedIndexStore, storeDir );
    }

    @Test
    public void logErrorWithIndexNameOnIndexMigrationException()
    {
        Log log = mock( Log.class );
        when( logProvider.getLog( TestExplicitIndexMigrator.class ) ).thenReturn( log );

        HashMap<String,IndexImplementation> indexProviders = getIndexProviders();
        try
        {
            ExplicitIndexMigrator indexMigrator = new TestExplicitIndexMigrator( fs, indexProviders, logProvider, false );
            indexMigrator.migrate( storeDir, migrationDir, progressMonitor, StandardV2_3.STORE_VERSION,
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

        HashMap<String,IndexImplementation> indexProviders = getIndexProviders();
        ExplicitIndexMigrator indexMigrator = new TestExplicitIndexMigrator( fs, indexProviders, logProvider, true );
        indexMigrator.migrate( storeDir, migrationDir, progressMonitor, StandardV2_3.STORE_VERSION,
                StandardV3_0.STORE_VERSION );
        indexMigrator.cleanup( migrationDir );

        verify( fs ).deleteRecursively( migratedIndexStore );
    }

    private HashMap<String,IndexImplementation> getIndexProviders()
    {
        HashMap<String,IndexImplementation> indexProviders = new HashMap<>();
        IndexImplementation indexImplementation = mock( IndexImplementation.class );
        indexProviders.put( "lucene", indexImplementation );

        when( indexImplementation.getIndexImplementationDirectory( storeDir ) ).thenReturn( originalIndexStore );
        when( indexImplementation.getIndexImplementationDirectory( migrationDir ) ).thenReturn( migratedIndexStore );

        return indexProviders;
    }

    private class TestExplicitIndexMigrator extends ExplicitIndexMigrator
    {

        private final boolean successfullMigration;

        TestExplicitIndexMigrator( FileSystemAbstraction fileSystem, Map<String,IndexImplementation> indexProviders,
                LogProvider logProvider, boolean successfullMigration )
        {
            super( fileSystem, indexProviders, logProvider );
            this.successfullMigration = successfullMigration;
        }

        @Override
        LuceneExplicitIndexUpgrader createLuceneExplicitIndexUpgrader( Path indexRootPath,
                ProgressReporter progressReporter )
        {
            return new HumbleExplicitIndexUpgrader( indexRootPath, successfullMigration );
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
