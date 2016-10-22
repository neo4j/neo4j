/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.index.impl.lucene.legacy;

import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.index.IndexConfigStore;
import org.neo4j.kernel.impl.index.IndexEntityType;
import org.neo4j.kernel.impl.store.format.standard.StandardV2_3;
import org.neo4j.kernel.impl.store.format.standard.StandardV3_0;
import org.neo4j.kernel.impl.store.format.standard.StandardV3_0_7;
import org.neo4j.kernel.impl.storemigration.monitoring.MigrationProgressMonitor;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.upgrade.lucene.LegacyIndexMigrationException;
import org.neo4j.upgrade.lucene.LuceneLegacyIndexUpgrader;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class LuceneLegacyIndexMigratorTest
{

    private final FileSystemAbstraction fs = mock( FileSystemAbstraction.class );
    private final LogProvider logProvider = mock( LogProvider.class );
    private final MigrationProgressMonitor.Section progressMonitor = mock( MigrationProgressMonitor.Section.class );
    private final LegacyIndexFieldsRestorer fieldsRestorer = mock( LegacyIndexFieldsRestorer.class );

    private final File emptyIndexStoreDir = new File( "" );
    private final File storeDir = new File( "storeDir" );
    private final File migrationDir = new File( "migrationDir" );
    private final File originalIndexStore = mock( File.class );
    private final File migratedIndexStore = new File( "." );

    private final IndexRootDirectoryMatcher indexRootDirectoryMatcher = new IndexRootDirectoryMatcher( storeDir,
        "index" );
    private final IndexRootDirectoryMatcher indexMigrationDirectoryMatcher = new IndexRootDirectoryMatcher(
            migrationDir, "index" );
    private final IndexRootDirectoryMatcher fieldsMigrationDirectoryMatcher = new IndexRootDirectoryMatcher(
            migrationDir, "sortFieldUpgrade" );

    @Before
    public void setUp()
    {
        when( fs.isDirectory( argThat( indexRootDirectoryMatcher ) ) ).thenReturn( true );
        when( fs.listFiles( argThat( indexRootDirectoryMatcher ) ) ).thenReturn( new File[]{mock( File.class )} );
        when( fs.fileExists( argThat( indexMigrationDirectoryMatcher ) ) ).thenReturn( true );
        when( fs.mkdir( any( File.class ) ) ).thenReturn( true );
    }

    @Test
    public void skipEmptyIndexStorageMigration() throws IOException
    {
        when( fs.isDirectory( argThat( indexRootDirectoryMatcher ) ) ).thenReturn( false );

        NumberOfIndexesAwareLuceneMigrator indexMigrator = new NumberOfIndexesAwareLuceneMigrator( fs, fieldsRestorer, logProvider );

        indexMigrator.migrate( emptyIndexStoreDir, migrationDir, progressMonitor, StandardV2_3.STORE_VERSION, StandardV3_0_7.STORE_VERSION );
        indexMigrator.moveMigratedFiles( migrationDir, emptyIndexStoreDir, StandardV2_3.STORE_VERSION, StandardV3_0_7.STORE_VERSION );
        indexMigrator.cleanup( migrationDir );

        verify( fieldsRestorer, never() ).restoreIndexSortFields( storeDir, emptyIndexStoreDir, migrationDir, progressMonitor );
        verify( fs, never() ).isDirectory( migratedIndexStore );
        verify( fs, never() ).deleteRecursively( originalIndexStore );
        verify( fs, never() ).moveToDirectory( migratedIndexStore, emptyIndexStoreDir );
        verify( fs, never() ).copyRecursively( migratedIndexStore, emptyIndexStoreDir );
    }

    @Test
    public void reportFailedIndexName() throws IOException
    {

        File file = Paths.get( storeDir.toPath().toString(), "index", "lucene", "node" ).toFile();
        when( fs.isDirectory( argThat( new IndexRootDirectoryMatcher( file, "testIndex" ) ) ) )
                .thenThrow( new RuntimeException( "something went wrong" ) );

        AssertableLogProvider logProvider = new AssertableLogProvider();
        ThrowingLegacyIndexRecoverer restorer = new ThrowingLegacyIndexRecoverer( Config.defaults(), fs, logProvider );
        NumberOfIndexesAwareLuceneMigrator indexMigrator = new NumberOfIndexesAwareLuceneMigrator( fs, restorer, logProvider );

        try
        {
            indexMigrator.migrate( storeDir, migrationDir, progressMonitor, StandardV3_0.STORE_VERSION, StandardV3_0_7.STORE_VERSION );
            fail( "Exception on migration should be thrown." );
        }
        catch ( IOException e )
        {
            logProvider.assertContainsMessageContaining( "Migration of legacy indexes failed. Index: testIndex can't be migrated." );
            assertThat( "Should be legacy index migration exception.", e.getCause(),
                    instanceOf( LegacyIndexMigrationException.class ) );}
    }

    @Test
    public void restoreIndexFieldsWhenVersionIsAffectedAndFormatCapabilitiesIsTheSame() throws IOException
    {
        NumberOfIndexesAwareLuceneMigrator indexMigrator = new NumberOfIndexesAwareLuceneMigrator( fs, fieldsRestorer, logProvider );

        performMigration( indexMigrator, StandardV3_0.STORE_VERSION, StandardV3_0_7.STORE_VERSION );

        verify( fieldsRestorer ).restoreIndexSortFields( storeDir, storeDir, migrationDir, progressMonitor );
    }

    @Test
    public void restoreIndexFieldsWhenFormatsHaveDifferentCapabilities() throws IOException
    {
        NumberOfIndexesAwareLuceneMigrator indexMigrator = new NumberOfIndexesAwareLuceneMigrator( fs, fieldsRestorer, logProvider );

        performMigration( indexMigrator, StandardV2_3.STORE_VERSION, StandardV3_0_7.STORE_VERSION );

        verify( fieldsRestorer ).restoreIndexSortFields( eq( storeDir ), eq( migrationDir ),
                argThat( fieldsMigrationDirectoryMatcher ), eq( progressMonitor ) );
    }

    @Test
    public void transferOriginalDataToMigrationDirectory() throws IOException
    {
        NumberOfIndexesAwareLuceneMigrator indexMigrator = new NumberOfIndexesAwareLuceneMigrator( fs, fieldsRestorer, logProvider );

        performMigration( indexMigrator, StandardV2_3.STORE_VERSION, StandardV3_0_7.STORE_VERSION );

        verify( fs ).copyRecursively( argThat( indexRootDirectoryMatcher ), argThat( indexMigrationDirectoryMatcher ) );
    }

    @Test
    public void transferMigratedIndexesToStoreDirectory() throws IOException
    {
        NumberOfIndexesAwareLuceneMigrator indexMigrator = new NumberOfIndexesAwareLuceneMigrator( fs, fieldsRestorer, logProvider );

        performMigration( indexMigrator, StandardV2_3.STORE_VERSION, StandardV3_0.STORE_VERSION );

        verify( fs ).deleteRecursively( argThat( indexRootDirectoryMatcher ) );
        verify( fs ).moveToDirectory( argThat( indexMigrationDirectoryMatcher ), eq( storeDir) );
    }

    @Test
    public void logErrorWithIndexNameOnIndexMigrationException() throws IOException
    {
        Log log = mock( Log.class );
        when( logProvider.getLog( TestLuceneLegacyIndexMigrator.class ) ).thenReturn( log );

        try
        {
            TestLuceneLegacyIndexMigrator indexMigrator = new TestLuceneLegacyIndexMigrator( fs, fieldsRestorer, logProvider );
            indexMigrator.migrate( storeDir, migrationDir, progressMonitor, StandardV2_3.STORE_VERSION,
                    StandardV3_0.STORE_VERSION );

            fail( "Index migration should fail" );
        }
        catch ( IOException e )
        {
            // ignored
        }
        verify( log ).error( eq( "Migration of legacy indexes failed. Index: testIndex can't be migrated." ),
                any( Throwable.class ) );
    }

    @Test
    public void cleanupMigrationDirectory() throws IOException
    {
        when( fs.fileExists( migratedIndexStore ) ).thenReturn( true );

        NumberOfIndexesAwareLuceneMigrator indexMigrator = new NumberOfIndexesAwareLuceneMigrator( fs, fieldsRestorer, logProvider );
        performMigration( indexMigrator, StandardV2_3.STORE_VERSION, StandardV3_0.STORE_VERSION );

        verify( fs, times( 2 ) ).deleteRecursively( argThat( indexMigrationDirectoryMatcher ) );
    }

    private void performMigration( LuceneLegacyIndexMigrator indexMigrator, String versionToMigrateFrom,
            String versionToMigrateTo ) throws IOException
    {
        indexMigrator.migrate( storeDir, migrationDir, progressMonitor, versionToMigrateFrom, versionToMigrateTo );
        indexMigrator.moveMigratedFiles( storeDir, migrationDir, versionToMigrateFrom, versionToMigrateTo );
        indexMigrator.cleanup( migrationDir );
    }

    private class NumberOfIndexesAwareLuceneMigrator extends LuceneLegacyIndexMigrator
    {

        NumberOfIndexesAwareLuceneMigrator( FileSystemAbstraction fileSystem, LegacyIndexFieldsRestorer fieldsRestorer,
                LogProvider logProvider )
        {
            super( fileSystem, fieldsRestorer, logProvider );
        }

        @Override
        long getNumberOfIndexes() throws IOException
        {
            return 1;
        }
    }

    private class TestLuceneLegacyIndexMigrator extends NumberOfIndexesAwareLuceneMigrator
    {

        TestLuceneLegacyIndexMigrator( FileSystemAbstraction fileSystem, LegacyIndexFieldsRestorer fieldsRestorer,
                LogProvider logProvider )
        {
            super( fileSystem, fieldsRestorer, logProvider );
        }

        @Override
        LuceneLegacyIndexUpgrader createLuceneLegacyIndexUpgrader( Path indexRootPath,
                MigrationProgressMonitor.Section progressMonitor )
        {
            return prepareUpgrader();
        }

        private LuceneLegacyIndexUpgrader prepareUpgrader()
        {
            try
            {
                LuceneLegacyIndexUpgrader upgrader = mock( LuceneLegacyIndexUpgrader.class );
                doThrow( new LegacyIndexMigrationException( "testIndex", "Index " + "migration failed", null ) )
                        .when( upgrader ).upgradeIndexes();
                return upgrader;
            }
            catch ( Exception e )
            {
                throw new RuntimeException( e );
            }
        }
    }

    private class ThrowingLegacyIndexRecoverer extends LegacyIndexFieldsRestorer
    {

        ThrowingLegacyIndexRecoverer( Config config, FileSystemAbstraction fileSystem, LogProvider logProvider )
        {
            super( config, fileSystem, logProvider );
        }

        @Override
        void migrateIndexes( File storeDir, MigrationProgressMonitor.Section progressMonitor,
                IndexConfigStore configStore, LuceneDataSource sourceDataSource, LuceneDataSource targetDataSource,
                String[] indexes, IndexEntityType entityType, Class<? extends PropertyContainer> entityClass )
                throws IOException, LegacyIndexMigrationException
        {
            super.migrateIndexes( storeDir, progressMonitor, configStore, sourceDataSource, targetDataSource,
                    new String[] {"testIndex"}, IndexEntityType.Node, Node.class );
        }
    }

    private class IndexRootDirectoryMatcher extends TypeSafeMatcher<File>
    {

        private final File directory;
        private final String childFolderName;
        private File file;

        IndexRootDirectoryMatcher( File directory, String childFolderName )
        {
            this.directory = directory;
            this.childFolderName = childFolderName;
        }

        @Override
        protected boolean matchesSafely( File file )
        {
            this.file = file;
            return getExpectedPath().equals( file.toPath() );
        }

        private Path getExpectedPath()
        {
            return Paths.get( directory.getPath(), childFolderName );
        }

        @Override
        public void describeTo( Description description )
        {
            description.appendText( "expected: " ).appendValue( getExpectedPath() )
                    .appendText( " but was: " ).appendValue( file.toPath() );
        }
    }
}
