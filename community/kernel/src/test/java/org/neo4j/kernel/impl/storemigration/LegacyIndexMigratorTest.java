/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.impl.storemigration;

import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.graphdb.index.IndexImplementation;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.impl.api.scan.LabelScanStoreProvider;
import org.neo4j.kernel.impl.storemigration.legacystore.v23.Legacy23Store;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.upgrade.lucene.LegacyIndexMigrationException;
import org.neo4j.upgrade.lucene.LuceneLegacyIndexUpgrader;

import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class LegacyIndexMigratorTest
{

    private FileSystemAbstraction fs = mock( FileSystemAbstraction.class );
    private LogProvider logProvider = mock( LogProvider.class );
    private SchemaIndexProvider schemaIndexProvider = mock( SchemaIndexProvider.class );
    private LabelScanStoreProvider labelScanStoreProvider = mock( LabelScanStoreProvider.class );
    private File storeDir = mock( File.class );
    private File migrationDir = mock( File.class );
    private File originalIndexStore = mock( File.class );
    private File migratedIndexStore = new File( "." );

    @Test
    public void transferOriginalDataToMigrationDirectory() throws IOException
    {
        HashMap<String,IndexImplementation> indexProviders = getIndexProviders();
        LegacyIndexMigrator indexMigrator = new TestLegacyIndexMigrator( fs, indexProviders, logProvider, true );

        indexMigrator.migrate( storeDir, migrationDir, schemaIndexProvider, labelScanStoreProvider,
                Legacy23Store.LEGACY_VERSION );

        verify( fs ).copyRecursively( originalIndexStore, migratedIndexStore );
    }

    @Test
    public void transferMigratedIndexesToStoreDirectory() throws IOException
    {
        HashMap<String,IndexImplementation> indexProviders = getIndexProviders();
        LegacyIndexMigrator indexMigrator = new TestLegacyIndexMigrator( fs, indexProviders, logProvider, true );

        indexMigrator.migrate( storeDir, migrationDir, schemaIndexProvider, labelScanStoreProvider,
                Legacy23Store.LEGACY_VERSION );
        reset( fs );

        indexMigrator.moveMigratedFiles( migrationDir, storeDir, "any" );

        verify( fs ).deleteRecursively( originalIndexStore );
        verify( fs ).copyRecursively( migratedIndexStore, originalIndexStore );
    }

    @Test
    public void logErrorWithIndexNameOnIndexMigrationException() throws IOException
    {
        Log log = mock( Log.class );
        when( logProvider.getLog( TestLegacyIndexMigrator.class ) ).thenReturn( log );

        HashMap<String,IndexImplementation> indexProviders = getIndexProviders();
        try
        {
            LegacyIndexMigrator indexMigrator = new TestLegacyIndexMigrator( fs, indexProviders, logProvider, false );
            indexMigrator.migrate( storeDir, migrationDir, schemaIndexProvider, labelScanStoreProvider,
                    Legacy23Store.LEGACY_VERSION );

            fail("Index migration should fail");
        }
        catch ( IOException e )
        {
            // ignored
        }

        verify( log ).error( eq( "Migration of legacy indexes failed. Index: testIndex can't be migrated." ),
                any( Throwable.class ) );

    }

    private HashMap<String,IndexImplementation> getIndexProviders()
    {
        HashMap<String,IndexImplementation> indexProviders = new HashMap<>();
        IndexImplementation indexImplementation = mock( IndexImplementation.class );
        indexProviders.put( "lucene", indexImplementation );

        when( indexImplementation.getStoreDirectory( storeDir ) ).thenReturn( originalIndexStore );
        when( indexImplementation.getStoreDirectory( migrationDir ) ).thenReturn( migratedIndexStore );
        return indexProviders;
    }

    private class TestLegacyIndexMigrator extends LegacyIndexMigrator
    {

        private boolean successfullMigration;

        public TestLegacyIndexMigrator( FileSystemAbstraction fileSystem,
                Map<String,IndexImplementation> indexProviders, LogProvider logProvider, boolean successfullMigration )
        {
            super( fileSystem, indexProviders, logProvider );
            this.successfullMigration = successfullMigration;
        }

        @Override
        LuceneLegacyIndexUpgrader createLuceneLegacyIndexUpgrader( Path indexRootPath )
        {
            return new HumbleLegacyIndexUpgrader( indexRootPath, successfullMigration );
        }
    }

    private class HumbleLegacyIndexUpgrader extends LuceneLegacyIndexUpgrader
    {
        private boolean successfulMigration;

        public HumbleLegacyIndexUpgrader( Path indexRootPath, boolean successfulMigration )
        {
            super( indexRootPath );
            this.successfulMigration = successfulMigration;
        }

        @Override
        public void upgradeIndexes() throws LegacyIndexMigrationException
        {
            if ( !successfulMigration )
            {
                throw new LegacyIndexMigrationException( "testIndex", "Index migration failed", null );
            }
        }
    }
}