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
package org.neo4j.kernel.impl.storemigration.participant;


import com.google.common.collect.ImmutableMap;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;

import org.neo4j.kernel.impl.storemigration.StoreMigrationParticipant;
import org.neo4j.kernel.impl.storemigration.monitoring.SilentMigrationProgressMonitor;
import org.neo4j.kernel.spi.legacyindex.IndexImplementation;
import org.neo4j.logging.AssertableLogProvider;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class LegacyIndexMigratorTest
{

    private final File storeDir = mock( File.class );
    private final File migrationDir = mock( File.class );
    private final String versionToMigrateFrom = "A";
    private final String versionToMigrateTo = "B";
    private final IndexImplementation indexImplementation = mock( IndexImplementation.class );
    private final ImmutableMap<String,IndexImplementation> testIndexProvider = ImmutableMap.of( "test",
            indexImplementation );
    private final ImmutableMap<String,IndexImplementation> luceneIndexProvider = ImmutableMap.of( "lucene",
            indexImplementation );
    private final AssertableLogProvider logProvider = new AssertableLogProvider();

    @Before
    public void setUp()
    {
        logProvider.clear();
    }

    @Test
    public void migrateLegacyIndexesWhenLuceneProviderNotFound() throws Exception
    {
        LegacyIndexMigrator indexMigrator = new LegacyIndexMigrator( testIndexProvider, logProvider );

        performMigration( indexMigrator );

        logProvider.assertContainsMessageContaining( "Lucene index provider not found, will do nothing during migration." );
    }

    @Test
    public void useLuceneLegacyIndexMigratorWhenLuceneProviderFound() throws IOException
    {
        StoreMigrationParticipant migrationParticipant = Mockito.mock( StoreMigrationParticipant.class );
        when( indexImplementation.getStoreMigrator() ).thenReturn( migrationParticipant );
        LegacyIndexMigrator indexMigrator = new LegacyIndexMigrator( luceneIndexProvider, logProvider );

        performMigration( indexMigrator );

        verify( indexImplementation ).getStoreMigrator();
        verify( migrationParticipant ).migrate( storeDir, migrationDir,
                SilentMigrationProgressMonitor.NO_OP_SECTION, versionToMigrateFrom, versionToMigrateTo );
        verify( migrationParticipant ).moveMigratedFiles( migrationDir, storeDir, versionToMigrateFrom, versionToMigrateTo );
        verify( migrationParticipant ).cleanup( migrationDir );
        logProvider.assertNoLoggingOccurred();
    }

    @Test
    public void useImplementationDelegateToPerformMigration() throws IOException
    {
        StoreMigrationParticipant migrationParticipant = mock( StoreMigrationParticipant.class );
        LegacyIndexMigrator indexMigrator = new LegacyIndexMigrator( migrationParticipant );

        performMigration( indexMigrator );

        verify( migrationParticipant ).migrate( storeDir, migrationDir,
                SilentMigrationProgressMonitor.NO_OP_SECTION, versionToMigrateFrom, versionToMigrateTo );
        verify( migrationParticipant ).moveMigratedFiles( migrationDir, storeDir, versionToMigrateFrom, versionToMigrateTo );
        verify( migrationParticipant ).cleanup( migrationDir );
    }

    private void performMigration( LegacyIndexMigrator indexMigrator ) throws IOException
    {
        indexMigrator.migrate( storeDir, migrationDir, SilentMigrationProgressMonitor.NO_OP_SECTION,
                versionToMigrateFrom, versionToMigrateTo );
        indexMigrator.moveMigratedFiles( migrationDir, storeDir, versionToMigrateFrom, versionToMigrateTo );
        indexMigrator.cleanup( migrationDir );
    }
}
