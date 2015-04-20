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

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import org.neo4j.helpers.UTF8;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.storemigration.legacystore.v19.Legacy19Store;
import org.neo4j.kernel.impl.storemigration.legacystore.v20.Legacy20Store;
import org.neo4j.kernel.impl.storemigration.legacystore.v21.Legacy21Store;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.kernel.impl.storemigration.MigrationTestUtils.changeVersionNumber;
import static org.neo4j.kernel.impl.storemigration.MigrationTestUtils.truncateFile;
import static org.neo4j.kernel.impl.storemigration.MigrationTestUtils.truncateToFixedLength;

@RunWith( Enclosed.class )
public class UpgradableDatabaseTest
{
    private static final FileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction();

    @RunWith(Parameterized.class)
    public static class SupportedVersions
    {
        @Parameterized.Parameter( 0 )
        public String version;
        private File workingDirectory;

        @Parameterized.Parameters( name = "{0}" )
        public static Collection<Object[]> versions()
        {
            return Arrays.asList(
                    new Object[]{Legacy19Store.LEGACY_VERSION},
                    new Object[]{Legacy20Store.LEGACY_VERSION},
                    new Object[]{Legacy21Store.LEGACY_VERSION}
            );
        }

        @Before
        public void setup() throws IOException
        {
            workingDirectory = MigrationTestUtils.findFormatStoreDirectoryForVersion( version );
        }

        @Test
        public void shouldAcceptTheStoresInTheSampleDatabaseAsBeingEligibleForUpgrade() throws IOException
        {
            // given
            final UpgradableDatabase upgradableDatabase = new UpgradableDatabase( new StoreVersionCheck( fileSystem ) );

            // when
            final boolean result = upgradableDatabase.storeFilesUpgradeable( workingDirectory );

            // then
            assertTrue( result );
        }

        @Test
        public void shouldRejectStoresIfOneFileHasIncorrectVersion() throws IOException
        {
            // given
            changeVersionNumber( fileSystem, new File( workingDirectory, "neostore.nodestore.db" ), "v0.9.5" );
            final UpgradableDatabase upgradableDatabase = new UpgradableDatabase( new StoreVersionCheck( fileSystem ) );

            // when
            final boolean result = upgradableDatabase.storeFilesUpgradeable( workingDirectory );

            // then
            assertFalse( result );
        }

        @Test
        public void shouldRejectStoresIfOneFileHasNoVersionAsIfNotShutDownCleanly() throws IOException
        {
            // given
            final File storeFile = new File( workingDirectory, "neostore.nodestore.db" );
            truncateFile( fileSystem, storeFile, "StringPropertyStore " + version );
            final UpgradableDatabase upgradableDatabase = new UpgradableDatabase( new StoreVersionCheck( fileSystem ) );

            // when
            final boolean result = upgradableDatabase.storeFilesUpgradeable( workingDirectory );

            // then
            assertFalse( result );
        }

        @Test
        public void shouldDetectOldVersionAsDifferentFromCurrent() throws Exception
        {
            // given
            final UpgradableDatabase upgradableDatabase = new UpgradableDatabase( new StoreVersionCheck( fileSystem ) );

            // when
            boolean currentVersion = upgradableDatabase.hasCurrentVersion( fileSystem, workingDirectory );

            // then
            assertFalse( currentVersion );
        }

        @Test
        public void shouldRejectStoresIfOneFileShorterThanExpectedVersionString() throws IOException
        {
            // given
            final int shortFileLength = 5 /* (RelationshipTypeStore.RECORD_SIZE) */ * 3;
            assertTrue( shortFileLength < UTF8.encode( "StringPropertyStore " + version ).length );
            truncateToFixedLength( fileSystem, new File( workingDirectory, "neostore.relationshiptypestore.db" ),
                    shortFileLength );
            final UpgradableDatabase upgradableDatabase = new UpgradableDatabase( new StoreVersionCheck( fileSystem ) );

            // when
            final boolean result = upgradableDatabase.storeFilesUpgradeable( workingDirectory );

            // then
            assertFalse( result );
        }
    }

    @RunWith( Parameterized.class )
    public static class UnsupportedVersions
    {
        @Parameterized.Parameter( 0 )
        public String version;
        private File workingDirectory;
        private static final String neostoreFilename = "neostore.nodestore.db";

        @Parameterized.Parameters( name = "{0}" )
        public static Collection<Object[]> versions()
        {
            return Arrays.asList(
                    new Object[]{"v0.9.5"},
                    new Object[]{"v0.A.4"}
            );
        }

        @Before
        public void setup() throws IOException
        {
            // doesn't matter which version we pick we are changing it to the wrong one...
            workingDirectory = MigrationTestUtils.findFormatStoreDirectoryForVersion( Legacy21Store.LEGACY_VERSION );
            changeVersionNumber( fileSystem, new File( workingDirectory, neostoreFilename ), version );
        }

        @Test
        public void shouldDetectOldVersionAsDifferentFromCurrent() throws Exception
        {
            // given
            final UpgradableDatabase upgradableDatabase = new UpgradableDatabase( new StoreVersionCheck( fileSystem ) );

            // when
            boolean currentVersion = upgradableDatabase.hasCurrentVersion( fileSystem, workingDirectory );

            // then
            assertFalse( currentVersion );
        }

        @Test
        public void shouldCommunicateWhatCausesInabilityToUpgrade() throws IOException
        {
            // given
            final UpgradableDatabase upgradableDatabase = new UpgradableDatabase( new StoreVersionCheck( fileSystem ) );
            try
            {
                // when
                upgradableDatabase.checkUpgradeable( workingDirectory );
                fail( "should not have been able to upgrade" );
            }
            catch ( StoreUpgrader.UnexpectedUpgradingStoreVersionException e )
            {
                // then
                final String expected = "'" + neostoreFilename + "' has a store version number that we cannot upgrade from. " +
                                        "Expected '" + Legacy21Store.LEGACY_VERSION + "' but file is version 'NodeStore " + version + "'.";
                assertThat( e.getMessage(), is( expected ) );
            }
        }
    }
}
