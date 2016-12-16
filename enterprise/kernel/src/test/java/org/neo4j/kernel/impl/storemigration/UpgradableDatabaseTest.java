/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.storemigration;

import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.rules.RuleChain;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.format.RecordFormatSelector;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.format.StoreVersion;
import org.neo4j.kernel.impl.store.format.standard.Standard;
import org.neo4j.kernel.impl.store.format.standard.StandardFormatFamily;
import org.neo4j.kernel.impl.store.format.standard.StandardV2_0;
import org.neo4j.kernel.impl.store.format.standard.StandardV2_1;
import org.neo4j.kernel.impl.store.format.standard.StandardV2_2;
import org.neo4j.kernel.impl.store.format.standard.StandardV2_3;
import org.neo4j.kernel.impl.storemigration.legacystore.LegacyStoreVersionCheck;
import org.neo4j.kernel.internal.Version;
import org.neo4j.string.UTF8;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.kernel.impl.store.MetaDataStore.Position.STORE_VERSION;
import static org.neo4j.kernel.impl.storemigration.MigrationTestUtils.changeVersionNumber;
import static org.neo4j.kernel.impl.storemigration.MigrationTestUtils.removeCheckPointFromTxLog;
import static org.neo4j.kernel.impl.storemigration.MigrationTestUtils.truncateToFixedLength;
import static org.neo4j.kernel.impl.storemigration.StoreUpgrader.UnexpectedUpgradingStoreVersionException.MESSAGE;

@RunWith( Enclosed.class )
public class UpgradableDatabaseTest
{
    @RunWith( Parameterized.class )
    public static class SupportedVersions
    {

        private final TestDirectory testDirectory = TestDirectory.testDirectory();
        private final PageCacheRule pageCacheRule = new PageCacheRule();
        private final DefaultFileSystemRule fileSystemRule = new DefaultFileSystemRule();

        @Rule
        public RuleChain ruleChain = RuleChain.outerRule( testDirectory )
                                              .around( fileSystemRule ).around( pageCacheRule );

        private File workingDirectory;
        private FileSystemAbstraction fileSystem;

        @Parameterized.Parameter( 0 )
        public String version;

        @Parameterized.Parameters( name = "{0}" )
        public static Collection<String> versions()
        {
            return Arrays.asList(
                    StandardV2_0.STORE_VERSION,
                    StandardV2_1.STORE_VERSION,
                    StandardV2_2.STORE_VERSION,
                    StandardV2_3.STORE_VERSION
            );
        }

        @Before
        public void setup() throws IOException
        {
            fileSystem = fileSystemRule.get();
            workingDirectory = testDirectory.graphDbDir();
            MigrationTestUtils.findFormatStoreDirectoryForVersion( version, workingDirectory );
        }

        boolean storeFilesUpgradeable( File storeDirectory, UpgradableDatabase upgradableDatabase )
        {
            try
            {
                upgradableDatabase.checkUpgradeable( storeDirectory );
                return true;
            }
            catch ( StoreUpgrader.UnableToUpgradeException e )
            {
                return false;
            }
        }

        @Test
        public void shouldAcceptTheStoresInTheSampleDatabaseAsBeingEligibleForUpgrade()
        {
            // given
            final UpgradableDatabase upgradableDatabase = new UpgradableDatabase( fileSystem,
                    new StoreVersionCheck( pageCacheRule.getPageCache( fileSystem ) ),
                    new LegacyStoreVersionCheck( fileSystem ), getRecordFormat() );

            // when
            final boolean result = storeFilesUpgradeable( workingDirectory, upgradableDatabase );

            // then
            assertTrue( result );
        }

        @Test
        public void shouldRejectStoresIfOneFileHasIncorrectVersion() throws IOException
        {
            // there are no store trailers in 2.3
            Assume.assumeFalse( StandardV2_3.STORE_VERSION.equals( version ) );

            // given
            changeVersionNumber( fileSystem, new File( workingDirectory, "neostore.nodestore.db" ), "v0.9.5" );
            final UpgradableDatabase upgradableDatabase = new UpgradableDatabase( fileSystem,
                    new StoreVersionCheck( pageCacheRule.getPageCache( fileSystem ) ),
                    new LegacyStoreVersionCheck( fileSystem ), getRecordFormat() );

            // when
            final boolean result = storeFilesUpgradeable( workingDirectory, upgradableDatabase );

            // then
            assertFalse( result );
        }

        @Test
        public void shouldDetectOldVersionAsDifferentFromCurrent() throws Exception
        {
            // given
            final UpgradableDatabase upgradableDatabase = new UpgradableDatabase( fileSystem,
                    new StoreVersionCheck( pageCacheRule.getPageCache( fileSystem ) ),
                    new LegacyStoreVersionCheck( fileSystem ), getRecordFormat() );

            // when
            boolean currentVersion = upgradableDatabase.hasCurrentVersion( workingDirectory );

            // then
            assertFalse( currentVersion );
        }

        @Test
        public void shouldRejectStoresIfOneFileShorterThanExpectedVersionString() throws IOException
        {
            // there are no store trailers in 2.3
            Assume.assumeFalse( StandardV2_3.STORE_VERSION.equals( version ) );

            // given
            final int shortFileLength = 5 /* (RelationshipTypeStore.RECORD_SIZE) */ * 3;
            assertTrue( shortFileLength < UTF8.encode( "StringPropertyStore " + version ).length );
            truncateToFixedLength( fileSystem, new File( workingDirectory, "neostore.relationshiptypestore.db" ),
                    shortFileLength );
            final UpgradableDatabase upgradableDatabase = new UpgradableDatabase( fileSystem,
                    new StoreVersionCheck( pageCacheRule.getPageCache( fileSystem ) ),
                    new LegacyStoreVersionCheck( fileSystem ), getRecordFormat() );

            // when
            final boolean result = storeFilesUpgradeable( workingDirectory, upgradableDatabase );

            // then
            assertFalse( result );
        }

        @Test
        public void shouldRejectStoresIfDBIsNotShutdownCleanly() throws IOException
        {
            // checkpoint has been introduced in 2.3
            Assume.assumeTrue( StandardV2_3.STORE_VERSION.equals( version ) );

            // given
            removeCheckPointFromTxLog( fileSystem, workingDirectory );
            final UpgradableDatabase upgradableDatabase = new UpgradableDatabase( fileSystem,
                    new StoreVersionCheck( pageCacheRule.getPageCache( fileSystem ) ),
                    new LegacyStoreVersionCheck( fileSystem ), getRecordFormat() );

            // when
            final boolean result = storeFilesUpgradeable( workingDirectory, upgradableDatabase );

            // then
            assertFalse( result );
        }
    }

    @RunWith( Parameterized.class )
    public static class UnsupportedVersions
    {
        private static final String neostoreFilename = "neostore";

        private final TestDirectory testDirectory = TestDirectory.testDirectory();
        private final PageCacheRule pageCacheRule = new PageCacheRule();
        private final DefaultFileSystemRule fileSystemRule = new DefaultFileSystemRule();

        @Rule
        public RuleChain ruleChain = RuleChain.outerRule( testDirectory )
                .around( fileSystemRule ).around( pageCacheRule );

        private File workingDirectory;
        private FileSystemAbstraction fileSystem;

        @Parameterized.Parameter( 0 )
        public String version;

        @Parameterized.Parameters( name = "{0}" )
        public static Collection<String> versions()
        {
            return Arrays.asList( "v0.A.4", StoreVersion.HIGH_LIMIT_V3_0_0.versionString() );
        }

        @Before
        public void setup() throws IOException
        {
            fileSystem = fileSystemRule.get();
            workingDirectory = testDirectory.graphDbDir();
            // doesn't matter which version we pick we are changing it to the wrong one...
            MigrationTestUtils.findFormatStoreDirectoryForVersion( StandardV2_1.STORE_VERSION, workingDirectory );
            changeVersionNumber( fileSystem, new File( workingDirectory, neostoreFilename ), version );
            File metadataStore = new File( workingDirectory, MetaDataStore.DEFAULT_NAME );
            MetaDataStore.setRecord( pageCacheRule.getPageCache( fileSystem ), metadataStore, STORE_VERSION,
                    MetaDataStore.versionStringToLong( version ) );
        }

        @Test
        public void shouldDetectOldVersionAsDifferentFromCurrent() throws Exception
        {
            // given
            final UpgradableDatabase upgradableDatabase = new UpgradableDatabase( fileSystem,
                    new StoreVersionCheck( pageCacheRule.getPageCache( fileSystem ) ),
                    new LegacyStoreVersionCheck( fileSystem ), getRecordFormat() );

            // when
            boolean currentVersion = upgradableDatabase.hasCurrentVersion( workingDirectory );

            // then
            assertFalse( currentVersion );
        }

        @Test
        public void shouldCommunicateWhatCausesInabilityToUpgrade()
        {
            // given
            final UpgradableDatabase upgradableDatabase = new UpgradableDatabase( fileSystem,
                    new StoreVersionCheck( pageCacheRule.getPageCache( fileSystem ) ),
                    new LegacyStoreVersionCheck( fileSystem ), getRecordFormat() );
            try
            {
                // when
                upgradableDatabase.checkUpgradeable( workingDirectory );
                fail( "should not have been able to upgrade" );
            }
            catch ( StoreUpgrader.UnexpectedUpgradingStoreVersionException e )
            {
                // then
                assertEquals( String.format( MESSAGE, version, upgradableDatabase.currentVersion(),
                        Version.getNeo4jVersion() ), e.getMessage() );
            }
            catch ( StoreUpgrader.UnexpectedUpgradingStoreFormatException e )
            {
                // then
                assertNotSame( StandardFormatFamily.INSTANCE,
                        RecordFormatSelector.selectForVersion( version ).getFormatFamily());
                assertEquals( String.format( StoreUpgrader.UnexpectedUpgradingStoreFormatException.MESSAGE,
                        GraphDatabaseSettings.record_format.name() ), e.getMessage() );
            }
        }
    }

    private static RecordFormats getRecordFormat()
    {
        return Standard.LATEST_RECORD_FORMATS;
    }
}
