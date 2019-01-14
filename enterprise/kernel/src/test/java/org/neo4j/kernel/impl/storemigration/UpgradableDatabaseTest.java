/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
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
import java.util.Collections;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.format.RecordFormatSelector;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.format.StoreVersion;
import org.neo4j.kernel.impl.store.format.standard.Standard;
import org.neo4j.kernel.impl.store.format.standard.StandardFormatFamily;
import org.neo4j.kernel.impl.store.format.standard.StandardV2_3;
import org.neo4j.kernel.impl.transaction.log.ReadableClosablePositionAwareChannel;
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.internal.Version;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.kernel.recovery.LogTailScanner;
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
        private LogTailScanner tailScanner;

        @Parameterized.Parameter( 0 )
        public String version;

        @Parameterized.Parameters( name = "{0}" )
        public static Collection<String> versions()
        {
            return Collections.singletonList(
                    StandardV2_3.STORE_VERSION
            );
        }

        @Before
        public void setup() throws IOException
        {
            fileSystem = fileSystemRule.get();
            workingDirectory = testDirectory.graphDbDir();
            MigrationTestUtils.findFormatStoreDirectoryForVersion( version, workingDirectory );
            VersionAwareLogEntryReader<ReadableClosablePositionAwareChannel> logEntryReader = new VersionAwareLogEntryReader<>();
            LogFiles logFiles = LogFilesBuilder.logFilesBasedOnlyBuilder( workingDirectory, fileSystem ).build();
            tailScanner = new LogTailScanner( logFiles, logEntryReader, new Monitors() );
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
            final UpgradableDatabase upgradableDatabase = getUpgradableDatabase();

            // when
            final boolean result = storeFilesUpgradeable( workingDirectory, upgradableDatabase );

            // then
            assertTrue( result );
        }

        @Test
        public void shouldDetectOldVersionAsDifferentFromCurrent()
        {
            // given
            final UpgradableDatabase upgradableDatabase = getUpgradableDatabase();
            // when
            boolean currentVersion = upgradableDatabase.hasCurrentVersion( workingDirectory );

            // then
            assertFalse( currentVersion );
        }

        @Test
        public void shouldRejectStoresIfDBIsNotShutdownCleanly() throws IOException
        {
            // checkpoint has been introduced in 2.3
            Assume.assumeTrue( StandardV2_3.STORE_VERSION.equals( version ) );

            // given
            removeCheckPointFromTxLog( fileSystem, workingDirectory );
            final UpgradableDatabase upgradableDatabase = getUpgradableDatabase();

            // when
            final boolean result = storeFilesUpgradeable( workingDirectory, upgradableDatabase );

            // then
            assertFalse( result );
        }

        private UpgradableDatabase getUpgradableDatabase()
        {
            return new UpgradableDatabase( new StoreVersionCheck( pageCacheRule.getPageCache( fileSystem ) ),
                    getRecordFormat(), tailScanner );
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
        private LogTailScanner tailScanner;

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
            MigrationTestUtils.findFormatStoreDirectoryForVersion( StandardV2_3.STORE_VERSION, workingDirectory );
            changeVersionNumber( fileSystem, new File( workingDirectory, neostoreFilename ), version );
            File metadataStore = new File( workingDirectory, MetaDataStore.DEFAULT_NAME );
            PageCache pageCache = pageCacheRule.getPageCache( fileSystem );
            MetaDataStore.setRecord( pageCache, metadataStore, STORE_VERSION, MetaDataStore.versionStringToLong( version ) );
            VersionAwareLogEntryReader<ReadableClosablePositionAwareChannel> logEntryReader = new VersionAwareLogEntryReader<>();
            LogFiles logFiles = LogFilesBuilder.logFilesBasedOnlyBuilder( workingDirectory, fileSystem ).build();
            tailScanner = new LogTailScanner( logFiles, logEntryReader, new Monitors() );
        }

        @Test
        public void shouldDetectOldVersionAsDifferentFromCurrent()
        {
            // given
            final UpgradableDatabase upgradableDatabase = getUpgradableDatabase();

            // when
            boolean currentVersion = upgradableDatabase.hasCurrentVersion( workingDirectory );

            // then
            assertFalse( currentVersion );
        }

        @Test
        public void shouldCommunicateWhatCausesInabilityToUpgrade()
        {
            // given
            final UpgradableDatabase upgradableDatabase = getUpgradableDatabase();
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

        private UpgradableDatabase getUpgradableDatabase()
        {
            return new UpgradableDatabase( new StoreVersionCheck( pageCacheRule.getPageCache( fileSystem ) ),
                    getRecordFormat(), tailScanner );
        }
    }

    private static RecordFormats getRecordFormat()
    {
        return Standard.LATEST_RECORD_FORMATS;
    }
}
