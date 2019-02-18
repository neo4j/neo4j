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
package org.neo4j.kernel.impl.storemigration;

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
import java.util.Optional;

import org.neo4j.configuration.Config;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.format.StoreVersion;
import org.neo4j.kernel.impl.store.format.standard.Standard;
import org.neo4j.kernel.impl.store.format.standard.StandardV3_4;
import org.neo4j.storageengine.api.StoreVersionCheck;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.neo4j.kernel.impl.store.MetaDataStore.Position.STORE_VERSION;
import static org.neo4j.kernel.impl.storemigration.MigrationTestUtils.changeVersionNumber;

@RunWith( Enclosed.class )
public class RecordStoreVersionTest
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

        private DatabaseLayout databaseLayout;
        private FileSystemAbstraction fileSystem;

        @Parameterized.Parameter( 0 )
        public String version;

        @Parameterized.Parameters( name = "{0}" )
        public static Collection<String> versions()
        {
            return Collections.singletonList( StandardV3_4.STORE_VERSION );
        }

        @Before
        public void setup() throws IOException
        {
            fileSystem = fileSystemRule.get();
            databaseLayout = testDirectory.databaseLayout();
            MigrationTestUtils.findFormatStoreDirectoryForVersion( version, databaseLayout.databaseDirectory() );
        }

        boolean storeFilesUpgradable( RecordStoreVersionCheck check )
        {
            return check.checkUpgrade( check.configuredVersion() ).outcome.isSuccessful();
        }

        @Test
        public void shouldAcceptTheStoresInTheSampleDatabaseAsBeingEligibleForUpgrade()
        {
            // given
            final RecordStoreVersionCheck check = getVersionCheck();

            // when
            final boolean result = storeFilesUpgradable( check );

            // then
            assertTrue( result );
        }

        @Test
        public void shouldDetectOldVersionAsDifferentFromCurrent()
        {
            // given
            final RecordStoreVersionCheck check = getVersionCheck();
            // when
            Optional<String> version = check.storeVersion();
            assertTrue( version.isPresent() );
            boolean currentVersion = version.get().equalsIgnoreCase( check.configuredVersion() );

            // then
            assertFalse( currentVersion );
        }

        private RecordStoreVersionCheck getVersionCheck()
        {
            return new RecordStoreVersionCheck( pageCacheRule.getPageCache( fileSystem ), databaseLayout, getRecordFormat(), Config.defaults() );
        }
    }

    @RunWith( Parameterized.class )
    public static class UnsupportedVersions
    {
        private final TestDirectory testDirectory = TestDirectory.testDirectory();
        private final PageCacheRule pageCacheRule = new PageCacheRule();
        private final DefaultFileSystemRule fileSystemRule = new DefaultFileSystemRule();

        @Rule
        public RuleChain ruleChain = RuleChain.outerRule( testDirectory )
                .around( fileSystemRule ).around( pageCacheRule );

        private DatabaseLayout databaseLayout;
        private FileSystemAbstraction fileSystem;

        @Parameterized.Parameter( 0 )
        public String version;

        @Parameterized.Parameters( name = "{0}" )
        public static Collection<String> versions()
        {
            return Arrays.asList( "v0.A.4", StoreVersion.HIGH_LIMIT_V3_4_0.versionString() );
        }

        @Before
        public void setup() throws IOException
        {
            fileSystem = fileSystemRule.get();
            databaseLayout = testDirectory.databaseLayout();
            // doesn't matter which version we pick we are changing it to the wrong one...
            MigrationTestUtils.findFormatStoreDirectoryForVersion( StandardV3_4.STORE_VERSION, databaseLayout.databaseDirectory() );
            changeVersionNumber( fileSystem, databaseLayout.metadataStore(), version );
            File metadataStore = databaseLayout.metadataStore();
            PageCache pageCache = pageCacheRule.getPageCache( fileSystem );
            MetaDataStore.setRecord( pageCache, metadataStore, STORE_VERSION, MetaDataStore.versionStringToLong( version ) );
        }

        @Test
        public void shouldDetectOldVersionAsDifferentFromCurrent()
        {
            // given
            final RecordStoreVersionCheck check = getVersionCheck();

            // when
            Optional<String> version = check.storeVersion();
            assertTrue( version.isPresent() );
            boolean currentVersion = version.get().equals( check.configuredVersion() );

            // then
            assertFalse( currentVersion );
        }

        @Test
        public void shouldCommunicateWhatCausesInabilityToUpgrade()
        {
            // given
            final RecordStoreVersionCheck check = getVersionCheck();

            // when
            StoreVersionCheck.Result result = check.checkUpgrade( check.configuredVersion() );
            assertFalse( result.outcome.isSuccessful() );
            assertSame( StoreVersionCheck.Outcome.unexpectedStoreVersion, result.outcome );
        }

        private RecordStoreVersionCheck getVersionCheck()
        {
            return new RecordStoreVersionCheck( pageCacheRule.getPageCache( fileSystem ), databaseLayout, getRecordFormat(), Config.defaults() );
        }
    }

    private static RecordFormats getRecordFormat()
    {
        return Standard.LATEST_RECORD_FORMATS;
    }
}
