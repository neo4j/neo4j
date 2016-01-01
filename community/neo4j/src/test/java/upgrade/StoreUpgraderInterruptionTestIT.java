/**
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
package upgrade;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import static org.neo4j.consistency.store.StoreAssertions.assertConsistentStore;
import static org.neo4j.kernel.impl.nioneo.store.CommonAbstractStore.ALL_STORES_VERSION;
import static org.neo4j.kernel.impl.storemigration.MigrationTestUtils.allStoreFilesHaveVersion;
import static org.neo4j.kernel.impl.storemigration.UpgradeConfiguration.ALLOW_UPGRADE;
import static org.neo4j.kernel.impl.storemigration.legacystore.v20.Legacy20Store.LEGACY_VERSION;

import java.io.File;
import java.io.IOException;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.consistency.checking.full.ConsistencyCheckIncompleteException;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.storemigration.MigrationTestUtils;
import org.neo4j.kernel.impl.storemigration.StoreMigrator;
import org.neo4j.kernel.impl.storemigration.StoreUpgrader;
import org.neo4j.kernel.impl.storemigration.monitoring.SilentMigrationProgressMonitor;
import org.neo4j.kernel.logging.DevNullLoggingService;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.TargetDirectory.TestDirectory;

public class StoreUpgraderInterruptionTestIT
{
    @Test
    public void shouldSucceedWithUpgradeAfterPreviousAttemptDiedDuringMigration()
            throws IOException, ConsistencyCheckIncompleteException
    {
        File workingDirectory = directory.directory();
        MigrationTestUtils.prepareSampleLegacyDatabase( fileSystem, workingDirectory );

        StoreMigrator failingStoreMigrator = new StoreMigrator( new SilentMigrationProgressMonitor(), fileSystem )
        {
            @Override
            public void migrate( FileSystemAbstraction fileSystem, File sourceStoreDir, File targetStoreDir,
                    DependencyResolver dependencyResolver ) throws IOException
            {
                super.migrate( fileSystem, sourceStoreDir, targetStoreDir, dependencyResolver );
                throw new RuntimeException( "This upgrade is failing" );
            }
        };

        assertTrue( allStoreFilesHaveVersion( fileSystem, workingDirectory, LEGACY_VERSION ) );

        try
        {
            newUpgrader( failingStoreMigrator ).migrateIfNeeded( workingDirectory );
            fail( "Should throw exception" );
        }
        catch ( RuntimeException e )
        {
            assertEquals( "This upgrade is failing", e.getMessage() );
        }

        assertTrue( allStoreFilesHaveVersion( fileSystem, workingDirectory, LEGACY_VERSION ) );

        newUpgrader( new StoreMigrator( new SilentMigrationProgressMonitor(), fileSystem ) )
                .migrateIfNeeded( workingDirectory );

        assertTrue( allStoreFilesHaveVersion( fileSystem, workingDirectory, ALL_STORES_VERSION ) );
        assertConsistentStore( workingDirectory );
    }

    private StoreUpgrader newUpgrader( StoreMigrator migrator )
    {
        DevNullLoggingService logging = new DevNullLoggingService();
        StoreUpgrader upgrader = new StoreUpgrader( ALLOW_UPGRADE, fileSystem, StoreUpgrader.NO_MONITOR, logging );
        upgrader.addParticipant( migrator );
        return upgrader;
    }

    public final @Rule TestDirectory directory = TargetDirectory.testDirForTest( getClass() );

    @SuppressWarnings( "deprecation" )
    private final FileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction();
}
