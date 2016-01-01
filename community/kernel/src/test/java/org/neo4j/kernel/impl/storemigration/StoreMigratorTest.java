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
package org.neo4j.kernel.impl.storemigration;

import java.io.File;
import java.io.IOException;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.kernel.DefaultIdGeneratorFactory;
import org.neo4j.kernel.DefaultTxHook;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.nioneo.store.DefaultWindowPoolFactory;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.store.StoreFactory;
import org.neo4j.kernel.impl.storemigration.monitoring.SilentMigrationProgressMonitor;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.test.EphemeralFileSystemRule;
import org.neo4j.test.impl.EphemeralFileSystemAbstraction;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class StoreMigratorTest
{
    @Test
    public void shouldBeAbleToResumeMigration() throws Exception
    {
        // GIVEN a legacy database
        File storeDirectory = createNeoStoreWithOlderVersion();
        // and a state of the migration saying that it has done the actual migration
        StoreMigrator migrator = new StoreMigrator( new SilentMigrationProgressMonitor(), fs.get() );
        File migrationDir = new File( storeDirectory, StoreUpgrader.MIGRATION_DIRECTORY );
        fs.get().mkdirs( migrationDir );
        assertTrue( migrator.needsMigration( fs.get(), storeDirectory ) );
        migrator.migrate( fs.get(), storeDirectory, migrationDir, mock( DependencyResolver.class ) );
        migrator.close();

        // WHEN simulating resuming the migration
        migrator = new StoreMigrator( new SilentMigrationProgressMonitor(), fs.get() );
        migrator.moveMigratedFiles( fs.get(), migrationDir, storeDirectory );

        // THEN starting the new store should be successful
        StoreFactory storeFactory = new StoreFactory( new Config(), new DefaultIdGeneratorFactory(),
                new DefaultWindowPoolFactory(), fs.get(), StringLogger.DEV_NULL, new DefaultTxHook() );
        storeFactory.newNeoStore( new File( storeDirectory, NeoStore.DEFAULT_NAME ) ).close();
    }

    private File createNeoStoreWithOlderVersion() throws IOException
    {
        File storeDir = new File( "dir" ).getAbsoluteFile();
        EphemeralFileSystemAbstraction fileSystem = fs.get();
        fileSystem.mkdirs( storeDir );
        MigrationTestUtils.prepareSampleLegacyDatabase( fileSystem, storeDir );
        return storeDir;
    }

    public final @Rule EphemeralFileSystemRule fs = new EphemeralFileSystemRule();
}
