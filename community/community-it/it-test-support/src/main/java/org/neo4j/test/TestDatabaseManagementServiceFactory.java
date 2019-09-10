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
package org.neo4j.test;

import java.time.Duration;
import java.util.function.Function;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.facade.DatabaseManagementServiceFactory;
import org.neo4j.graphdb.facade.ExternalDependencies;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.graphdb.factory.module.edition.AbstractEditionModule;
import org.neo4j.io.fs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.kernel.impl.factory.DatabaseInfo;
import org.neo4j.kernel.internal.locker.FileLockerService;
import org.neo4j.kernel.internal.locker.Locker;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.logging.internal.LogService;
import org.neo4j.logging.internal.SimpleLogService;
import org.neo4j.time.SystemNanoClock;

public class TestDatabaseManagementServiceFactory extends DatabaseManagementServiceFactory
{
    private final boolean impermanent;
    private FileSystemAbstraction fileSystem;
    private LogProvider internalLogProvider;
    private final SystemNanoClock clock;

    public TestDatabaseManagementServiceFactory( DatabaseInfo databaseInfo, Function<GlobalModule,AbstractEditionModule> editionFactory, boolean impermanent,
            FileSystemAbstraction fileSystem, SystemNanoClock clock, LogProvider internalLogProvider )
    {
        super( databaseInfo, editionFactory );
        this.impermanent = impermanent;
        this.fileSystem = fileSystem;
        this.clock = clock;
        this.internalLogProvider = internalLogProvider;
    }

    @Override
    protected GlobalModule createGlobalModule( Config config, ExternalDependencies dependencies )
    {
        config.setIfNotSet( GraphDatabaseSettings.shutdown_transaction_end_timeout, Duration.ZERO );
        if ( impermanent )
        {
            config.set( GraphDatabaseSettings.ephemeral_lucene, true );
            config.setIfNotSet( GraphDatabaseSettings.keep_logical_logs, "1 files" );
            return new ImpermanentTestDatabaseGlobalModule( config, dependencies, this.databaseInfo );
        }

        return new TestDatabaseGlobalModule( config, dependencies, this.databaseInfo );
    }

    class TestDatabaseGlobalModule extends GlobalModule
    {

        TestDatabaseGlobalModule( Config config, ExternalDependencies dependencies, DatabaseInfo databaseInfo )
        {
            super( config, databaseInfo, dependencies );
        }

        @Override
        protected FileSystemAbstraction createFileSystemAbstraction()
        {
            if ( fileSystem != null )
            {
                return fileSystem;
            }
            else
            {
                return createNewFileSystem();
            }
        }

        protected FileSystemAbstraction createNewFileSystem()
        {
            return super.createFileSystemAbstraction();
        }

        @Override
        protected LogService createLogService( LogProvider userLogProvider )
        {
            if ( internalLogProvider == null )
            {
                if ( !impermanent )
                {
                    return super.createLogService( userLogProvider );
                }
                internalLogProvider = NullLogProvider.getInstance();
            }
            return new SimpleLogService( userLogProvider, internalLogProvider );
        }

        @Override
        protected SystemNanoClock createClock()
        {
            return clock != null ? clock : super.createClock();
        }
    }

    private class ImpermanentTestDatabaseGlobalModule extends TestDatabaseGlobalModule
    {

        ImpermanentTestDatabaseGlobalModule( Config config, ExternalDependencies dependencies, DatabaseInfo databaseInfo )
        {
            super( config, dependencies, databaseInfo );
        }

        @Override
        protected FileSystemAbstraction createNewFileSystem()
        {
            return new EphemeralFileSystemAbstraction();
        }

        @Override
        protected FileLockerService createFileLockerService()
        {
            return new ImpermanentLockerService();
        }
    }

    /**
     * Locker service implementation that provide dbms and database level locks that are not registered globally
     * anywhere and only holds underlying file channel locks.
     */
    private static class ImpermanentLockerService implements FileLockerService
    {
        @Override
        public Locker createStoreLocker( FileSystemAbstraction fileSystem, Neo4jLayout storeLayout )
        {
            return new Locker( fileSystem, storeLayout.storeLockFile() );
        }

        @Override
        public Locker createDatabaseLocker( FileSystemAbstraction fileSystem, DatabaseLayout databaseLayout )
        {
            return new Locker( fileSystem, databaseLayout.databaseLockFile() );
        }
    }
}
