/*
 * Copyright (c) "Neo4j"
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

import static java.util.Objects.requireNonNull;

import java.time.Duration;
import java.util.function.Function;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.facade.DatabaseManagementServiceFactory;
import org.neo4j.graphdb.facade.ExternalDependencies;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.graphdb.factory.module.edition.AbstractEditionModule;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.io.locker.Locker;
import org.neo4j.kernel.impl.factory.DbmsInfo;
import org.neo4j.kernel.internal.locker.FileLockerService;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.logging.internal.LogService;
import org.neo4j.logging.internal.SimpleLogService;
import org.neo4j.time.SystemNanoClock;

public class TestDatabaseManagementServiceFactory extends DatabaseManagementServiceFactory {
    private final FileSystemAbstraction fileSystem;
    private InternalLogProvider internalLogProvider;
    private final SystemNanoClock clock;

    public TestDatabaseManagementServiceFactory(
            DbmsInfo dbmsInfo,
            Function<GlobalModule, AbstractEditionModule> editionFactory,
            FileSystemAbstraction fileSystem,
            SystemNanoClock clock,
            InternalLogProvider internalLogProvider) {
        super(dbmsInfo, editionFactory);
        this.fileSystem = requireNonNull(fileSystem);
        this.clock = clock;
        this.internalLogProvider = internalLogProvider;
    }

    @Override
    protected GlobalModule createGlobalModule(Config config, ExternalDependencies dependencies) {
        config.setIfNotSet(GraphDatabaseSettings.shutdown_transaction_end_timeout, Duration.ZERO);

        if (!fileSystem.isPersistent()) {
            config.setIfNotSet(GraphDatabaseSettings.keep_logical_logs, "1 files");
        }

        return new TestDatabaseGlobalModule(config, this.dbmsInfo, dependencies);
    }

    class TestDatabaseGlobalModule extends GlobalModule {
        TestDatabaseGlobalModule(Config config, DbmsInfo dbmsInfo, ExternalDependencies dependencies) {
            super(config, dbmsInfo, dependencies);
        }

        @Override
        protected FileSystemAbstraction createFileSystemAbstraction() {
            return fileSystem;
        }

        @Override
        protected LogService createLogService(InternalLogProvider userLogProvider) {
            if (internalLogProvider == null) {
                if (fileSystem.isPersistent()) {
                    return super.createLogService(userLogProvider);
                }
                internalLogProvider = NullLogProvider.getInstance();
            }

            // Some tests appear to (inadvertently?) depend on log user log messages being duplicated to the debug log
            // because they assert on the debug log.
            return new SimpleLogService(userLogProvider, internalLogProvider, true);
        }

        @Override
        protected SystemNanoClock createClock() {
            return clock != null ? clock : super.createClock();
        }

        @Override
        protected FileLockerService createFileLockerService() {
            if (fileSystem.isPersistent()) {
                return super.createFileLockerService();
            }
            return new ImpermanentLockerService();
        }
    }

    /**
     * Locker service implementation that provide dbms and database level locks that are not registered globally
     * anywhere and only holds underlying file channel locks.
     */
    private static class ImpermanentLockerService implements FileLockerService {
        @Override
        public Locker createStoreLocker(FileSystemAbstraction fileSystem, Neo4jLayout storeLayout) {
            return new Locker(fileSystem, storeLayout.storeLockFile());
        }

        @Override
        public Locker createDatabaseLocker(FileSystemAbstraction fileSystem, DatabaseLayout databaseLayout) {
            return new Locker(fileSystem, databaseLayout.databaseLockFile());
        }
    }
}
