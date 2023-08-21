/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.test;

import static java.util.Objects.requireNonNull;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.duplication_user_messages;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
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
import org.neo4j.logging.log4j.Log4jLogProvider;
import org.neo4j.logging.log4j.LogConfig;
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
    protected GlobalModule createGlobalModule(Config config, boolean daemonMode, ExternalDependencies dependencies) {
        config.setIfNotSet(GraphDatabaseSettings.shutdown_transaction_end_timeout, Duration.ZERO);

        if (!fileSystem.isPersistent()) {
            config.setIfNotSet(GraphDatabaseSettings.keep_logical_logs, "1 files");
        }

        return new TestDatabaseGlobalModule(config, daemonMode, this.dbmsInfo, dependencies);
    }

    public class TestDatabaseGlobalModule extends GlobalModule {
        public TestDatabaseGlobalModule(
                Config config, boolean daemonMode, DbmsInfo dbmsInfo, ExternalDependencies dependencies) {
            super(config, dbmsInfo, daemonMode, dependencies);
        }

        @Override
        protected FileSystemAbstraction createFileSystemAbstraction() {
            return fileSystem;
        }

        @Override
        protected LogService createLogService(InternalLogProvider userLogProvider, boolean daemonMode) {
            if (internalLogProvider == null) {
                if (fileSystem.isPersistent()) {
                    return super.createLogService(userLogProvider, daemonMode);
                } else if (getGlobalConfig().get(GraphDatabaseSettings.debug_log_enabled)) {
                    try {
                        return specialLoggingForEphemeral(userLogProvider);
                    } catch (IOException e) {
                        throw new UncheckedIOException("Failed to set up logging for EphemeralFilesystem", e);
                    }
                }
                internalLogProvider = NullLogProvider.getInstance();
            }

            // Some tests appear to (inadvertently?) depend on log user log messages being duplicated to the debug log
            // because they assert on the debug log.
            return new SimpleLogService(userLogProvider, internalLogProvider, true);
        }

        private SimpleLogService specialLoggingForEphemeral(InternalLogProvider userLogProvider) throws IOException {
            Config config = getGlobalConfig();
            Path logDir = config.get(GraphDatabaseSettings.logs_directory);
            fileSystem.mkdirs(logDir);
            internalLogProvider =
                    new Log4jLogProvider(fileSystem.openAsOutputStream(logDir.resolve(LogConfig.DEBUG_LOG), true));
            return getGlobalLife()
                    .add(new SimpleLogService(
                            userLogProvider == null ? NullLogProvider.getInstance() : userLogProvider,
                            internalLogProvider,
                            config.get(duplication_user_messages)));
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
