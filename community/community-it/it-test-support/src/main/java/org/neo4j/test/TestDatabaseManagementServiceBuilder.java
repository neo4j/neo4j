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

import static java.lang.Boolean.FALSE;
import static org.neo4j.util.Preconditions.checkState;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.function.Predicate;
import org.neo4j.collection.Dependencies;
import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilderImplementation;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.facade.ExternalDependencies;
import org.neo4j.graphdb.facade.GraphDatabaseDependencies;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.UncloseableDelegatingFileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.kernel.database.NoOpSystemGraphInitializer;
import org.neo4j.kernel.extension.ExtensionFactory;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.logging.LogProvider;
import org.neo4j.monitoring.Monitors;
import org.neo4j.procedure.LazyProcedures;
import org.neo4j.time.SystemNanoClock;

/**
 * Test factory for graph databases.
 * Please be aware that since it's a database it will close filesystem as part of its lifecycle.
 * If you expect your file system to be open after database is closed, use {@link UncloseableDelegatingFileSystemAbstraction}
 */
public class TestDatabaseManagementServiceBuilder extends DatabaseManagementServiceBuilderImplementation
        implements TestNeo4jDatabaseManagementServiceBuilder {
    private static final Path EPHEMERAL_PATH =
            Path.of("/target/test data/" + GraphDatabaseSettings.DEFAULT_DATABASE_NAME);

    protected FileSystemAbstraction fileSystem;
    protected InternalLogProvider internalLogProvider;
    protected SystemNanoClock clock;
    protected Config fromConfig;
    protected boolean noOpSystemGraphInitializer;
    private boolean lazyProcedures = true;

    public TestDatabaseManagementServiceBuilder() {
        super(null);
    }

    public TestDatabaseManagementServiceBuilder(Path homeDirectory) {
        super(homeDirectory);
    }

    public TestDatabaseManagementServiceBuilder(Neo4jLayout layout) {
        super(layout.homeDirectory());
        setConfig(GraphDatabaseInternalSettings.databases_root_path, layout.databasesDirectory());
        setConfig(GraphDatabaseSettings.transaction_logs_root_path, layout.transactionLogsRootDirectory());
        setConfig(BoltConnector.thread_pool_max_size, 2000);
    }

    public TestDatabaseManagementServiceBuilder(DatabaseLayout layout) {
        this(layout.getNeo4jLayout());
        setConfig(GraphDatabaseSettings.initial_default_database, layout.getDatabaseName());
    }

    @Override
    public DatabaseManagementService build() {
        return build0();
    }

    protected DatabaseManagementService build0() {
        fileSystem = fileSystem != null ? fileSystem : new DefaultFileSystemAbstraction();
        if (homeDirectory == null) {
            if (fileSystem.isPersistent()) {
                throw new RuntimeException("You have to specify a home directory or use an impermanent filesystem.");
            } else {
                homeDirectory = EPHEMERAL_PATH;
            }
        }

        Config cfg = config.set(GraphDatabaseSettings.neo4j_home, homeDirectory.toAbsolutePath())
                .fromConfig(fromConfig)
                .build();

        var originalDependencies = dependencies;
        if (noOpSystemGraphInitializer) {
            dependencies = NoOpSystemGraphInitializer.noOpSystemGraphInitializer(dependencies, cfg);
        }
        if (lazyProcedures) {
            var dependencyWrapper = new Dependencies(dependencies);
            dependencyWrapper.satisfyDependency(new LazyProcedures());
            dependencies = dependencyWrapper;
        }

        var dbms = newDatabaseManagementService(cfg, databaseDependencies());
        dependencies = originalDependencies;
        return dbms;
    }

    @Override
    protected DatabaseManagementService newDatabaseManagementService(Config config, ExternalDependencies dependencies) {
        var factory = new TestDatabaseManagementServiceFactory(
                getDbmsInfo(config), getEditionFactory(config), fileSystem, clock, internalLogProvider);

        return factory.build(
                augmentConfig(config), daemonMode, GraphDatabaseDependencies.newDependencies(dependencies));
    }

    @Override
    protected Config augmentConfig(Config config) {
        var builder = Config.newBuilder()
                .fromConfig(config)
                .setDefault(GraphDatabaseSettings.pagecache_memory, ByteUnit.mebiBytes(8))
                .setDefault(GraphDatabaseSettings.logical_log_rotation_threshold, ByteUnit.kibiBytes(256))
                .setDefault(BoltConnector.enabled, FALSE)
                .setDefault(GraphDatabaseInternalSettings.dump_diagnostics, false)
                .setDefault(GraphDatabaseInternalSettings.track_tx_statement_close, true)
                .setDefault(GraphDatabaseInternalSettings.trace_tx_statements, true)
                .setDefault(GraphDatabaseInternalSettings.track_cursor_close, true)
                .setDefault(GraphDatabaseInternalSettings.netty_server_shutdown_quiet_period, 0)
                .setDefault(GraphDatabaseInternalSettings.netty_server_shutdown_timeout, Duration.ofSeconds(3))
                .setDefault(GraphDatabaseSettings.query_cache_size, 10)
                .setDefault(GraphDatabaseInternalSettings.additional_lock_verification, true)
                .setDefault(GraphDatabaseInternalSettings.lock_manager_verbose_deadlocks, true)
                .setDefault(GraphDatabaseInternalSettings.vm_pause_monitor_enabled, false)
                .setDefault(GraphDatabaseSettings.check_point_iops_limit, -1)
                .setDefault(GraphDatabaseInternalSettings.gbptree_structure_log_enabled, true)
                .setDefault(GraphDatabaseSettings.filewatcher_enabled, false)
                .setDefault(GraphDatabaseSettings.udc_enabled, false)
                .setDefault(
                        BoltConnector.listen_address,
                        new SocketAddress("localhost", DynamicPorts.OS_SELECTED_DYNAMIC_PORT));
        return builder.build();
    }

    public Path getHomeDirectory() {
        return homeDirectory;
    }

    @Override
    public TestDatabaseManagementServiceBuilder setFileSystem(FileSystemAbstraction fileSystem) {
        this.fileSystem = fileSystem;
        return this;
    }

    public TestDatabaseManagementServiceBuilder setDatabaseRootDirectory(Path storeDir) {
        this.homeDirectory = storeDir;
        return this;
    }

    public TestDatabaseManagementServiceBuilder setInternalLogProvider(InternalLogProvider internalLogProvider) {
        this.internalLogProvider = internalLogProvider;
        return this;
    }

    public TestDatabaseManagementServiceBuilder setClock(SystemNanoClock clock) {
        this.clock = clock;
        return this;
    }

    private TestDatabaseManagementServiceBuilder addExtensions(Iterable<ExtensionFactory<?>> extensions) {
        for (ExtensionFactory<?> extension : extensions) {
            this.extensions.add(extension);
        }
        return this;
    }

    public TestDatabaseManagementServiceBuilder addExtension(ExtensionFactory<?> extension) {
        return addExtensions(Collections.singletonList(extension));
    }

    public TestDatabaseManagementServiceBuilder setExtensions(Iterable<ExtensionFactory<?>> newExtensions) {
        extensions.clear();
        addExtensions(newExtensions);
        return this;
    }

    public TestDatabaseManagementServiceBuilder removeExtensions(Predicate<ExtensionFactory<?>> toRemove) {
        extensions.removeIf(toRemove);
        return this;
    }

    /**
     * Mark this {@link DatabaseManagementService} as impermanent.
     *
     * This will create a new file system. If you want an impermanent database and access to
     * the underlying file system, use {@link #setFileSystem(FileSystemAbstraction)} instead.
     *
     * @return the builder.
     */
    public TestDatabaseManagementServiceBuilder impermanent() {
        checkState(fileSystem == null, "Filesystem is already assigned, can't update it.");
        fileSystem = new EphemeralFileSystemAbstraction();
        return this;
    }

    public TestDatabaseManagementServiceBuilder setConfig(Config fromConfig) {
        if (this.fromConfig != null) {
            throw new IllegalStateException("You can only set config once.");
        }
        this.fromConfig = fromConfig;
        return this;
    }

    @Override
    public TestDatabaseManagementServiceBuilder setConfigRaw(Map<String, String> raw) {
        config.setRaw(raw);
        return this;
    }

    public TestDatabaseManagementServiceBuilder useLazyProcedures(boolean useLazyProcedures) {
        this.lazyProcedures = useLazyProcedures;
        return this;
    }

    public TestDatabaseManagementServiceBuilder noOpSystemGraphInitializer() {
        this.noOpSystemGraphInitializer = true;
        return this;
    }

    // Override to allow chaining

    @Override
    public TestDatabaseManagementServiceBuilder setExternalDependencies(DependencyResolver dependencies) {
        return (TestDatabaseManagementServiceBuilder) super.setExternalDependencies(dependencies);
    }

    @Override
    public TestDatabaseManagementServiceBuilder setMonitors(Monitors monitors) {
        return (TestDatabaseManagementServiceBuilder) super.setMonitors(monitors);
    }

    @Override
    public TestDatabaseManagementServiceBuilder setUserLogProvider(LogProvider logProvider) {
        return (TestDatabaseManagementServiceBuilder) super.setUserLogProvider(logProvider);
    }

    @Override
    public <T> TestDatabaseManagementServiceBuilder setConfig(Setting<T> setting, T value) {
        return (TestDatabaseManagementServiceBuilder) super.setConfig(setting, value);
    }

    @Override
    public TestDatabaseManagementServiceBuilder setConfig(Map<Setting<?>, Object> config) {
        return (TestDatabaseManagementServiceBuilder) super.setConfig(config);
    }

    public <T> TestDatabaseManagementServiceBuilder overrideDefaultSetting(Setting<T> setting, T value) {
        config.setDefault(setting, value);
        return this;
    }
}
