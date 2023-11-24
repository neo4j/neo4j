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
package org.neo4j.dbms.api;

import static org.neo4j.graphdb.facade.GraphDatabaseDependencies.newDependencies;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;
import org.neo4j.collection.Dependencies;
import org.neo4j.common.DependencyResolver;
import org.neo4j.common.Edition;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.event.DatabaseEventListener;
import org.neo4j.graphdb.facade.DatabaseManagementServiceFactory;
import org.neo4j.graphdb.facade.ExternalDependencies;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.graphdb.factory.module.edition.AbstractEditionModule;
import org.neo4j.graphdb.factory.module.edition.CommunityEditionModule;
import org.neo4j.kernel.extension.ExtensionFactory;
import org.neo4j.kernel.impl.factory.DbmsInfo;
import org.neo4j.logging.ExternalLogProviderWrapper;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.monitoring.Monitors;
import org.neo4j.service.Services;

public class DatabaseManagementServiceBuilderImplementation implements Neo4jDatabaseManagementServiceBuilder {
    protected final List<ExtensionFactory<?>> extensions = new ArrayList<>();
    protected final List<DatabaseEventListener> databaseEventListeners = new ArrayList<>();
    protected Monitors monitors;
    private InternalLogProvider userLogProvider = NullLogProvider.getInstance();
    protected DependencyResolver dependencies = new Dependencies();
    protected Path homeDirectory;
    protected Config.Builder config = Config.newBuilder();
    protected boolean daemonMode;

    public DatabaseManagementServiceBuilderImplementation(
            Path homeDirectory, Predicate<Class<? extends ExtensionFactory>> extensionFilter) {
        this.homeDirectory = homeDirectory;
        Services.loadAll(ExtensionFactory.class).stream()
                .filter(e -> extensionFilter.test(e.getClass()))
                .forEach(extensions::add);
    }

    public DatabaseManagementServiceBuilderImplementation(Path homeDirectory) {
        this(homeDirectory, extension -> true);
    }

    @Override
    public DatabaseManagementService build() {
        config.set(GraphDatabaseSettings.neo4j_home, homeDirectory.toAbsolutePath());
        return newDatabaseManagementService(config.build(), databaseDependencies());
    }

    protected DatabaseManagementService newDatabaseManagementService(Config config, ExternalDependencies dependencies) {
        return new DatabaseManagementServiceFactory(getDbmsInfo(config), getEditionFactory(config))
                .build(augmentConfig(config), daemonMode, dependencies);
    }

    protected DbmsInfo getDbmsInfo(Config config) {
        return DbmsInfo.COMMUNITY;
    }

    protected Function<GlobalModule, AbstractEditionModule> getEditionFactory(Config config) {
        return CommunityEditionModule::new;
    }

    /**
     * Override to augment config values
     * @param config
     */
    protected Config augmentConfig(Config config) {
        return config;
    }

    @Override
    public DatabaseManagementServiceBuilderImplementation addDatabaseListener(
            DatabaseEventListener databaseEventListener) {
        databaseEventListeners.add(databaseEventListener);
        return this;
    }

    @Override
    public DatabaseManagementServiceBuilderImplementation setUserLogProvider(LogProvider userLogProvider) {
        if (userLogProvider instanceof InternalLogProvider internalLogProvider) {
            this.userLogProvider = internalLogProvider;
        } else {
            this.userLogProvider = new ExternalLogProviderWrapper(userLogProvider);
        }
        return this;
    }

    public DatabaseManagementServiceBuilderImplementation setMonitors(Monitors monitors) {
        this.monitors = monitors;
        return this;
    }

    public DatabaseManagementServiceBuilderImplementation setExternalDependencies(DependencyResolver dependencies) {
        this.dependencies = dependencies;
        return this;
    }

    public String getEdition() {
        return Edition.COMMUNITY.toString();
    }

    protected ExternalDependencies databaseDependencies() {
        return newDependencies()
                .monitors(monitors)
                .userLogProvider(userLogProvider)
                .dependencies(dependencies)
                .extensions(extensions)
                .databaseEventListeners(databaseEventListeners);
    }

    @Override
    public <T> DatabaseManagementServiceBuilderImplementation setConfig(Setting<T> setting, T value) {
        if (value == null) {
            config.remove(setting);
        } else {
            config.set(setting, value);
        }
        return this;
    }

    @Override
    public DatabaseManagementServiceBuilderImplementation setConfig(Map<Setting<?>, Object> config) {
        this.config.set(config);
        return this;
    }

    public DatabaseManagementServiceBuilderImplementation setConfigRaw(Map<String, String> raw) {
        config.setRaw(raw);
        return this;
    }

    @Override
    public DatabaseManagementServiceBuilderImplementation loadPropertiesFromFile(Path path) {
        try {
            config.fromFileNoThrow(path);
        } catch (Exception e) {
            throw new IllegalArgumentException("Unable to load " + path, e);
        }
        return this;
    }
}
