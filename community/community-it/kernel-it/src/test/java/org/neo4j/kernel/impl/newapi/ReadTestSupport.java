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
package org.neo4j.kernel.impl.newapi;

import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import org.neo4j.common.DependencyResolver;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.kernel.api.Kernel;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.monitoring.Monitors;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

public class ReadTestSupport implements KernelAPIReadTestSupport {
    private final Map<Setting<?>, Object> settings = new HashMap<>();
    private GraphDatabaseService db;
    private DatabaseManagementService managementService;
    private Monitors monitors = new Monitors();

    public <T> void addSetting(Setting<T> setting, T value) {
        settings.put(setting, value);
    }

    public void setMonitors(Monitors monitors) {
        this.monitors = monitors;
    }

    @Override
    public void setup(Path storeDir, Consumer<GraphDatabaseService> create, Consumer<GraphDatabaseService> sysCreate) {
        TestDatabaseManagementServiceBuilder databaseManagementServiceBuilder = newManagementServiceBuilder(storeDir);
        databaseManagementServiceBuilder.setConfig(settings);
        databaseManagementServiceBuilder.setMonitors(monitors);
        managementService = databaseManagementServiceBuilder.build();
        db = managementService.database(DEFAULT_DATABASE_NAME);
        GraphDatabaseService sysDb = managementService.database(SYSTEM_DATABASE_NAME);
        create.accept(db);
        sysCreate.accept(sysDb);
    }

    protected TestDatabaseManagementServiceBuilder newManagementServiceBuilder(Path storeDir) {
        return new TestDatabaseManagementServiceBuilder(storeDir).impermanent();
    }

    @Override
    public Kernel kernelToTest() {
        DependencyResolver resolver = ((GraphDatabaseAPI) this.db).getDependencyResolver();
        return resolver.resolveDependency(Kernel.class);
    }

    @Override
    public void tearDown() {
        managementService.shutdown();
        db = null;
    }
}
