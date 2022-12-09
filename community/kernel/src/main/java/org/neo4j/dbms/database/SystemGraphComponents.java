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
package org.neo4j.dbms.database;

import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.dbms.database.ComponentVersion.SYSTEM_GRAPH_COMPONENT;
import static org.neo4j.dbms.database.SystemGraphComponent.VERSION_LABEL;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import org.neo4j.function.ThrowingConsumer;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.Exceptions;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.util.Preconditions;
import org.neo4j.util.VisibleForTesting;

/**
 * Central collection for managing multiple versioned system graph initializers. There could be several components in the DBMS that each have a requirement on
 * the system database to contain a graph with a specific schema. Each component needs to maintain that schema and support multiple versions of that schema in
 * order to allow rolling upgrades to be possible where newer versions of Neo4j will be able to run on older versions of the system database.
 * <p>
 * The core design is that each component is able to detect the version of their own sub-graph and from that decide if they can support it or not, and how to
 * upgrade from one version to another.
 */
public class SystemGraphComponents {
    private final Map<SystemGraphComponent.Name, SystemGraphComponent> componentMap = new LinkedHashMap<>();
    private final InternalLog log;

    /**
     * Latch to indicate that something has started reading the state of this component, implying that any changes
     * after this point are risky.
     */
    private boolean readAccessBegun;

    public SystemGraphComponents(InternalLogProvider logProvider) {
        this.log = logProvider.getLog(getClass());
    }

    public void register(SystemGraphComponent initializer) {
        if (readAccessBegun) {
            log.warn(String.format(
                    "Registering component '%s' after some reads of the component state have occurred",
                    initializer.componentName()));
        }
        if (componentMap.containsKey(initializer.componentName())) {
            throw new IllegalStateException("Component already registered: " + initializer.componentName());
        }
        componentMap.put(initializer.componentName(), initializer);
    }

    @VisibleForTesting
    @SuppressWarnings("WeakerAccess")
    public void deregister(SystemGraphComponent.Name key) {
        componentMap.remove(key);
    }

    public void forEachThrowing(ThrowingConsumer<SystemGraphComponent, Exception> process) throws Exception {
        ensureReadAccessBegun();
        for (SystemGraphComponent component : componentMap.values()) {
            process.accept(component);
        }
    }

    public void forEach(Consumer<SystemGraphComponent> process) {
        ensureReadAccessBegun();
        componentMap.values().forEach(process);
    }

    public static SystemGraphComponent.Name component() {
        return SYSTEM_GRAPH_COMPONENT;
    }

    public SystemGraphComponent.Status detect(Transaction tx) {
        ensureReadAccessBegun();
        return componentMap.values().stream()
                .map(c -> c.detect(tx))
                .reduce(SystemGraphComponent.Status::with)
                .orElse(SystemGraphComponent.Status.CURRENT);
    }

    public SystemGraphComponent.Status detect(GraphDatabaseService system) {
        ensureReadAccessBegun();
        return componentMap.values().stream()
                .map(c -> c.detect(system))
                .reduce(SystemGraphComponent.Status::with)
                .orElse(SystemGraphComponent.Status.CURRENT);
    }

    public void initializeSystemGraph(GraphDatabaseService system) {
        ensureReadAccessBegun();

        Preconditions.checkState(
                system.databaseName().equals(SYSTEM_DATABASE_NAME),
                "Cannot initialize system graph on database '" + system.databaseName() + "'");

        boolean newlyCreated;
        try (Transaction tx = system.beginTx();
                ResourceIterator<Node> nodes = tx.findNodes(VERSION_LABEL)) {
            newlyCreated = !nodes.hasNext();
        }

        Exception failure = null;
        for (SystemGraphComponent component : componentMap.values()) {
            try {
                component.initializeSystemGraph(system, newlyCreated);
            } catch (Exception e) {
                failure = Exceptions.chain(failure, e);
            }
        }

        if (failure != null) {
            throw new IllegalStateException(
                    "Failed to initialize system graph component: " + failure.getMessage(), failure);
        }
    }

    public void upgradeToCurrent(GraphDatabaseService system) throws Exception {
        ensureReadAccessBegun();

        Exception failure = null;
        for (SystemGraphComponent component : componentsToUpgrade(system)) {
            try {
                component.upgradeToCurrent(system);
            } catch (Exception e) {
                failure = Exceptions.chain(failure, e);
            }
        }

        if (failure != null) {
            throw new IllegalStateException("Failed to upgrade system graph:" + failure.getMessage(), failure);
        }
    }

    private List<SystemGraphComponent> componentsToUpgrade(GraphDatabaseService system) throws Exception {
        List<SystemGraphComponent> componentsToUpgrade = new ArrayList<>();
        SystemGraphComponent.executeWithFullAccess(system, tx -> componentMap.values().stream()
                .filter(c -> {
                    SystemGraphComponent.Status status = c.detect(tx);
                    return status == SystemGraphComponent.Status.UNSUPPORTED_BUT_CAN_UPGRADE
                            || status == SystemGraphComponent.Status.REQUIRES_UPGRADE
                            ||
                            // New components are not currently initialised in cluster deployment when new binaries are
                            // booted on top of an existing database.
                            // This is a known shortcoming of the lifecycle and a state transfer from UNINITIALIZED to
                            // CURRENT must be supported
                            // as a workaround until it is fixed.
                            status == SystemGraphComponent.Status.UNINITIALIZED;
                })
                .forEach(componentsToUpgrade::add));
        return componentsToUpgrade;
    }

    private void ensureReadAccessBegun() {
        readAccessBegun = true;
    }
}
