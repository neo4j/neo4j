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
package org.neo4j.dbms.database;

import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;

import java.util.function.Function;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.util.Preconditions;

/**
 * These components only care about the version number
 */
public abstract class AbstractVersionComponent<T extends ComponentVersion> extends AbstractSystemGraphComponent
        implements SystemGraphComponentWithVersion {
    private final SystemGraphComponent.Name componentName;
    protected final T latestVersion;
    protected final Function<Integer, T> convertToVersion;

    public AbstractVersionComponent(
            SystemGraphComponent.Name componentName,
            T latestVersion,
            Config config,
            Function<Integer, T> convertFunction) {
        super(config);
        this.componentName = componentName;
        this.latestVersion = latestVersion;
        this.convertToVersion = convertFunction;
    }

    abstract T getFallbackVersion();

    @Override
    public Name componentName() {
        return componentName;
    }

    @Override
    public int getLatestSupportedVersion() {
        return latestVersion.getVersion();
    }

    @Override
    public Status detect(Transaction tx) {
        try {
            Integer versionNumber = getVersion(tx, componentName);
            if (versionNumber == null) {
                return Status.UNINITIALIZED;
            } else {
                T version = convertToVersion.apply(versionNumber);
                if (latestVersion.isGreaterThan(version)) {
                    return Status.REQUIRES_UPGRADE;
                } else if (latestVersion.equals(version)) {
                    return Status.CURRENT;
                } else {
                    return Status.UNSUPPORTED_FUTURE;
                }
            }
        } catch (IllegalArgumentException e) {
            return Status.UNSUPPORTED_FUTURE;
        }
    }

    @Override
    public void initializeSystemGraph(GraphDatabaseService system, boolean firstInitialization) throws Exception {
        boolean mayUpgrade = config.get(GraphDatabaseInternalSettings.automatic_upgrade_enabled);

        Preconditions.checkState(
                system.databaseName().equals(SYSTEM_DATABASE_NAME),
                "Cannot initialize system graph on database '" + system.databaseName() + "'");

        Status status;
        try (Transaction tx = system.beginTx()) {
            status = detect(tx);
            tx.commit();
        }

        switch (status) {
            case CURRENT:
                break;
            case UNINITIALIZED:
                if (mayUpgrade || firstInitialization) {
                    initializeSystemGraphModel(system);
                }
                break;
            case REQUIRES_UPGRADE:
                if (mayUpgrade) {
                    upgradeToCurrent(system);
                }
                break;
            default:
                throw new IllegalStateException(String.format(
                        "Unsupported component state for '%s': %s", componentName(), status.description()));
        }
    }

    @Override
    protected void initializeSystemGraphModel(GraphDatabaseService system) throws Exception {
        SystemGraphComponent.executeWithFullAccess(system, this::setToLatestVersion);
    }

    void setToLatestVersion(Transaction tx) {
        try (var nodes = tx.findNodes(VERSION_LABEL)) {
            var node = nodes.stream().findFirst().orElseGet(() -> tx.createNode(VERSION_LABEL));

            node.setProperty(componentName.name(), latestVersion.getVersion());
        }
    }

    @Override
    public void upgradeToCurrent(GraphDatabaseService system) throws Exception {
        initializeSystemGraphModel(system);
    }

    T fetchStateFromSystemDatabase(GraphDatabaseService system) {
        T result = getFallbackVersion();
        try (var tx = system.beginTx()) {
            Integer version = getVersion(tx, componentName);
            if (version != null) {
                result = convertToVersion.apply(version);
            }
        }
        return result;
    }
}
