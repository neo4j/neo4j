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

import static org.neo4j.dbms.database.SystemGraphComponent.VERSION_LABEL;

import org.neo4j.configuration.Config;
import org.neo4j.dbms.database.SystemGraphComponent.Name;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.logging.Log;

/**
 * Version of a system graph component.
 * Components that due to breaking schema changes requires migrations also needs versions.
 * Each component has its own version scheme starting at 0 and increasing each release with breaking changes.
 * The versions should be on the format [component]Version_[versionNbr]_[neo4jRelease].
 * The version schemes are described in {@link ComponentVersion}.
 */
public abstract class KnownSystemComponentVersion {
    public static final int UNKNOWN_VERSION = -1;

    private final ComponentVersion componentVersion;
    protected final SystemGraphComponent.Name componentName;
    public final int version;
    public final String description;
    protected final Log debugLog;

    protected KnownSystemComponentVersion(ComponentVersion componentVersion, Log debugLog) {
        this.componentVersion = componentVersion;
        this.componentName = componentVersion.getComponentName();
        this.version = componentVersion.getVersion();
        this.description = componentVersion.getDescription();
        this.debugLog = debugLog;
    }

    public boolean isCurrent(Config config) {
        return componentVersion.isCurrent(config);
    }

    public boolean migrationSupported() {
        return componentVersion.migrationSupported();
    }

    public boolean runtimeSupported() {
        return componentVersion.runtimeSupported();
    }

    protected Integer getSystemGraphInstalledVersion(Transaction tx) {
        return SystemGraphComponent.getVersionNumber(tx, componentName);
    }

    /**
     * The version associated with this component **in this copy of the Neo4j binaries**.
     * This may be different to the value returned by {@link #getSystemGraphInstalledVersion(Transaction)}.
     *
     * @return the positive integer number associated with this component's version
     */
    public int binaryVersion() {
        return version;
    }

    public boolean detected(Transaction tx) {
        Integer version = getSystemGraphInstalledVersion(tx);
        return version != null && version == this.version;
    }

    public UnsupportedOperationException unsupported() {
        String message = String.format(
                "System graph version %d for component '%s' in '%s' is not supported",
                version, componentName, description);
        debugLog.error(message);
        return new UnsupportedOperationException(message);
    }

    public SystemGraphComponent.Status getStatus(Config config) {
        if (this.version == UNKNOWN_VERSION) {
            return SystemGraphComponent.Status.UNINITIALIZED;
        } else if (this.isCurrent(config)) {
            return SystemGraphComponent.Status.CURRENT;
        } else if (this.migrationSupported()) {
            return this.runtimeSupported()
                    ? SystemGraphComponent.Status.REQUIRES_UPGRADE
                    : SystemGraphComponent.Status.UNSUPPORTED_BUT_CAN_UPGRADE;
        } else {
            return SystemGraphComponent.Status.UNSUPPORTED;
        }
    }

    protected static boolean nodesWithLabelExist(Transaction tx, Label label) {
        try (ResourceIterator<Node> nodes = tx.findNodes(label)) {
            return nodes.hasNext();
        }
    }

    public void setVersionProperty(Transaction tx, int newVersion) {
        setVersionProperty(tx, newVersion, componentName, debugLog);
    }

    public static void setVersionProperty(Transaction tx, int newVersion, Name componentName, Log debugLog) {
        Node versionNode = findOrCreateVersionNode(tx);
        var oldVersion = versionNode.getProperty(componentName.name(), null);
        if (oldVersion != null) {
            debugLog.info(String.format(
                    "Upgrading '%s' version property from %s to %d", componentName, oldVersion, newVersion));
        } else {
            debugLog.info(String.format("Setting version for '%s' to %d", componentName, newVersion));
        }
        versionNode.setProperty(componentName.name(), newVersion);
    }

    private static Node findOrCreateVersionNode(Transaction tx) {
        try (ResourceIterator<Node> nodes = tx.findNodes(VERSION_LABEL)) {
            if (nodes.hasNext()) {
                Node node = nodes.next();
                if (nodes.hasNext()) {
                    throw new IllegalStateException("More than one Version node exists");
                }
                return node;
            }
        }
        return tx.createNode(VERSION_LABEL);
    }
}
