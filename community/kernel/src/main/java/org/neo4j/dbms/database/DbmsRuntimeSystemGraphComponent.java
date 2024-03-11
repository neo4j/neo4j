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

import static org.neo4j.dbms.database.ComponentVersion.DBMS_RUNTIME_COMPONENT;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Iterators;

public class DbmsRuntimeSystemGraphComponent extends AbstractVersionComponent<DbmsRuntimeVersion> {
    public static final Label OLD_COMPONENT_LABEL = Label.label("DbmsRuntime");
    public static final String OLD_PROPERTY_NAME = "version";

    private final DbmsRuntimeVersion fallbackVersion;

    public DbmsRuntimeSystemGraphComponent(Config config) {
        super(
                DBMS_RUNTIME_COMPONENT,
                DbmsRuntimeVersion.getLatestVersion(config),
                config,
                DbmsRuntimeVersion::fromVersionNumber);

        final var semanticFallbackVersion = DbmsRuntimeVersion.V5_0;
        this.fallbackVersion = config.get(GraphDatabaseInternalSettings.fallback_to_latest_runtime_version)
                ? latestVersion
                : semanticFallbackVersion;
    }

    @Override
    public DbmsRuntimeVersion getFallbackVersion() {
        return fallbackVersion;
    }

    @Override
    public Integer getVersion(Transaction tx, Name componentName) {
        Integer result = null;
        try (ResourceIterator<Node> nodes = tx.findNodes(OLD_COMPONENT_LABEL)) {
            if (nodes.hasNext()) {
                Node versionNode = nodes.next();
                result = (Integer) versionNode.getProperty(OLD_PROPERTY_NAME, null);
            }
        }
        return result != null ? result : SystemGraphComponent.getVersionNumber(tx, componentName);
    }

    @Override
    public void upgradeToCurrent(GraphDatabaseService systemDb) throws Exception {
        SystemGraphComponent.executeWithFullAccess(systemDb, tx -> {
            Iterators.forEachRemaining(tx.findNodes(OLD_COMPONENT_LABEL), Node::delete);
            setToLatestVersion(tx);
        });
    }

    public Config config() {
        return config;
    }
}
