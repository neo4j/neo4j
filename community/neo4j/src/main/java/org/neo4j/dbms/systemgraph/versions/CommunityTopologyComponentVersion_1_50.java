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
package org.neo4j.dbms.systemgraph.versions;

import static org.neo4j.dbms.systemgraph.CommunityTopologyGraphVersion.COMMUNITY_TOPOLOGY_50;
import static org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.DATABASE_NAME_LABEL;
import static org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.DEFAULT_NAMESPACE;
import static org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.DISPLAY_NAME_PROPERTY;
import static org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.NAMESPACE_PROPERTY;
import static org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.NAME_PROPERTY;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Iterators;

/**
 * This is the CommunityTopologyComponent version for Neo4j 5.0.0
 */
public class CommunityTopologyComponentVersion_1_50 extends KnownCommunityTopologyComponentVersion {
    public CommunityTopologyComponentVersion_1_50() {
        super(COMMUNITY_TOPOLOGY_50);
    }

    @Override
    public void upgradeTopologyGraph(Transaction tx, int fromVersion) throws Exception {
        if (fromVersion < version) {
            this.setVersionProperty(tx, version);
            this.addDatabaseNameNodes(tx);
            this.addNamespaceProperties(tx);
        }
    }

    private void addNamespaceProperties(Transaction tx) {
        try (ResourceIterator<Node> nodes = tx.findNodes(DATABASE_NAME_LABEL)) {
            Iterators.forEachRemaining(nodes, databaseNameNode -> {
                if (!databaseNameNode.hasProperty(NAMESPACE_PROPERTY))
                    databaseNameNode.setProperty(NAMESPACE_PROPERTY, DEFAULT_NAMESPACE);
                if (!databaseNameNode.hasProperty(DISPLAY_NAME_PROPERTY))
                    databaseNameNode.setProperty(DISPLAY_NAME_PROPERTY, databaseNameNode.getProperty(NAME_PROPERTY));
            });
        }
    }
}
