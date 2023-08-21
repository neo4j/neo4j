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

import static org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.DATABASE_ACCESS_PROPERTY;
import static org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.DATABASE_LABEL;
import static org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.DATABASE_NAME_LABEL;
import static org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.DATABASE_NAME_PROPERTY;
import static org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.NAME_PROPERTY;
import static org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.PRIMARY_PROPERTY;
import static org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.TARGETS_RELATIONSHIP;

import java.util.stream.Stream;
import org.neo4j.dbms.database.ComponentVersion;
import org.neo4j.dbms.database.KnownSystemComponentVersion;
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.logging.NullLog;

public abstract class KnownCommunityTopologyComponentVersion extends KnownSystemComponentVersion {
    KnownCommunityTopologyComponentVersion(ComponentVersion componentVersion) {
        super(componentVersion, NullLog.getInstance());
    }

    /**
     * Initialize the topology graph to this version.
     *
     * @param tx open transaction to perform the initialization in
     */
    public void initializeTopologyGraph(Transaction tx) {
        setDatabaseAccessToReadWrite(tx);
        addDatabaseNameNodes(tx);
    }

    /**
     * Set access property to read-write for all existing nodes
     *
     * @param tx open transaction to perform the method in
     */
    protected void setDatabaseAccessToReadWrite(Transaction tx) {
        String rwString = TopologyGraphDbmsModel.DatabaseAccess.READ_WRITE.toString();
        Iterators.forEachRemaining(
                tx.findNodes(DATABASE_LABEL), node -> node.setProperty(DATABASE_ACCESS_PROPERTY, rwString));
    }

    /**
     * Add DatabaseName nodes to Database nodes
     * (:Database)<-[:TARGETS]-(:DatabaseName)
     * @param tx open transaction to perform the method in
     */
    protected void addDatabaseNameNodes(Transaction tx) {
        Iterators.forEachRemaining(tx.findNodes(DATABASE_LABEL), databaseNode -> {
            if (!hasPrimaryAlias(databaseNode)) {
                Node nameNode = tx.createNode(DATABASE_NAME_LABEL);
                nameNode.setProperty(NAME_PROPERTY, databaseNode.getProperty(DATABASE_NAME_PROPERTY));
                nameNode.setProperty(PRIMARY_PROPERTY, true);
                nameNode.createRelationshipTo(databaseNode, TARGETS_RELATIONSHIP);
            }
        });
    }

    private boolean hasPrimaryAlias(Node node) {
        try (Stream<Relationship> targets = node.getRelationships(TARGETS_RELATIONSHIP).stream()) {
            return targets.anyMatch(
                    r -> r.getStartNode().getProperty(PRIMARY_PROPERTY).equals(true));
        }
    }

    /**
     * Upgrade the topology graph to this version.
     * This method recursively calls older versions and performs the upgrades in steps.
     *
     * @param tx open transaction to perform the upgrade in
     * @param fromVersion the detected version, upgrade will be performed rolling from this
     */
    public abstract void upgradeTopologyGraph(Transaction tx, int fromVersion) throws Exception;
}
