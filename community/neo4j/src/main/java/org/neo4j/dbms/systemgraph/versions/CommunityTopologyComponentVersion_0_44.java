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

import static org.neo4j.dbms.systemgraph.CommunityTopologyGraphVersion.COMMUNITY_TOPOLOGY_44;

import org.neo4j.graphdb.Transaction;

/**
 * This is the CommunityTopologyComponent version for Neo4j 4.4
 */
public class CommunityTopologyComponentVersion_0_44 extends KnownCommunityTopologyComponentVersion {
    public CommunityTopologyComponentVersion_0_44() {
        super(COMMUNITY_TOPOLOGY_44);
    }

    @Override
    public void upgradeTopologyGraph(Transaction tx, int fromVersion) throws Exception {
        if (fromVersion < version) {
            this.setVersionProperty(tx, version);
        }
    }
}
