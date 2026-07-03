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
package org.neo4j.kernel.impl.core;

import java.util.concurrent.atomic.AtomicLong;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;

/**
 * Virtual nodes are returned from multiple places,
 * for example both the `db.schema.visualization` procedure and
 * the `SHOW CURRENT GRAPH TYPE AS GRAPH` command.
 * And since they can be combined into a single query we need a common MIN_ID counter
 * to not get overlapping IDs for the virtual nodes.
 */
public abstract class AbstractVirtualNode implements Node {
    // Don't start on -1 to avoid conflicts with NO_ID
    protected static final AtomicLong MIN_ID = new AtomicLong(-100);

    // Writes are not allowed on virtual nodes

    @Override
    public void delete() {}

    @Override
    public Relationship createRelationshipTo(Node otherNode, RelationshipType type) {
        return null;
    }

    @Override
    public void addLabel(Label label) {}

    @Override
    public void removeLabel(Label label) {}

    @Override
    public void setProperty(String key, Object value) {}

    @Override
    public Object removeProperty(String key) {
        return null;
    }
}
