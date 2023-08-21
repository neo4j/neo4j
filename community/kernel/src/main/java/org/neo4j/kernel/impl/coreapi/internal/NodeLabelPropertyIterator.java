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
package org.neo4j.kernel.impl.coreapi.internal;

import org.neo4j.graphdb.Node;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.NodeLabelIndexCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.kernel.api.ResourceMonitor;
import org.neo4j.storageengine.api.PropertySelection;

public class NodeLabelPropertyIterator extends PropertyFilteringIterator<Node, NodeLabelIndexCursor, NodeCursor> {
    private final Read read;

    public NodeLabelPropertyIterator(
            Read read,
            NodeLabelIndexCursor nodeLabelCursor,
            NodeCursor nodeCursor,
            PropertyCursor propertyCursor,
            CursorEntityFactory<NodeLabelIndexCursor, Node> nodeFactory,
            ResourceMonitor resourceMonitor,
            PropertyIndexQuery... queries) {
        super(nodeLabelCursor, nodeCursor, propertyCursor, nodeFactory, resourceMonitor, queries);
        this.read = read;
    }

    @Override
    protected long entityReference(NodeLabelIndexCursor cursor) {
        return cursor.nodeReference();
    }

    @Override
    protected void singleEntity(long id, NodeCursor cursor) {
        read.singleNode(id, cursor);
    }

    @Override
    protected void properties(
            NodeCursor nodeCursor, PropertyCursor propertyCursor, PropertySelection propertySelection) {
        nodeCursor.properties(propertyCursor, propertySelection);
    }
}
