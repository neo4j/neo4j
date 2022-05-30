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
package org.neo4j.bolt.protocol.io;

import static org.neo4j.bolt.protocol.io.BoltStructType.NODE;
import static org.neo4j.bolt.protocol.io.BoltStructType.PATH;
import static org.neo4j.bolt.protocol.io.BoltStructType.RELATIONSHIP;
import static org.neo4j.bolt.protocol.io.BoltStructType.UNBOUND_RELATIONSHIP;

import java.util.ArrayList;
import org.neo4j.packstream.io.PackstreamBuf;
import org.neo4j.packstream.io.value.PackstreamValueWriter;
import org.neo4j.packstream.struct.StructHeader;
import org.neo4j.packstream.util.PrimitiveLongIntKeyValueArray;
import org.neo4j.util.VisibleForTesting;
import org.neo4j.values.storable.TextArray;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.NodeValue;
import org.neo4j.values.virtual.RelationshipValue;

public class BoltValueWriter extends PackstreamValueWriter {

    public BoltValueWriter(PackstreamBuf target) {
        super(target);
    }

    protected void writeHeader(long length, BoltStructType type) {
        this.buf.writeStructHeader(new StructHeader(length, type.getTag()));
    }

    protected void writeHeader(BoltStructType type) {
        this.writeHeader(type.getDefaultSize(), type);
    }

    @Override
    public void writeNodeReference(long nodeId) throws RuntimeException {}

    @Override
    public void writeNode(long nodeId, TextArray labels, MapValue properties, boolean isDeleted) {
        this.writeHeader(NODE);
        this.buf.writeInt(nodeId);

        this.buf.writeListHeader(labels.length());
        labels.forEach(label -> this.buf.writeString(((TextValue) label).stringValue()));

        properties.writeTo(this);
    }

    @Override
    public void writeRelationship(
            long relId, long startNodeId, long endNodeId, TextValue type, MapValue properties, boolean isDeleted) {
        this.writeHeader(RELATIONSHIP);

        this.buf.writeInt(relId).writeInt(startNodeId).writeInt(endNodeId).writeString(type.stringValue());

        properties.writeTo(this);
    }

    /**
     * Encodes an unbound relationship structure to a given buffer.
     *
     * @param relId      a relationship id.
     * @param type       a type string.
     * @param properties a set of properties.
     */
    @VisibleForTesting
    void writeUnboundRelationship(long relId, String type, MapValue properties) {
        writeHeader(UNBOUND_RELATIONSHIP);

        this.buf.writeInt(relId).writeString(type);

        properties.writeTo(this);
    }

    /**
     * Encodes a path structure to a given buffer.
     * <p>
     * Within Packstream, paths are encoded as three separate lists: Nodes, Relationships and Indices where the first
     * two simply encode the unique nodes and relationships present within the path.
     * <p>
     * The indices list alternates between the indices of the nodes and relationships within the nodes/relationships
     * lists respectively (beginning with the first relationship as the first node within the path will _ALWAYS_ be the
     * first node within th nodes list). While node indices are zero-based as usual, relationship indices are one-based
     * and signed in order to denote direction. For instance, the index {@code -1} refers to the 0th relationship with
     * its direction reversed (e.g. it traverses right-to-left instead of left-to-right).
     * <p>
     * For example:
     * <pre>
     *     (pers:Person)-[o:OWNS]->(comp:Computer)<-[m:Makes]-(ven:Vendor)
     *
     *     [pers, comp, ven]
     *     [o, m]
     *     [1, 1, -2, 2]
     * </pre>
     * <p>
     * Where the indices are interpreted as follows:
     * <ul>
     *     <li>{@code #0} (Value: {@code 1}) => Relationship at Index {@code 1 - 1} (#0) => {@code o}</li>
     *     <li>{@code #1} (Value: {@code 1}) => Node at Index #1 => {@code comp}</li>
     *     <li>{@code #2} (Value {@code -2}) => Relationship at Index {@code abs(-2) - 1} (#1) => {@code m}</li>
     *     <li>{@code #3} (Value {@code 2}) => Node at Index #2 => {@code ven}</li>
     * </ul>
     * <p>
     * <b>Important:</b> The first element within the {@code nodes} parameter is expected to be the first node within
     * the encoded path.
     *
     * @param nodes         an array of nodes.
     * @param relationships an array of relationships.
     */
    @Override
    public void writePath(NodeValue[] nodes, RelationshipValue[] relationships) throws RuntimeException {
        writeHeader(PATH);

        if (nodes.length == 0) {
            throw new IllegalArgumentException("Illegal node/relationship combination: path contains no nodes");
        }

        if (relationships.length == 0) {
            buf.writeListHeader(1);
            nodes[0].writeTo(this);

            buf.writeListHeader(0).writeListHeader(0);
            return;
        }

        var nodeIndices = new PrimitiveLongIntKeyValueArray(nodes.length);
        var relationshipIndices = new PrimitiveLongIntKeyValueArray(relationships.length);

        var reducedNodes = new ArrayList<NodeValue>();
        for (var node : nodes) {
            if (!nodeIndices.putIfAbsent(node.id(), reducedNodes.size())) {
                continue;
            }

            reducedNodes.add(node);
        }

        var reducedRelationships = new ArrayList<RelationshipValue>();
        for (var rel : relationships) {
            if (!relationshipIndices.putIfAbsent(rel.id(), reducedRelationships.size())) {
                continue;
            }

            reducedRelationships.add(rel);
        }

        buf.writeList(reducedNodes, (b, node) -> node.writeTo(this));
        buf.writeList(
                reducedRelationships,
                (b, rel) -> this.writeUnboundRelationship(rel.id(), rel.type().stringValue(), rel.properties()));

        buf.writeListHeader(relationships.length * 2);
        var currentOrigin = nodes[0];
        for (var i = 0; i < reducedRelationships.size(); ++i) {
            var relationship = reducedRelationships.get(i);

            int targetIndex;
            if (currentOrigin.id() == relationship.startNodeId()) {
                buf.writeInt(i + 1);
            } else {
                buf.writeInt((-i) - 1);
            }

            targetIndex = nodeIndices.getOrDefault(nodes[i + 1].id(), -1);

            if (targetIndex == -1) {
                throw new IllegalArgumentException(
                        "Illegal node/relationship combination: Cannot locate target node for relationship #" + i);
            }

            buf.writeInt(targetIndex);
            currentOrigin = reducedNodes.get(targetIndex);
        }
    }
}
