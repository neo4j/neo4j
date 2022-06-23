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

import org.neo4j.packstream.io.PackstreamBuf;
import org.neo4j.values.storable.TextArray;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.virtual.MapValue;

public class DefaultBoltValueWriter extends LegacyBoltValueWriter {

    public DefaultBoltValueWriter(PackstreamBuf target) {
        super(target);
    }

    @Override
    protected void writeHeader(BoltStructType type) {
        super.writeHeader(type.getDefaultSize(), type);
    }

    @Override
    public void writeNode(String elementId, long nodeId, TextArray labels, MapValue properties, boolean isDeleted) {
        super.writeNode(elementId, nodeId, labels, properties, isDeleted);

        buf.writeString(elementId);
    }

    @Override
    public void writeRelationship(
            String elementId,
            long relId,
            String startNodeElementId,
            long startNodeId,
            String endNodeElementId,
            long endNodeId,
            TextValue type,
            MapValue properties,
            boolean isDeleted) {
        super.writeRelationship(
                elementId,
                relId,
                startNodeElementId,
                startNodeId,
                endNodeElementId,
                endNodeId,
                type,
                properties,
                isDeleted);

        buf.writeString(elementId).writeString(startNodeElementId).writeString(endNodeElementId);
    }

    @Override
    void writeUnboundRelationship(String elementId, long relId, String type, MapValue properties) {
        super.writeUnboundRelationship(elementId, relId, type, properties);

        buf.writeString(elementId);
    }
}
