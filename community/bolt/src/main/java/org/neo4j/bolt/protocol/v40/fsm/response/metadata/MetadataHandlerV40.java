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
package org.neo4j.bolt.protocol.v40.fsm.response.metadata;

import org.neo4j.bolt.protocol.common.fsm.response.metadata.AbstractLegacyMetadataHandler;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.MapValueBuilder;

/**
 * Facilitates the generation of metadata entries for versions 4.3 and
 * older
 */
public final class MetadataHandlerV40 extends AbstractLegacyMetadataHandler {
    private static final MetadataHandlerV40 INSTANCE = new MetadataHandlerV40();

    private MetadataHandlerV40() {}

    public static MetadataHandlerV40 getInstance() {
        return INSTANCE;
    }

    @Override
    protected void addContainsUpdates(MapValueBuilder metadata) {
        // Protocol doesn't supports contains updates flag
    }

    @Override
    protected void addContainsSystemUpdates(MapValueBuilder metadata) {
        // Protocol doesn't supports contains system updates flag
    }

    @Override
    protected MapValue enrichRoutingTable(String databaseName, MapValue routingTable) {
        // Nothing to add
        return routingTable;
    }
}
