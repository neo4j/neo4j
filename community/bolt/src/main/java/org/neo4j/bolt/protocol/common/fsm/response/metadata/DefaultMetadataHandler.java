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
package org.neo4j.bolt.protocol.common.fsm.response.metadata;

import org.neo4j.bolt.protocol.common.fsm.response.MetadataConsumer;
import org.neo4j.graphdb.QueryStatistics;
import org.neo4j.values.storable.BooleanValue;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.MapValueBuilder;

public final class DefaultMetadataHandler extends AbstractMetadataHandler {
    private static final DefaultMetadataHandler INSTANCE = new DefaultMetadataHandler();

    private DefaultMetadataHandler() {}

    public static DefaultMetadataHandler getInstance() {
        return INSTANCE;
    }

    @Override
    protected void generateUpdateQueryStatistics(MapValueBuilder metadata, QueryStatistics statistics) {
        metadata.add("contains-updates", BooleanValue.TRUE);
        super.generateUpdateQueryStatistics(metadata, statistics);
    }

    @Override
    protected void generateSystemQueryStatistics(MapValueBuilder metadata, QueryStatistics statistics) {
        metadata.add("contains-system-updates", BooleanValue.TRUE);
        super.generateSystemQueryStatistics(metadata, statistics);
    }

    @Override
    public void onRoutingTable(MetadataConsumer consumer, String databaseName, MapValue routingTable) {
        // from 4.4 onwards, the target database is included within the routing table response to
        // facilitate resolving of the home database when connecting to a server without explicitly
        // selecting the target database
        routingTable = routingTable.updatedWith("db", Values.stringValue(databaseName));

        super.onRoutingTable(consumer, databaseName, routingTable);
    }
}
