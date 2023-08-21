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
package org.neo4j.bolt.protocol.common.connector.connection;

import java.util.HashMap;
import java.util.Map;
import org.neo4j.bolt.protocol.io.StructType;
import org.neo4j.bolt.protocol.io.pipeline.WriterPipeline;
import org.neo4j.bolt.protocol.io.reader.DateTimeReader;
import org.neo4j.bolt.protocol.io.reader.DateTimeZoneIdReader;
import org.neo4j.bolt.protocol.io.writer.UtcStructWriter;
import org.neo4j.packstream.struct.StructRegistry;
import org.neo4j.values.storable.Value;

/**
 * Provides a list of features which are recognized by connections and may be toggled on based on negotiation.
 */
public enum Feature {

    /**
     * Enables the transmission of date times using UTC based timestamps.
     * <p />
     * This functionality is enabled in protocol versions from 5.0 onwards and may optionally be enabled through the
     * "utc" bugfix flag within all 4.x protocol revisions.
     */
    UTC_DATETIME("utc") {
        @Override
        public StructRegistry<Connection, Value> decorateStructRegistry(StructRegistry<Connection, Value> delegate) {
            return delegate.builderOf()
                    .unregisterReader(StructType.DATE_TIME_LEGACY.getTag())
                    .unregisterReader(StructType.DATE_TIME_ZONE_ID_LEGACY.getTag())
                    .register(DateTimeReader.getInstance())
                    .register(DateTimeZoneIdReader.getInstance())
                    .build();
        }

        @Override
        @SuppressWarnings("removal")
        public void configureWriterPipeline(WriterPipeline pipeline) {
            pipeline.addFirst(UtcStructWriter.getInstance());
        }
    };

    private static final Map<String, Feature> idToFeatureMap = new HashMap<>();

    static {
        for (var feature : values()) {
            idToFeatureMap.put(feature.id, feature);
        }
    }

    private final String id;

    Feature(String id) {
        this.id = id;
    }

    public static Feature findFeatureById(String id) {
        return idToFeatureMap.get(id);
    }

    /**
     * Provides a globally unique identifier via which this feature is typically referred to when selected through
     * capability negotiation.
     *
     * @return an identifier.
     */
    public String getId() {
        return id;
    }

    /**
     * Decorates the struct registry for a given selected protocol.
     * <p />
     * If a feature does not alter the encoding of values, {@code delegate} is returned as-is instead.
     *
     * @param delegate an implementation to delegate to.
     * @return a decorated implementation.
     */
    public StructRegistry<Connection, Value> decorateStructRegistry(StructRegistry<Connection, Value> delegate) {
        return delegate;
    }

    /**
     * Decorates the writer pipeline of a given selected protocol.
     * <p />
     * If a feature does not alter the encoding of values, this method should be left unimplemented.
     *
     * @param pipeline a pipeline.
     */
    public void configureWriterPipeline(WriterPipeline pipeline) {}
}
