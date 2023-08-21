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
package org.neo4j.fabric.config;

import java.time.Duration;
import java.util.function.Supplier;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;

public class FabricConfig {
    private final Supplier<Duration> transactionTimeout;
    private final DataStream dataStream;
    private final boolean routingEnabled;

    public FabricConfig(Supplier<Duration> transactionTimeout, DataStream dataStream, boolean routingEnabled) {
        this.transactionTimeout = transactionTimeout;
        this.dataStream = dataStream;
        this.routingEnabled = routingEnabled;
    }

    public Duration getTransactionTimeout() {
        return transactionTimeout.get();
    }

    public DataStream getDataStream() {
        return dataStream;
    }

    public boolean isRoutingEnabled() {
        return routingEnabled;
    }

    public static FabricConfig from(Config config) {
        var syncBatchSize = FabricConstants.BATCH_SIZE;
        // the rest of the settings are not used for any type of queries supported in CE
        var dataStream = new DataStream(0, 0, syncBatchSize, 0);
        return new FabricConfig(() -> config.get(GraphDatabaseSettings.transaction_timeout), dataStream, false);
    }

    public static class DataStream {
        private final int bufferLowWatermark;
        private final int bufferSize;
        private final int batchSize;
        private final int concurrency;

        public DataStream(int bufferLowWatermark, int bufferSize, int batchSize, int concurrency) {
            if (bufferLowWatermark > bufferSize) {
                this.bufferLowWatermark = bufferSize;
            } else {
                this.bufferLowWatermark = bufferLowWatermark;
            }

            this.bufferSize = bufferSize;
            this.batchSize = batchSize;
            this.concurrency = concurrency;
        }

        public int getBufferLowWatermark() {
            return bufferLowWatermark;
        }

        public int getBufferSize() {
            return bufferSize;
        }

        public int getBatchSize() {
            return batchSize;
        }

        public int getConcurrency() {
            return concurrency;
        }
    }
}
