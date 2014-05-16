/**
 * Copyright (c) 2002-2014 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.unsafe.impl.batchimport;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.configuration.Config;

/**
 * User controlled configuration for a {@link BatchImporter}.
 */
public interface Configuration
{
    /**
     * Batch importer works with batches going through one or more stages where one or more threads
     * process each stage. This setting dictates how big the batches that are passed around are.
     */
    int batchSize();

    /**
     * Heap dedicated to buffering data to be written to each store file.
     */
    int fileChannelBufferSize();

    /**
     * Number of batches that a stage can queue up before awaiting the downstream stage to catch up.
     */
    int workAheadSize();

    int denseNodeThreshold();

    public static class Default implements Configuration
    {
        private static final int OPTIMAL_FILE_CHANNEL_CHUNK_SIZE = 1024*1024 * 4;

        @Override
        public int batchSize()
        {
            return 10_000;
        }

        @Override
        public int fileChannelBufferSize()
        {
            return OPTIMAL_FILE_CHANNEL_CHUNK_SIZE * 2;
        }

        @Override
        public int workAheadSize()
        {
            return 20;
        }

        @Override
        public int denseNodeThreshold()
        {
            return 50;
        }
    }

    public static final Configuration DEFAULT = new Default();

    public static class FromConfig extends Default
    {
        private final Config config;

        public FromConfig( Config config )
        {
            this.config = config;
        }

        @Override
        public int denseNodeThreshold()
        {
            return config.get( GraphDatabaseSettings.dense_node_threshold );
        }
    }

    // TODO Add Configuration option "calibrate()" which probes the hardware and returns optimal values.
}
