/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import static java.lang.Math.max;
import static java.lang.Math.round;

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

    /**
     * The number of relationships threshold for considering a node dense.
     */
    int denseNodeThreshold();

    /**
     * Max number of I/O threads doing file write operations. Optimal value for this setting is heavily
     * dependent on hard drive. A spinning disk is most likely best off with 1, where an SSD may see
     * better performance with a handful of threads writing to it simultaneously.
     */
    int numberOfIoThreads();

    public static class Default implements Configuration
    {
        private static final int OPTIMAL_FILE_CHANNEL_CHUNK_SIZE = 1024 * 4;

        @Override
        public int batchSize()
        {
            return 10_000;
        }

        @Override
        public int fileChannelBufferSize()
        {
            // Do a little calculation here where the goal of the returned value is that if a file channel
            // would be seen as a batch itself (think asynchronous writing) there would be created roughly
            // as many as the other types of batches.
            return roundToClosest( batchSize() * 40 /*some kind of record size average*/,
                    OPTIMAL_FILE_CHANNEL_CHUNK_SIZE );
        }

        private int roundToClosest( int value, int divisible )
        {
            double roughCount = (double) value / divisible;
            int count = (int) round( roughCount );
            return divisible*count;
        }

        @Override
        public int workAheadSize()
        {
            return 20;
        }

        @Override
        public int denseNodeThreshold()
        {
            return Integer.parseInt( GraphDatabaseSettings.dense_node_threshold.getDefaultValue() );
        }

        @Override
        public int numberOfIoThreads()
        {
            return max( 2, Runtime.getRuntime().availableProcessors()/3 );
        }
    }

    public static final Configuration DEFAULT = new Default();

    public static class OverrideFromConfig implements Configuration
    {
        private final Configuration defaults;
        private final Config config;

        public OverrideFromConfig( Configuration defaults, Config config )
        {
            this.defaults = defaults;
            this.config = config;
        }

        public OverrideFromConfig( Config config )
        {
            this( DEFAULT, config );
        }

        @Override
        public int batchSize()
        {
            return defaults.batchSize();
        }

        @Override
        public int fileChannelBufferSize()
        {
            return defaults.fileChannelBufferSize();
        }

        @Override
        public int workAheadSize()
        {
            return defaults.workAheadSize();
        }

        @Override
        public int denseNodeThreshold()
        {
            return config.get( GraphDatabaseSettings.dense_node_threshold );
        }

        @Override
        public int numberOfIoThreads()
        {
            return defaults.numberOfIoThreads();
        }
    }

    // TODO Add Configuration option "calibrate()" which probes the hardware and returns optimal values.
}
