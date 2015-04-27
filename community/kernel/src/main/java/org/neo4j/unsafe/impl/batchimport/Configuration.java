/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
public interface Configuration extends org.neo4j.unsafe.impl.batchimport.staging.Configuration
{
    /**
     * File name in which bad entries from the import will end up. This file will be created in the
     * database directory of the imported database, i.e. <into>/bad.log.
     */
    String BAD_FILE_NAME = "bad.log";

    /**
     * Memory dedicated to buffering data to be written to each store file.
     */
    int fileChannelBufferSize();

    /**
     * Some files require a bigger buffer to avoid some performance culprits imposed by the OS.
     * This is a multiplier for how many times bigger such buffers are compared to {@link #fileChannelBufferSize()}.
     */
    int bigFileChannelBufferSizeMultiplier();

    /**
     * The number of relationships threshold for considering a node dense.
     */
    int denseNodeThreshold();

    /**
     * Max number of I/O threads doing file write operations. Optimal value for this setting is heavily
     * dependent on hard drive. A spinning disk is most likely best off with 1, where an SSD may see
     * better performance with a handful of threads writing to it simultaneously.
     * This value eats into the cake of {@link #maxNumberOfProcessors()}. The total number of threads
     * used by the importer at any given time is {@link #maxNumberOfProcessors()}, out of those
     * a maximum number of I/O threads can be used.
     *   "Processor" in the context of the batch importer is different from "thread" since when discovering
     * how many processors are fully in use there's a calculation where one thread takes up 0 < fraction <= 1
     * of a processor.
     */
    int maxNumberOfIoProcessors();

    /**
     * Rough max number of processors (CPU cores) simultaneously used in total by importer at any given time.
     * This value should be set including {@link #maxNumberOfIoProcessors()} in mind.
     * Defaults to the value provided by the {@link Runtime#availableProcessors() jvm}. There's a discrete
     * number of threads that needs to be used just to get the very basics of the import working,
     * so for that reason there's no lower bound to this value.
     *   "Processor" in the context of the batch importer is different from "thread" since when discovering
     * how many processors are fully in use there's a calculation where one thread takes up 0 < fraction <= 1
     * of a processor.
     */
    int maxNumberOfProcessors();

    class Default
            extends org.neo4j.unsafe.impl.batchimport.staging.Configuration.Default
            implements Configuration
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

        @Override
        public int bigFileChannelBufferSizeMultiplier()
        {
            return 50;
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
        public int maxNumberOfIoProcessors()
        {
            return max( 2, Runtime.getRuntime().availableProcessors()/3 );
        }

        @Override
        public int maxNumberOfProcessors()
        {
            return Runtime.getRuntime().availableProcessors();
        }

        @Override
        public int movingAverageSize()
        {
            return 100;
        }
    }

    Configuration DEFAULT = new Default();

    class Overridden
            extends org.neo4j.unsafe.impl.batchimport.staging.Configuration.Overridden
            implements Configuration
    {
        private final Configuration defaults;
        private final Config config;

        public Overridden( Configuration defaults, Config config )
        {
            super( defaults );
            this.defaults = defaults;
            this.config = config;
        }

        public Overridden( Config config )
        {
            this( Configuration.DEFAULT, config );
        }

        @Override
        public int fileChannelBufferSize()
        {
            return defaults.fileChannelBufferSize();
        }

        @Override
        public int bigFileChannelBufferSizeMultiplier()
        {
            return defaults.bigFileChannelBufferSizeMultiplier();
        }

        @Override
        public int denseNodeThreshold()
        {
            return config.get( GraphDatabaseSettings.dense_node_threshold );
        }

        @Override
        public int maxNumberOfIoProcessors()
        {
            return defaults.maxNumberOfIoProcessors();
        }

        @Override
        public int maxNumberOfProcessors()
        {
            return defaults.maxNumberOfProcessors();
        }

        @Override
        public int movingAverageSize()
        {
            return defaults.movingAverageSize();
        }
    }
}
