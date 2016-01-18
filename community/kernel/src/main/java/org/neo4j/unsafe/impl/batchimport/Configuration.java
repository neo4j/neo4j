/*
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
     * Memory dedicated to buffering data to be written.
     */
    int writeBufferSize();

    /**
     * The number of relationships threshold for considering a node dense.
     */
    int denseNodeThreshold();

    class Default
            extends org.neo4j.unsafe.impl.batchimport.staging.Configuration.Default
            implements Configuration
    {
        private static final int DEFAULT_PAGE_SIZE = 1024 * 8;

        @Override
        public int batchSize()
        {
            return 10_000;
        }

        @Override
        public int writeBufferSize()
        {
            // Do a little calculation here where the goal of the returned value is that if a file channel
            // would be seen as a batch itself (think asynchronous writing) there would be created roughly
            // as many as the other types of batches.
            int averageRecordSize = 40; // Gut-feel estimate
            int batchesToBuffer = 1000;
            int maxWriteBufferSize = batchSize() * averageRecordSize * batchesToBuffer;
            int writeBufferSize = (int) Math.min( maxWriteBufferSize, Runtime.getRuntime().maxMemory() / 5);
            return roundToClosest( writeBufferSize, DEFAULT_PAGE_SIZE * 30 );
        }

        private int roundToClosest( int value, int divisible )
        {
            double roughCount = (double) value / divisible;
            int count = (int) round( roughCount );
            return divisible*count;
        }

        @Override
        public int denseNodeThreshold()
        {
            return Integer.parseInt( GraphDatabaseSettings.dense_node_threshold.getDefaultValue() );
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
        public int writeBufferSize()
        {
            return defaults.writeBufferSize();
        }

        @Override
        public int denseNodeThreshold()
        {
            return config.get( GraphDatabaseSettings.dense_node_threshold );
        }

        @Override
        public int movingAverageSize()
        {
            return defaults.movingAverageSize();
        }
    }
}
