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

import static org.neo4j.io.ByteUnit.mebiBytes;

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
     * @return number of relationships threshold for considering a node dense.
     */
    int denseNodeThreshold();

    /**
     * @return page size for the page cache managing the store.
     */
    long pageSize();

    class Default
            extends org.neo4j.unsafe.impl.batchimport.staging.Configuration.Default
            implements Configuration
    {
        @Override
        public int batchSize()
        {
            return 10_000;
        }

        @Override
        public long pageSize()
        {
            return mebiBytes( 8 );
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
        public long pageSize()
        {
            return defaults.pageSize();
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
