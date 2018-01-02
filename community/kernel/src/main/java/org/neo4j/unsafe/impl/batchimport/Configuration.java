/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import org.neo4j.kernel.configuration.Config;

import static java.lang.Math.min;

import static org.neo4j.graphdb.factory.GraphDatabaseSettings.dense_node_threshold;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.pagecache_memory;
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
    long MAX_PAGE_CACHE_MEMORY = mebiBytes( 240 );

    /**
     * @return number of relationships threshold for considering a node dense.
     */
    int denseNodeThreshold();

    /**
     * @return amount of memory to reserve for the page cache. This should just be "enough" for it to be able
     * to sequentially read and write a couple of stores at a time. If configured too high then there will
     * be less memory available for other caches which are critical during the import. Optimal size is
     * estimated to be 100-200 MiB. The importer will figure out an optimal page size from this value,
     * with slightly bigger page size than "normal" random access use cases.
     */
    long pageCacheMemory();

    class Default
            extends org.neo4j.unsafe.impl.batchimport.staging.Configuration.Default
            implements Configuration
    {
        @Override
        public long pageCacheMemory()
        {
            // Get the upper bound of what we can get from the default config calculation
            // We even want to limit amount of memory a bit more since we don't need very much during import
            return min( MAX_PAGE_CACHE_MEMORY, new Config().get( pagecache_memory ) );
        }

        @Override
        public int denseNodeThreshold()
        {
            return Integer.parseInt( dense_node_threshold.getDefaultValue() );
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
        public long pageCacheMemory()
        {
            return min( MAX_PAGE_CACHE_MEMORY, config.get( pagecache_memory ) );
        }

        @Override
        public int denseNodeThreshold()
        {
            return config.get( dense_node_threshold );
        }

        @Override
        public int movingAverageSize()
        {
            return defaults.movingAverageSize();
        }
    }
}
