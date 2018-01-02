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
package org.neo4j.unsafe.impl.batchimport.staging;

/**
 * Configuration of a {@link Step}.
 */
public interface Configuration
{
    /**
     * A {@link Stage} works with batches going through one or more {@link Step steps} where one or more threads
     * process batches at each {@link Step}. This setting dictates how big the batches that are passed around are.
     */
    int batchSize();

    /**
     * For statistics the average processing time is based on total processing time divided by
     * number of batches processed. A total average is probably not that interesting so this configuration
     * option specifies how many of the latest processed batches counts in the equation above.
     */
    int movingAverageSize();

    /**
     * Rough max number of processors (CPU cores) simultaneously used in total by importer at any given time.
     * This value should be set while taking the necessary IO threads into account; the page cache and the operating
     * system will require a couple of threads between them, to handle the IO workload the importer generates.
     * Defaults to the value provided by the {@link Runtime#availableProcessors() jvm}. There's a discrete
     * number of threads that needs to be used just to get the very basics of the import working,
     * so for that reason there's no lower bound to this value.
     *   "Processor" in the context of the batch importer is different from "thread" since when discovering
     * how many processors are fully in use there's a calculation where one thread takes up 0 < fraction <= 1
     * of a processor.
     */
    int maxNumberOfProcessors();

    class Default implements Configuration
    {
        @Override
        public int batchSize()
        {
            return 10_000;
        }

        @Override
        public int movingAverageSize()
        {
            return 100;
        }

        @Override
        public int maxNumberOfProcessors()
        {
            return Runtime.getRuntime().availableProcessors();
        }
    }

    Configuration DEFAULT = new Default();

    class Overridden implements Configuration
    {
        private final Configuration defaults;

        public Overridden( Configuration defaults )
        {
            this.defaults = defaults;
        }

        @Override
        public int batchSize()
        {
            return defaults.batchSize();
        }

        @Override
        public int movingAverageSize()
        {
            return defaults.movingAverageSize();
        }

        @Override
        public int maxNumberOfProcessors()
        {
            return defaults.maxNumberOfProcessors();
        }
    }
}
