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
     * Number of batches that a {@link Step} can queue up before awaiting the downstream step to catch up.
     */
    int workAheadSize();

    /**
     * For statistics the average processing time is based on total processing time divided by
     * number of batches processed. A total average is probably not that interesting so this configuration
     * option specifies how many of the latest processed batches counts in the equation above.
     */
    int movingAverageSize();

    class Default implements Configuration
    {
        @Override
        public int batchSize()
        {
            return 10_000;
        }

        @Override
        public int workAheadSize()
        {
            return 20;
        }

        @Override
        public int movingAverageSize()
        {
            return 1000;
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
        public int workAheadSize()
        {
            return defaults.workAheadSize();
        }

        @Override
        public int movingAverageSize()
        {
            return defaults.movingAverageSize();
        }
    }

    class Explicit implements Configuration
    {
        private final int batchSize;
        private final int workAheadSize;
        private final int movingAverageSize;

        public Explicit( int batchSize, int workAheadSize, int movingAverageSize )
        {
            this.batchSize = batchSize;
            this.workAheadSize = workAheadSize;
            this.movingAverageSize = movingAverageSize;
        }

        @Override
        public int batchSize()
        {
            return batchSize;
        }

        @Override
        public int workAheadSize()
        {
            return workAheadSize;
        }

        @Override
        public int movingAverageSize()
        {
            return movingAverageSize;
        }
    }
}
