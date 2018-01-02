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
package org.neo4j.commandline.dbms.config;

import org.neo4j.unsafe.impl.batchimport.Configuration;
/**
 * Provides a wrapper around {@link Configuration} with overridden defaults for neo4j-admin import
 * Use all available processors
 */
public class WrappedBatchImporterConfigurationForNeo4jAdmin implements Configuration
{
    private Configuration defaults;

    public WrappedBatchImporterConfigurationForNeo4jAdmin( Configuration defaults )
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
        return Configuration.allAvailableProcessors();
    }

    @Override
    public int denseNodeThreshold()
    {
        return defaults.denseNodeThreshold();
    }

    @Override
    public long pageCacheMemory()
    {
        return defaults.pageCacheMemory();
    }

    @Override
    public long maxMemoryUsage()
    {
        return defaults.maxMemoryUsage();
    }

    @Override
    public boolean sequentialBackgroundFlushing()
    {
        return defaults.sequentialBackgroundFlushing();
    }
}
