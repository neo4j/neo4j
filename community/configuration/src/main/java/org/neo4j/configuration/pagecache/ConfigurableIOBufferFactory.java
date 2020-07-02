/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.configuration.pagecache;

import org.neo4j.configuration.Config;
import org.neo4j.io.pagecache.buffer.IOBufferFactory;
import org.neo4j.io.pagecache.buffer.NativeIOBuffer;
import org.neo4j.memory.MemoryTracker;

import static org.neo4j.configuration.GraphDatabaseSettings.pagecache_buffered_flush_enabled;

public class ConfigurableIOBufferFactory implements IOBufferFactory
{
    private final Config config;
    private final MemoryTracker memoryTracker;

    public ConfigurableIOBufferFactory( Config config, MemoryTracker memoryTracker )
    {
        this.config = config;
        this.memoryTracker = memoryTracker;
    }

    @Override
    public NativeIOBuffer createBuffer()
    {
        return config.get( pagecache_buffered_flush_enabled ) ? new ConfigurableIOBuffer( config, memoryTracker ) : DISABLED_BUFFER_FACTORY.createBuffer();
    }
}
