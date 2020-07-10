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

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import org.neo4j.configuration.Config;
import org.neo4j.memory.ScopedMemoryTracker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.pagecache_buffered_flush_enabled;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

class ConfigurableIOBufferFactoryTest
{
    @Test
    void createDisabledBufferCanBeDisabled()
    {
        var config = Config.defaults( pagecache_buffered_flush_enabled, false );
        var bufferFactory = new ConfigurableIOBufferFactory( config, INSTANCE );
        try ( var ioBuffer = bufferFactory.createBuffer() )
        {
            assertFalse( ioBuffer.isEnabled() );
        }
    }

    @Test
    void disabledBufferDoesNotConsumeMemory()
    {
        var config = Config.defaults( pagecache_buffered_flush_enabled, false );
        var memoryTracker = new ScopedMemoryTracker( INSTANCE );
        var bufferFactory = new ConfigurableIOBufferFactory( config, INSTANCE );
        try ( var ioBuffer = bufferFactory.createBuffer() )
        {
            assertFalse( ioBuffer.isEnabled() );
            assertThat( memoryTracker.usedNativeMemory() ).isZero();
        }
    }

    @Test
    @Disabled
    void defaultBufferCreation()
    {
        var config = Config.defaults();
        var memoryTracker = new ScopedMemoryTracker( INSTANCE );
        var bufferFactory = new ConfigurableIOBufferFactory( config, memoryTracker );
        try ( var ioBuffer = bufferFactory.createBuffer() )
        {
            assertTrue( ioBuffer.isEnabled() );
            assertThat( memoryTracker.usedNativeMemory() ).isGreaterThan( 0 );
        }
        assertThat( memoryTracker.usedNativeMemory() ).isZero();
    }
}
